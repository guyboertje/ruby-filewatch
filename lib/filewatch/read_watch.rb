require "logger"
require_relative 'watched_file'
require_relative 'watch_base'

if RbConfig::CONFIG['host_os'] =~ /mswin|mingw|cygwin/
  require "filewatch/winhelper"
  INODE_METHOD = :win_inode
else
  INODE_METHOD = :nix_inode
end

module FileWatch
  class ReadWatch < WatchBase

    # Calls &block with params [event_type, path]
    # event_type can be one of:
    #   :create_initial - initially present file (so start at end for tail)
    #   :create - file is created (new file after initial globs, start at 0)
    #   :modify - file is modified (size increases)
    #   :delete - file is deleted
    def each(&block)
      synchronized do
        return if @files.empty?

        file_deleteable = []
        # look at the unwatched to see if its gone away
        unwatched = @files.select {|k, wf| wf.closed? }
        closed.each do |path, watched_file|
          file_deleteable << path
          debug_log("each: closed: #{path}, deleting from @files")
        end

        # look at the ignored to see if its changed
        ignored = @files.select {|k, wf| wf.ignored? }
        ignored.each do |path, watched_file|
          begin
            stat = watched_file.restat
            if watched_file.size_changed? || watched_file.inode_changed?(inode(path,stat))
              # if the ignored file changed, move it to the watched state
              # this file has not been yielded to the block yet
              # but we need to allow the tail to start from the ignored_size
              # by adding this to the sincedb so that the subsequent modify
              # event can detect the change
              yield(:unignore, watched_file)
              watched_file.activate
            end
          rescue Errno::ENOENT
            # file has gone away or we can't read it anymore.
            file_deleteable << path
            debug_log("each: stat failed: #{path}: (#{$!}), deleting from @files")
          end
        end

        # Send any creates.
        creates = @files.select {|k, wf| wf.watched? }
        creates.each do |path, watched_file|
          if watched_file.initial?
            yield(:create_initial, watched_file)
          else
            yield(:create, watched_file)
          end
          watched_file.activate
        end

        actives = @files.select {|k, wf| wf.active? }
        # wf.active? does not mean the actual files are open
        # only that the watch_file is active for further handling
        actives.each do |path, watched_file|
          if watched_file.file_closable?
            debug_log("each: file expired: #{path}")
            yield(:timeout, watched_file)
            watched_file.close
            next
          end

          _inode = inode(path,stat)
          old_size = watched_file.size

          if watched_file.inode_changed?(_inode)
            debug_log("each: new inode: #{path}: old inode was #{watched_file.inode.inspect}, new is #{_inode.inspect}")
            watched_file.update_inode(_inode)
            yield(:delete, watched_file)
            yield(:create, watched_file)
            watched_file.update_size
          elsif stat.size < old_size
            debug_log("each: file rolled: #{path}: new size is #{stat.size}, old size #{old_size}")
            yield(:delete, watched_file)
            yield(:create, watched_file)
            watched_file.update_size
          elsif stat.size > old_size
            debug_log("each: file grew: #{path}: old size #{old_size}, new size #{stat.size}")
            yield(:modify, watched_file)
            watched_file.update_size
          end
        end

        file_deleteable.each {|f| @files.delete(f)}
      end
    end # def each

    private

    def _discover_file(path)
      file_deleteable = []
      _globbed_files(path).each do |file|
        next unless File.file?(file)
        new_discovery = false
        if @files.member?(file)
          # so we can check if it has been excluded in the meantime
          watched_file = @files[file]
        else
          debug_log("_discover_file: #{path}: new: #{file} (exclude is #{@exclude.inspect})")
          # let the caller build the object in its context
          new_discovery = true
          watched_file = yield(file, File::Stat.new(file))
        end

        skip = false
        @exclude.each do |pattern|
          if File.fnmatch?(pattern, File.basename(file))
            skip = true
            if new_discovery
              debug_log("_discover_file: #{file}: skipping because it " +
                          "matches exclude #{pattern}")
            else
              debug_log("_discover_file: #{file}: removing because it " +
                          "matches exclude #{pattern}")
              watched_file.file_close
              # allow state change side effects if any
              watched_file.unwatch
              file_deleteable << file
            end
            break
          end
        end
        next if skip

        if watched_file.file_ignorable?
          debug_log("_discover_file: #{file}: removing because it was last modified more than #{@ignore_older} seconds ago")
          watched_file.file_close
          watched_file.unwatch
          file_deleteable << file if !new_discovery
        else
          @files[file] = watched_file
        end
      end
      file_deleteable.each {|f| @files.delete(f)}
    end # def _discover_file

  end # class Watch
end # module FileWatch
