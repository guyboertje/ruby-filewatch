require "logger"
require_relative 'watch_base'

module FileWatch
  #AKA TailWatch
  class Watch < WatchBase
    public

    def discover
      synchronized do
        @watching.each do |path|
          _discover_file(path) do |filepath, stat|
            WatchedFile.new_ongoing(
              filepath, inode(filepath, stat), stat).tap do |inst|
                inst.delimiter = @delimiter
                inst.ignore_older = @ignore_older
                inst.close_older = @close_older
            end
          end
        end
      end
    end

    def watch(path)
      synchronized do
        if !@watching.member?(path)
          @watching << path
          _discover_file(path) do |filepath, stat|
            WatchedFile.new_initial(
              filepath, inode(filepath, stat), stat).tap do |inst|
                inst.delimiter = @delimiter
                inst.ignore_older = @ignore_older
                inst.close_older = @close_older
            end
          end
        end
      end
      return true
    end # def watch

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
        # look at the closed to see if its changed

        @files.values.select {|wf| wf.closed? }.each do |watched_file|
          path = watched_file.path
          begin
            stat = watched_file.restat
            if watched_file.size_changed? || watched_file.inode_changed?(inode(path,stat))
              # if the closed file changed, move it to the watched state
              # not to active state because we want to use MAX_OPEN_FILES throttling.
              watched_file.watch
            end
          rescue Errno::ENOENT
            # file has gone away or we can't read it anymore.
            file_deleteable << path
            debug_log("each: closed: stat failed: #{path}: (#{$!}), deleting from @files")
          rescue => e
            debug_log("each: closed: stat failed: #{path}: (#{e.inspect})")
          end
        end

        # look at the ignored to see if its changed
        @files.values.select {|wf| wf.ignored? }.each do |watched_file|
          path = watched_file.path
          begin
            stat = watched_file.restat
            if watched_file.size_changed? || watched_file.inode_changed?(inode(path,stat))
              # if the ignored file changed, move it to the watched state
              # not to active state because we want to use MAX_OPEN_FILES throttling.
              # this file has not been yielded to the block yet
              # but we need to allow the tail to start from the ignored_size
              # by adding this to the sincedb so that the subsequent modify
              # event can detect the change
              watched_file.watch
              yield(:unignore, watched_file)
            end
          rescue Errno::ENOENT
            # file has gone away or we can't read it anymore.
            file_deleteable << path
            debug_log("each: ignored: stat failed: #{path}: (#{$!}), deleting from @files")
          rescue => e
            debug_log("each: ignored: stat failed: #{path}: (#{e.inspect})")
          end
        end

        # Send any creates.
        if (to_take = @max_active - @files.values.count{|wf| wf.active?}) > 0
          @files.values.select {|wf| wf.watched? }.take(to_take).each do |watched_file|
            watched_file.activate
            # don't do create again
            next if watched_file.state_history_any?(:closed, :ignored)
            if watched_file.initial?
              yield(:create_initial, watched_file)
            else
              yield(:create, watched_file)
            end
          end
        end

        # warning on open file limit

        # wf.active? does not mean the actual files are open
        # only that the watch_file is active for further handling
        @files.values.select {|wf| wf.active? }.each do |watched_file|
          path = watched_file.path
          begin
            stat = watched_file.restat
          rescue Errno::ENOENT
            # file has gone away or we can't read it anymore.
            file_deleteable << path
            debug_log("each: active: stat failed: #{path}: (#{$!}), deleting from @files")
            watched_file.unwatch
            yield(:delete, watched_file)
            next
          rescue => e
            debug_log("each: active: stat failed: #{path}: (#{e.inspect})")
            next
          end

          if watched_file.file_closable?
            debug_log("each: active: file expired: #{path}")
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
            debug_log("_discover_file: #{file}: skipping because it " +
                          "matches exclude #{pattern}") if new_discovery
            skip = true
            watched_file.file_close
            watched_file.unwatch
            break
          end
        end
        next if skip

        if new_discovery
          if watched_file.file_ignorable?
            debug_log("_discover_file: #{file}: skipping because it was last modified more than #{@ignore_older} seconds ago")
            # on discovery we put watched_file into the ignored state and that
            # updates the size from the internal stat
            # so the existing contents are not read.
            # because, normally, a newly discovered file will
            # have a watched_file size of zero
            watched_file.ignore
          end
          @files[file] = watched_file
        end
      end
    end # def _discover_file
  end # class Watch
end # module FileWatch
