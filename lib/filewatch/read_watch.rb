require "logger"
require_relative 'watch_base'

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
        # clean up any closed
        @files.values.select {|wf| wf.closed? }.each do |watched_file|
          file_deleteable << watched_file.path
          debug_log("each: closed: #{watched_file.path}, deleting from @files")
        end

        # Send any creates.
        @files.values.select {|wf| wf.watched? }.each do |watched_file|
          debug_log("each: reading: #{path}")
          yield(:read, watched_file)
          watched_file.activate
        end

        # wf.active? does not mean the actual files are open
        # only that the watch_file is active for further handling
        @files.values.select {|wf| wf.active? }.each do |watched_file|
          debug_log("each: reading: #{path}")
          yield(:read_more, watched_file)
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

        end
        # dont store excluded and ignored
        @files[file] = watched_file unless watched_file.unwatched?
      end
      file_deleteable.each {|f| @files.delete(f)}
    end # def _discover_file

  end # class Watch
end # module FileWatch
