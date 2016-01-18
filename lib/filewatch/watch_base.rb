require "logger"
require_relative 'watched_file'

if RbConfig::CONFIG['host_os'] =~ /mswin|mingw|cygwin/
  require "filewatch/winhelper"
  FILEWATCH_INODE_METHOD = :win_inode
else
  FILEWATCH_INODE_METHOD = :nix_inode
end

module FileWatch
  class WatchBase

    def self.win_inode(path, stat)
      fileId = Winhelper.GetWindowsUniqueFileIdentifier(path)
      [fileId, 0, 0] # dev_* doesn't make sense on Windows
    end

    def self.nix_inode(path, stat)
      [stat.ino.to_s, stat.dev_major, stat.dev_minor]
    end

    def self.inode(path, stat)
      send(FILEWATCH_INODE_METHOD, path, stat)
    end

    attr_accessor :logger
    attr_accessor :delimiter
    attr_reader :max_active

    def initialize(opts={})
      if opts[:logger]
        @logger = opts[:logger]
      else
        @logger = Logger.new(STDERR)
        @logger.level = Logger::INFO
      end
      @watching = []
      @exclude = []
      @files = Hash.new { |h, k| h[k] = WatchedFile.new(k, nil, nil, false) }
      # we need to be threadsafe about the mutation
      # of the above 2 ivars because the public
      # methods each, discover, watch and unwatch
      # can be called from different threads.
      @lock = Mutex.new
      # we need to be threadsafe about the quit mutation
      @quit = false
      @max_active = ENV.fetch("FILEWATCH_MAX_OPEN_FILES", 4095).to_i
      @quit_lock = Mutex.new
    end # def initialize

    public

    def max_open_files=(value)
      val = value.to_i
      return if value.nil? || val <= 0
      @max_active = val
    end

    def ignore_older=(value)
      #nil is allowed but 0 and negatives are made nil
      if !value.nil?
        val = value.to_i
        val = val <= 0 ? nil : val
      end
      @ignore_older = val
    end

    def close_older=(value)
      if !value.nil?
        val = value.to_i
        val = val <= 0 ? nil : val
      end
      @close_older = val
    end

    def exclude(path)
      path.to_a.each { |p| @exclude << p }
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

    def unwatch(path)
      synchronized do
        result = false
        if @watching.delete(path)
          # path is a directory
          _globbed_files(path).each do |file|
            if (watched_file = @files[path])
              watched_file.unwatch
            end
          end
          result = true
        elsif (watched_file = @files[path])
          watched_file.unwatch
          result = true
        end
        return !!result
      end
    end

    def inode(path, stat)
      self.class.inode(path, stat)
    end

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

    def subscribe(stat_interval = 1, discover_interval = 5, &block)
      glob = 0
      reset_quit
      while !quit?
        each(&block)

        glob += 1
        if glob == discover_interval
          discover
          glob = 0
        end
        break if quit?
        sleep(stat_interval)
      end
      @files.values.each(&:file_close)
    end # def subscribe

    def quit
      @quit_lock.synchronize { @quit = true }
    end # def quit

    def quit?
      @quit_lock.synchronize { @quit }
    end

    private

    def debug_log(msg)
      return unless @logger.debug?
      @logger.debug(msg)
    end

    private
    def _globbed_files(path)
      globbed_dirs = Dir.glob(path)
      debug_log("_globbed_files: #{path}: glob is: #{globbed_dirs}")
      if globbed_dirs.empty? && File.file?(path)
        globbed_dirs = [path]
        debug_log("_globbed_files: #{path}: glob is: #{globbed_dirs} because glob did not work")
      end
      # return Enumerator
      globbed_dirs.to_enum
    end

    private
    def synchronized(&block)
      @lock.synchronize { block.call }
    end

    public

    private
    def reset_quit
      @quit_lock.synchronize { @quit = false }
    end
  end # class Watch
end # module FileWatch
