
if RbConfig::CONFIG['host_os'] =~ /mswin|mingw|cygwin/
  require "filewatch/winhelper"
end
require "logger"
require "rbconfig"

include Java if defined? JRUBY_VERSION
require "JRubyFileExtension.jar" if defined? JRUBY_VERSION

module FileWatch
  class ContextAcceptingReadBase
    class NullListener
      def accept(context, data = nil)
      end
    end

    class NullObserver
      def channel_for(path) NullListener.new(path); end
    end

    # how often (in seconds) we @logger.warn a failed file open, per path.
    OPEN_WARN_INTERVAL = ENV["FILEWATCH_OPEN_WARN_INTERVAL"] ?
                         ENV["FILEWATCH_OPEN_WARN_INTERVAL"].to_i : 300

    attr_reader :logger

    class NoSinceDBPathGiven < StandardError; end

    public
    def initialize(opts={})
      @iswindows = ((RbConfig::CONFIG['host_os'] =~ /mswin|mingw|cygwin/) != nil)

      if opts[:logger]
        @logger = opts[:logger]
      else
        @logger = Logger.new(STDERR)
        @logger.level = Logger::INFO
      end
      @files = {}
      @lastwarn = Hash.new { |h, k| h[k] = 0 }
      @buffers = {}
      @watch = FileWatch::Watch.new
      @watch.logger = @logger
      @sincedb = {}
      @sincedb_last_write = 0
      @statcache = {}
      @opts = {
        :sincedb_write_interval => 10,
        :stat_interval => 1,
        :discover_interval => 5,
        :exclude => []
      }.merge(opts)
      if !@opts.include?(:sincedb_path)
        @opts[:sincedb_path] = File.join(ENV["HOME"], ".sincedb") if ENV.include?("HOME")
        @opts[:sincedb_path] = ENV["SINCEDB_PATH"] if ENV.include?("SINCEDB_PATH")
      end
      if !@opts.include?(:sincedb_path)
        raise NoSinceDBPathGiven.new("No HOME or SINCEDB_PATH set in environment. I need one of these set so I can keep track of the files I am following.")
      end
      @watch.exclude(@opts[:exclude])
      @watch.close_older = nil
      @watch.ignore_older = @opts[:ignore_older]
      @chunk_size = @opts.fetch(:chunk_size, 16384)

      _sincedb_open
    end # def initialize

    public
    def logger=(logger)
      @logger = logger
      @watch.logger = logger
    end # def logger=

    public
    def monitor(path)
      @watch.watch(path)
    end # def tail


    def sincedb_write(reason=nil)
      debug_log("caller requested sincedb write (#{reason})")
      _sincedb_write
    end

    # quit is a sort-of finalizer,
    # it should be called for clean up
    # before the instance is disposed of.
    def quit
      _sincedb_write
      @watch.quit
      @files.each {|path, file| file.close }
      @files.clear
    end # def quit

    # close_file(path) is to be used by external code
    # when it knows that it is completely done with a file.
    # Other files or folders may still be being watched.
    # Caution, once unwatched, a file can't be watched again
    # unless a new instance of this class begins watching again.
    # The sysadmin should rename, move or delete the file.
    def close_file(path)
      @watch.unwatch(path)
      file = @files.delete(path)
      return if file.nil?
      _sincedb_write
      file.close
    end

    private

    def _open_file(path, event)
      debug_log("_open_file: #{path}: opening")
      begin
        if @iswindows && defined? JRUBY_VERSION
          @files[path] = Java::RubyFileExt::getRubyFile(path)
        else
          @files[path] = File.open(path)
        end
      rescue
        # don't emit this message too often. if a file that we can't
        # read is changing a lot, we'll try to open it more often,
        # and might be spammy.
        now = Time.now.to_i
        if now - @lastwarn[path] > OPEN_WARN_INTERVAL
          @logger.warn("failed to open #{path}: #{$!}")
          @lastwarn[path] = now
        else
          debug_log("(warn supressed) failed to open #{path}: #{$!}")
        end
        @files.delete(path)
        return false
      end

      stat = File::Stat.new(path)
      sincedb_record_uid = sincedb_record_uid(path, stat)

      expired_based_size = file_expired?(stat) ? stat.size : 0

      if @sincedb.member?(sincedb_record_uid)
        last_size = @sincedb[sincedb_record_uid]
        debug_log("#{path}: sincedb last value #{@sincedb[sincedb_record_uid]}, cur size #{stat.size}")
        if last_size <= stat.size
          debug_log("#{path}: sincedb: seeking to #{last_size}")
          @files[path].sysseek(last_size, IO::SEEK_SET)
        else
          debug_log("#{path}: last value size is greater than current value, starting over")
          @sincedb[sincedb_record_uid] = 0
        end
      elsif event == :create_initial && @files[path]
        debug_log("#{path}: initial create, no sincedb, seeking to beginning of file")
        @files[path].sysseek(expired_based_size, IO::SEEK_SET)
        @sincedb[sincedb_record_uid] = expired_based_size
      elsif event == :create && @files[path]
        @sincedb[sincedb_record_uid] = expired_based_size
      else
        debug_log("#{path}: staying at position 0, no sincedb")
      end

      return true
    end # def _open_file

    def _sincedb_open
      path = @opts[:sincedb_path]
      begin
        db = File.open(path)
      rescue
        #No existing sincedb to load
        debug_log("_sincedb_open: #{path}: #{$!}")
        return
      end

      debug_log("_sincedb_open: reading from #{path}")
      db.each do |line|
        ino, dev_major, dev_minor, pos = line.split(" ", 4)
        sincedb_record_uid = [ino, dev_major.to_i, dev_minor.to_i]
        debug_log("_sincedb_open: setting #{sincedb_record_uid.inspect} to #{pos.to_i}")
        @sincedb[sincedb_record_uid] = pos.to_i
      end
      db.close
    end # def _sincedb_open

    def _sincedb_write
      path = @opts[:sincedb_path]
      if @iswindows || File.device?(path)
        IO.write(path, serialize_sincedb, 0)
      else
        File.atomic_write(path) {|file| file.write(serialize_sincedb) }
      end
    end # def _sincedb_write

    def serialize_sincedb
      @sincedb.map do |inode, pos|
        [inode, pos].flatten.join(" ")
      end.join("\n") + "\n"
    end

    def debug_log(message, options = nil)
      return unless @logger.debug?
      if options.nil?
        @logger.debug(message)
      else
        @logger.debug(message, options)
      end
    end
  end
end # module FileWatch
