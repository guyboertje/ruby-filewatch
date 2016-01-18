require_relative 'context_accepting_read_base'

module FileWatch
  class ContextAcceptingBreadthRead  < ContextAcceptingReadBase
    def initialize(opts={})
      super
      @wait_for_files = @opts.fetch(:wait_for_files, 5)
      @chunky_read_thread = threaded_chunked_read
    end # def initialize

    public

    def subscribe(observer = NullObserver.new)
      @observer = observer
      @watch.subscribe(@opts[:stat_interval],
                       @opts[:discover_interval]) do |event, path|
        ctx = { :path => path }
        listener = @observer.channel_for(ctx)
        case event
        when :create, :create_initial
          if @files.member?(path)
            debug_log("#{event} for #{path}: already exists in @files")
          elsif _open_file(path, event)
            listener.accept(ctx.merge(:action => "created"))
          end
        when :modify
          if !@files.member?(path)
            debug_log(":modify for #{path}, does not exist in @files")
            if _open_file(path, event)
              listener.accept(ctx.merge(:action => "created"))
            end
          end
        when :delete
          debug_log(":delete for #{path}, deleted from @files")
          if @files[path]
            @files[path].close
          end
          listener.accept(ctx.merge(:action => "deleted"))
          @files.delete(path)
          @statcache.delete(path)
        when :timeout
          debug_log(":timeout for #{path}, deleted from @files")
          if (deleted = @files.delete(path))
            deleted.close
          end
          listener.accept(ctx.merge(:action => "timed_out"))
          @statcache.delete(path)
        else
          @logger.warn("unknown event type #{event} for #{path}")
        end
      end # @watch.subscribe
    end # def subscribe

    private

    def threaded_chunked_read
      Thread.new do
        while !@watch.quit? do
          if @files.empty? || @observer.nil?
            Stud.stoppable_sleep(@wait_for_files, 0.5) do
              @watch.quit? || !@files.empty?
            end
          else
            _read_chunks
          end
        end
      end
    end

    def _read_chunks
      changed = false
      # breath first read each file in chunks.
      # get a snapshot of the paths active right now
      @files.keys.dup.each do |path|
        changed |= _read_chunk(path)
      end

      if changed
        now = Time.now.to_i
        delta = now - @sincedb_last_write
        if delta >= @opts[:sincedb_write_interval]
          debug_log("writing sincedb (delta since last write = #{delta})")
          _sincedb_write
          @sincedb_last_write = now
        end
      end
    end

    def _read_chunk(path)
      ctx = { :identity => path }
      listener = @observer.channel_for(ctx)
      changed = false
      file = @files[path]
      return if file.nil? || file.closed?
      begin
        data = file.sysread(@chunk_size)
        changed = true
        @sincedb[@statcache[path]] += data.bytesize
        listener.accept(ctx.merge(:action => "chunk"), data)
        if file.eof?
          listener.accept(ctx.merge(:action => "eof"))
        end
      rescue EOFError
        listener.accept(ctx.merge(:action => "eof"))
      rescue Errno::EWOULDBLOCK, Errno::EINTR, StandardError => e
        listener.accept(ctx.merge(:action => "error", :error => e.message))
      end
      changed
    end # def _read_chunk

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
