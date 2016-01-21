require 'filewatch/tail_base'

module FileWatch
  class ContextAcceptingTail
    include TailBase
    public

    class NullListener
      def accept(context, data = nil)
      end
    end

    class NullObserver
      def channel_for(path) NullListener.new(path); end
    end

    def subscribe(observer = NullObserver.new)
      @watch.subscribe(@opts[:stat_interval],
                       @opts[:discover_interval]) do |event, path|
        ctx = { :path => path }
        listener = observer.channel_for(ctx)
        case event
        when :create, :create_initial
          if @files.member?(path)
            @logger.debug? && @logger.debug("#{event} for #{path}: already exists in @files")
            next
          end
          if _open_file(path, event)
            listener.accept(ctx.merge(:action => "created"))
            accept_read_file(ctx, listener)
          end
        when :modify
          if !@files.member?(path)
            @logger.debug? && @logger.debug(":modify for #{path}, does not exist in @files")
            if _open_file(path, event)
              accept_read_file(ctx, listener)
            end
          else
            accept_read_file(ctx, listener)
          end
        when :delete
          @logger.debug? && @logger.debug(":delete for #{path}, deleted from @files")
          if @files[path]
            accept_read_file(path, listener)
            @files[path].close
          end
          listener.accept(ctx.merge(:action => "deleted"))
          @files.delete(path)
          @statcache.delete(path)
        when :timeout
          @logger.debug? && @logger.debug(":timeout for #{path}, deleted from @files")
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
    def accept_read_file(ctx, listener)
      path = ctx[:path]
      @buffers[path] ||= FileWatch::BufferedTokenizer.new(@opts[:delimiter])
      delimiter_byte_size = @opts[:delimiter].bytesize
      changed = false
      loop do
        begin
          data = @files[path].sysread(32768)
          changed = true
          @buffers[path].extract(data).each do |line|
            listener.accept(ctx.merge(:action => "line"), line)
            @sincedb[@statcache[path]] += (line.bytesize + delimiter_byte_size)
          end
        rescue EOFError
          listener.accept(ctx.merge(:action => "eof"))
          break
        rescue Errno::EWOULDBLOCK, Errno::EINTR
          listener.accept(ctx.merge(:action => "error"))
          break
        end
      end

      if changed
        now = Time.now.to_i
        delta = now - @sincedb_last_write
        if delta >= @opts[:sincedb_write_interval]
          @logger.debug? && @logger.debug("writing sincedb (delta since last write = #{delta})")
          _sincedb_write
          @sincedb_last_write = now
        end
      end
    end # def _read_file
  end
end # module FileWatch
