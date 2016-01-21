require_relative 'context_accepting_read_base'

module FileWatch
  class ContextAcceptingDepthRead < ContextAcceptingReadBase
    def subscribe(observer = NullObserver.new)
      @watch.subscribe(@opts[:stat_interval],
                       @opts[:discover_interval]) do |event, path|
        ctx = { :path => path }
        listener = observer.channel_for(ctx)
        case event
        when :create, :create_initial
          if @files.member?(path)
            debug_log("#{event} for #{path}: already exists in @files")
            next
          end
          if _open_file(path, event)
            listener.accept(ctx.merge(:action => "created"))
            _read_chunk(ctx, listener)
          end
        when :modify
          if !@files.member?(path)
            debug_log(":modify for #{path}, does not exist in @files")
            if _open_file(path, event)
              _read_chunk(ctx, listener)
            end
          else
            _read_chunk(ctx, listener)
          end
        when :delete
          debug_log(":delete for #{path}, deleted from @files")
          if @files[path]
            _read_chunk(path, listener)
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

    def _read_chunk(ctx, listener)
      path = ctx[:path]
      changed = false
      loop do
        begin
          data = @files[path].sysread(@chunk_size)
          changed = true
          listener.accept(ctx.merge(:action => "chunk"), data)
          @sincedb[@statcache[path]] += data.bytesize
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
          debug_log("writing sincedb (delta since last write = #{delta})")
          _sincedb_write
          @sincedb_last_write = now
        end
      end
    end # def _read_file
  end
end # module FileWatch
