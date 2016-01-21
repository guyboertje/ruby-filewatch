require 'filewatch/tail_base'

module FileWatch
  class YieldingTail
    include TailBase

    public
    def subscribe(&block)
      # subscribe(stat_interval = 1, discover_interval = 5, &block)
      @watch.subscribe(@opts[:stat_interval],
                       @opts[:discover_interval]) do |event, watched_file|
        path = watched_file.path
        file_is_open = watched_file.file_open?

        case event
        when :unignore
          _add_to_sincedb(watched_file, event)
        when :create, :create_initial
          if file_is_open
            debug_log("#{event} for #{path}: file already open")
            next
          end
          if _open_file(watched_file, event)
            yield_read_file(watched_file, &block)
          end
        when :modify
          if !file_is_open
            debug_log(":modify for #{path}, file is not open, opening now")
            if _open_file(watched_file, event)
              yield_read_file(watched_file, &block)
            end
          else
            yield_read_file(watched_file, &block)
          end
        when :delete
          if file_is_open
            debug_log(":delete for #{path}, closing file")
            yield_read_file(watched_file, &block)
            watched_file.file_close
          else
            debug_log(":delete for #{path}, file already closed")
          end
        when :timeout
          debug_log(":timeout for #{path}, closing file")
          watched_file.file_close
        else
          @logger.warn("unknown event type #{event} for #{path}")
        end
      end # @watch.subscribe
    end # def subscribe

    private
    def yield_read_file(watched_file, &block)
      changed = false
      loop do
        begin
          data = watched_file.file_read(32768)
          changed = true
          watched_file.buffer_extract(data).each do |line|
            yield(watched_file.path, line)
            @sincedb[watched_file.inode] += (line.bytesize + @delimiter_byte_size)
          end
          # update what we have read so far
          # if the whole file size is smaller than 32768 bytes
          # we would have read it all now.
          watched_file.update_read_size(@sincedb[watched_file.inode])
        rescue Errno::EWOULDBLOCK, Errno::EINTR, EOFError
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
    end
  end # module YieldingTail
end # module FileWatch
