# TODO [boertje] Add support for ungzipping
module FileWatch
  class ReadFile
    attr_reader :size, :inode, :state, :file
    attr_reader :path, :filestat, :state_history

    def delimiter
      @delimiter
    end

    def initialize(path, inode, stat)
      @path = path
      @size = 0
      @inode = inode
      @state_history = []
      @state = :watched
    end

    def size_changed?
      filestat.size != size
    end

    def inode_changed?(value)
      self.inode != value
    end

    def file_add_opened(rubyfile)
      @file = rubyfile
    end

    def file_close
      return if @file.nil? || @file.closed?
      @file.close
      @file = nil
    end

    def file_seek(amount, whence = IO::SEEK_SET)
      @file.sysseek(amount, whence)
    end

    def file_read(amount)
      set_accessed_at
      @file.sysread(amount)
    end

    def file_open?
      !@file.nil? && !@file.closed?
    end

    def update_read_size(total_bytes_read)
      return if total_bytes_read.nil?
      @size = total_bytes_read
    end

    def buffer_extract(data)
      @buffer.extract(data)
    end

    def update_inode(_inode)
      @inode = _inode
    end

    def update_size
      @size = @filestat.size
    end

    def reading
      set_state :reading
    end

    def reading_more
      set_state :reading_more
    end

    def close
      set_state :closed
    end

    def watch
      set_state :watched
    end

    def unwatch
      set_state :unwatched
    end

    def reading?
      @state == :reading
    end

    def reading_more?
      @state == :reading_more
    end

    def closed?
      @state == :closed
    end

    def watched?
      @state == :watched
    end

    def unwatched?
      @state == :unwatched
    end

    def set_state(value)
      @state_history << @state
      @state = value
    end

    def state_history_any?(*previous)
      (@state_history & previous).any?
    end

    def to_s() inspect; end
  end
end
