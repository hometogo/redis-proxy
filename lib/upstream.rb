require_relative "queue"
require_relative "command"

module Proxy

  class Upstream

    ROLE_MAP = {
      "master" => :master,
      "slave"  => :slave
    }

    SLAVE_STATUS_MAP = {
      "connect"    => :connect,
      "connecting" => :connecting,
      "sync"       => :sync,
      "connected"  => :connected
    }

    attr_reader :host
    attr_reader :slave_of

    def initialize( log, host, timeout )
      @log     = log
      @host    = host
      @ip      = host.split( ":" ).first
      @port    = host.split( ":" ).last
      @timeout = timeout
      @mutex   = Mutex.new

      @connected    = false
      @queue        = nil
      @role         = nil
      @slave_of     = nil
      @slave_status = nil
      @active       = false

      Upstream.add self
    end

    ##
    ## Instance methods
    ##

    def connected?
      @connected
    end

    def master?
      @connected && @role == :master
    end

    def slave?
      @connected && @role == :slave && @slave_status == :connected
    end

    def open
      begin
        @socket = Socket.tcp @ip, @port, connect_timeout: @timeout
      rescue Errno::ETIMEDOUT
        @log.warn "#{self.class.to_s}##{__method__} connection timeout for upstream #{@host}"
      rescue Errno::ECONNREFUSED
        @log.warn "#{self.class.to_s}##{__method__} connection refused for upstream #{@host}"
      else
        @log.info "#{self.class.to_s}##{__method__} connection open for upstream #{@host}"

        @queue     = Queue.new @log, @socket
        @connected = true
      end
    end

    def read
      return false unless @connected

      result = false

      begin
        result = @queue.fetch
      rescue Exception => e
        @log.error "#{self.class.to_s}##{__method__} exception #{e.inspect} for upstream #{@host}"
        close
      end

      @mutex.unlock
      result
    end

    def write( payload )
      return false unless @connected

      result = false
      @mutex.lock

      begin
        result = @socket.write( payload ) == payload.size
      rescue Exception => e
        @log.error "#{self.class.to_s}##{__method__} exception #{e.inspect} for upstream #{@host}"
        close
        @mutex.unlock
      end

      result
    end

    def close
      @log.info "#{self.class.to_s}##{__method__} connection close for upstream #{@host}"
      @connected = false
      @socket.close
    end

    def check
      role = Command::role self
      return false unless role

      @role = ROLE_MAP[role[1][1]]
      @log.debug "#{self.class.to_s}##{__method__} upstream #{@host} role #{@role.inspect}"

      return if master?

      @slave_of     = [role[2][1], role[3][0]].join ":"
      @slave_status = SLAVE_STATUS_MAP[role[4][1]]

      @log.debug "#{self.class.to_s}##{__method__} upstream #{@host} slave_of #{@slave_of.inspect}"
      @log.debug "#{self.class.to_s}##{__method__} upstream #{@host} slave_status #{@slave_status.inspect}"

      return if slave?

      @log.error "#{self.class.to_s}##{__method__} upstream #{@host} unknown check results"
    end

    private

    ##
    ## Class methods
    ##

    @@upstream = []

    class << self

      def add( upstream )
        @@upstream << upstream
      end

      def all
        @@upstream
      end

    end

  end

end
