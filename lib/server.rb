require_relative "../lib/queue"

module Proxy

  class Server

    ERROR_NO_UPSTREAM   = "-No upstreams available\r\n"
    ERROR_READ_UPSTREAM = "-Upstream read error\r\n"

    def initialize( log, host, pool )
      @log    = log
      @host   = host
      @ip     = host.split( ":" ).first
      @port   = host.split( ":" ).last
      @pool   = pool
      @socket = nil
    end

    ##
    ## Instance methods
    ##

    def open
      @socket = TCPServer.new @ip, @port
    end

    def run
      loop do
        Thread.start @socket.accept do |client|
          running     = true
          remote_host = client.remote_address.ip_unpack.join ":"
          queue       = Proxy::Queue.new @log, client

          @log.info "connection open from #{remote_host}"

          while running do
            statement = queue.fetch

            unless statement
              running = false
              next
            end

            index = @pool.write statement

            unless index
              @log.error "#{self.class.to_s}##{__method__} no upstreams available"
              client.write ERROR_NO_UPSTREAM
              next
            end

            payload = @pool.read index

            unless payload
              @log.error "#{self.class.to_s}##{__method__} upstream read error"
              client.write ERROR_READ_UPSTREAM
              next
            end

            client.write payload
          end

          @log.info "connection close from #{remote_host}"

          client.close
        end
      end
    end

  end

end
