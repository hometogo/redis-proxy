require_relative "parser"

module Proxy

  class Queue

    def initialize( log, client )
      @log    = log
      @client = client
      @fail   = false

      @buffer = []
    end

    ##
    ## Instance methods
    ##

    def fetch
      statement = Parser.new @log

      loop do
        populate_buffer if @buffer.empty?
        break if @fail

        statement.feed @buffer.shift
        break if statement.complete?
      end

      if @fail || !@buffer.empty?
        @log.error "#{self.class.to_s}##{__method__} data read error"
        @log.debug "#{self.class.to_s}##{__method__} buffer #{@buffer.inspect}"
        return false
      end

      statement.build
    end

    private

    def populate_buffer
      payload = ""

      loop do
        begin
          payload << @client.read_nonblock( 32768 )
        rescue IO::WaitReadable
          unless IO.select [@client], nil, nil, 0.1
            @log.error "#{self.class.to_s}##{__method__} data read timeout"
            @fail = true
            break
          end

          retry
        end

        partial = payload[payload.size - 1] != "\n"
        @log.warn "#{self.class.to_s}##{__method__} partial payload, recv buffer too small?" if partial

        break unless partial
      end

      @log.debug "#{self.class.to_s}##{__method__} payload #{payload.inspect}"
      @buffer.concat payload.split( "\r\n" )
      @log.debug "#{self.class.to_s}##{__method__} buffer #{@buffer.inspect}"
    end

  end

end
