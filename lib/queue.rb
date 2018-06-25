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

      return false if @fail || !@buffer.empty? # TODO: log?

      statement.build # TODO: log?
    end

    private

    def populate_buffer
      payload = ""

      loop do
        data = @client.recv 32768

        if data.empty?
          @fail = true
          break
        end

        payload << data

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
