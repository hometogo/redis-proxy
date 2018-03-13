module Proxy

  class Parser

    TYPE_MAP = {
      "+" => :string,
      "-" => :error,
      ":" => :integer,
      "$" => :bulk,
      "*" => :array
    }

    def initialize( log )
      @log   = log
      @index = 0

      @type     = nil
      @size     = 0
      @data     = nil
      @complete = false
    end

    ##
    ## Instance methods
    ##

    def complete?
      @complete
    end

    def feed( chunk )
      unless @data
        parse_chunk chunk
      else
        case @type
          when :bulk
            parse_type_bulk chunk

          when :array
            parse_type_array chunk

          else
            @log.warn "#{self.class.to_s}##{__method__} unknown type '#{@type.inspect}' for non header chunk"
        end
      end
    end

    def build
      @data.map { |i| i.kind_of?( Proxy::Parser ) ? i.build : i  }
    end

    private

    def parse_chunk( chunk )
      parse_chunk_header chunk

      case @type
        when :inline
          @data = Array.new 1

          @data[0]  = chunk
          @complete = true

        when :string, :error, :integer
          @data = Array.new 1

          @data[0]  = chunk
          @complete = true

        when :bulk
          @data = Array.new 2
          @data[0] = chunk

          parse_chunk_header_bulk if @size < 0

        when :array
          @data = Array.new @size + 1
          @data[0] = chunk

          parse_chunk_header_array if @size <= 0
      end

      @index += 1
    end

    def parse_chunk_header( chunk )
      @type = TYPE_MAP[chunk[0]] || :inline
      @size = chunk[1..chunk.size - 1].to_i

      @log.debug "#{self.class.to_s}##{__method__} chunk #{chunk.inspect} type #{@type.inspect} size #{@size.inspect}"
    end

    def parse_chunk_header_bulk
      @data[1]  = @size.zero? ? "" : nil # TODO : null only
      @complete = true

      @log.debug "#{self.class.to_s}##{__method__} empty or null bulk string for size #{@size.inspect}"
    end

    def parse_chunk_header_array
      @complete = true
      @log.debug "#{self.class.to_s}##{__method__} empty array for size #{@size.inspect}"
    end

    def parse_type_bulk( chunk )
      unless @data[@index]
        @data[@index] = chunk
      else
        @data[@index] << "\r\n" << chunk
      end

      if @data[@index].bytesize == @size - 2
        @data[@index] << "\r\n"
        @log.warn "#{self.class.to_s}##{__method__} bulk string byte size mismatch, correcting with \\r\\n"
      end

      @complete = true if @data[@index].bytesize >= @size
    end

    def parse_type_array( chunk )
      @data[@index] = Parser.new @log unless @data[@index]
      @data[@index].feed chunk

      @index += 1 if @data[@index].complete?

      @complete = true if @index >= @size + 1
    end

    ##
    ## Class methods
    ##

    class << self

      def analyze( log, statement )
        command = statement[1][1].upcase
        key     = nil

        case command
          when "SET", "SETEX", "DEL"
            action = :write
            key    = statement[2][1]

          when "GET"
            action = :read
            key    = statement[2][1]

          else
            action = :unknown
            log.warn "#{self.to_s}::#{__method__} action '#{action.inspect}' for command #{command.inspect}"
        end

        result = Struct::Analyzer.new action, key
        log.debug "#{self.to_s}::#{__method__} result #{result.inspect}"
        result
      end

      def raw( log, statement )
        log.debug "#{self.to_s}::#{__method__} statement #{statement.inspect}"
        result = statement.flatten.compact.join( "\r\n" ).concat "\r\n"
        log.debug "#{self.to_s}::#{__method__} result #{result.inspect}"
        result
      end

    end

  end

end

Struct.new "Analyzer", :action, :key
