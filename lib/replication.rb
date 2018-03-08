module Proxy

  class Replication < Pool

    ##
    ## Instance methods
    ##

    def write( statement )
      analyzer = Parser.analyze @log, statement
      payload  = Parser.raw @log, statement
      index    = nil

      case analyzer.action
        when :write
          index = @upstream.index { |u| u.master? }

        when :read, :unknown
          mindex = @upstream.index { |u| u.master? }
          index  = upstream_lookup_slave mindex
      end

      return false unless index

      result = @upstream[index].write payload
      result ? index : false
    end

  end

end
