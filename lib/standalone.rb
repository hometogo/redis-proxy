module Proxy

  class Standalone < Pool

    ##
    ## Instance methods
    ##

    def write( statement )
      payload = Parser.raw @log, statement
      index   = 0

      result = @upstream[index].write payload
      result ? index : false
    end

  end

end
