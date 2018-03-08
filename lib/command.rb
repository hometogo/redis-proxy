module Proxy

  class Command

    CMD_ROLE  = "*1\r\n$4\r\nrole\r\n"
    CMD_SLOTS = "*2\r\n$7\r\ncluster\r\n$5\r\nslots\r\n"

    ##
    ## Class methods
    ##

    class << self

      def role( upstream )
        result = upstream.write CMD_ROLE
        return false unless result

        statement = upstream.read
        statement ? statement : false
      end

      def read_map( upstream )
        result = upstream.write CMD_SLOTS
        return false unless result

        statement = upstream.read
        statement ? statement : false
      end

    end

  end

end
