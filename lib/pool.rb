require_relative "upstream"

module Proxy

  class Pool

    def initialize( log )
      @log      = log
      @upstream = []
    end

    ##
    ## Instance methods
    ##

    def add( host, timeout )
      @upstream << Upstream.new( @log, host, timeout )
    end

    def read( index )
      statement = @upstream[index].read
      statement ? Parser.raw( @log, statement ) : false
    end

    protected

    def upstream_lookup_slave( mindex )
      return nil unless mindex

      upstream_index = @upstream.select { |u| u.slave? && u.slave_of == @upstream[mindex].host }
      @log.debug "#{self.class.to_s}##{__method__} upstream_index #{upstream_index.inspect}"
      upstream_index.empty? ? mindex : @upstream.find_index( upstream_index.sample )
    end

  end

end
