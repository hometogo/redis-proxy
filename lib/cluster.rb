require "digest/crc16"

require_relative "command"

module Proxy

  class Cluster < Pool

    def initialize( log )
      super log

      @map = []
    end

    ##
    ## Instance methods
    ##

    def write( statement )
      analyzer = Parser.analyze @log, statement
      payload  = Parser.raw @log, statement
      index    = nil

      populate_map if @map.empty?

      case analyzer.action
        when :write
          index = upstream_lookup_master analyzer.key

        when :read
          mindex = upstream_lookup_master analyzer.key
          index  = upstream_lookup_slave mindex

        when :unknown
          index = 0
      end

      return false unless index

      result = @upstream[index].write payload
      result ? index : false
    end

    private

    def upstream_lookup_master( key )
      map_index = map_lookup_master key
      return false unless map_index

      upstream_index = @upstream.index { |u| u.host == @map[map_index].host }
      @log.debug "#{self.class.to_s}##{__method__} upstream_index #{upstream_index.inspect}"
      upstream_index
    end

    def map_lookup_master( key )
      slot      = get_slot key
      map_index = @map.index { |sm| sm.range.cover? slot }

      @log.debug "#{self.class.to_s}##{__method__} map_index #{map_index.inspect}"
      map_index
    end

    def get_slot( key )
      slot = Digest::CRC16.checksum( key ) % 16384
      @log.debug "#{self.class.to_s}##{__method__} result #{slot.inspect}"
      slot
    end

    def populate_map
      @log.info "#{self.class.to_s}##{__method__} rebuilding cluster map"

      index = @upstream.select { |u| u.master? }
      return if index.empty?

      map = Command::read_map index.sample
      return unless map

      map.each do |s|
        next if s.kind_of? String

        first = s[1][0].slice(1..-1).to_i
        last  = s[2][0].slice(1..-1).to_i
        ip    = s[3][1][1].empty? ? "127.0.0.1" : s[3][1][1]
        port  = s[3][2][0].slice(1..-1)

        slot = Struct::Slot.new first..last, [ip, port].join( ":" )
        @log.debug "#{self.class.to_s}##{__method__} slot #{slot.inspect}"

        @map << slot
      end
    end

  end

end

Struct.new "Slot", :range, :host
