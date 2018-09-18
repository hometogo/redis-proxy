require "yaml"

BasicSocket.do_not_reverse_lookup = true

module Proxy

  class Config

    attr_reader :log_level, :proxy

    def initialize( log )
      @log = log
      @raw = nil

      @log_level = Logger::WARN
      @proxy     = []
    end

    ##
    ## Instance methods
    ##

    def read( file )
      return false unless File.exists? file

      @log.info "reading configuration file #{file}"

      begin
        @raw = YAML::load File.read( file )
      rescue
        @log.error "configuration syntax error"
        return false
      end

      if @raw.has_key? "log_level"
        case @raw["log_level"]
          when "debug"
            @log_level = Logger::DEBUG
          when "info"
            @log_level = Logger::INFO
          when "warn"
            @log_level = Logger::WARN
          when "error"
            @log_level = Logger::ERROR
          else
            @log.warn "unknown log_level, defaulting to warn"
        end
      end

      if @raw.has_key? "proxy"
        @proxy = @raw["proxy"] if @raw["proxy"].kind_of? Array
      end

      if @proxy.empty?
        @log.error "no proxy configuration defined, exiting"
        exit
      end

      true
    end

  end

end
