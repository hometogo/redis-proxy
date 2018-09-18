require "yaml"

require_relative "pool"
require_relative "standalone"
require_relative "replication"
require_relative "cluster"
require_relative "server"

BasicSocket.do_not_reverse_lookup = true

module Proxy

  class Config

    attr_reader :log_level, :server

    def initialize( log )
      @log = log
      @raw = nil

      @log_level = nil
      @server    = []
    end

    ##
    ## Instance methods
    ##

    def read( file )
      unless File.exists? file
        @log.error "configuration file #{file} not found"
        exit
      end

      @log.info "reading configuration file #{file}"

      begin
        @raw = YAML::load File.read( file )
      rescue
        @log.error "configuration file syntax error"
        exit
      end

      unless @raw.has_key? "log_level"
        @log.error "configuration item 'log_level' not found"
        exit
      end

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
          @log.error "configuration item 'log_level' not valid"
          exit
      end

      unless @raw.has_key? "server"
        @log.error "configuration item 'server' not found"
        exit
      end

      unless @raw["server"].kind_of? Array
        @log.error "configuration item 'server' not valid"
        exit
      end

       @raw["server"].each do |s|
         case s["type"]
           when "standalone"
             pool = Proxy::Standalone.new @log
           when "replication"
             pool = Proxy::Replication.new @log
           when "cluster"
             pool = Proxy::Cluster.new @log
           else
             @log.error "configuration item 'type' not valid"
             exit
         end

         timeout = s["timeout"].to_i

         s["upstream"].each do |u|
           pool.add u, timeout
         end

         @server << Proxy::Server.new( @log, s["listen"], pool )
       end
    end

  end

end
