require "logger"
require "socket"
require "rufus-scheduler"

require_relative "../lib/config"
require_relative "../lib/server"
require_relative "../lib/pool"
require_relative "../lib/standalone"
require_relative "../lib/replication"
require_relative "../lib/cluster"

log = Logger.new STDOUT

log.level = Logger::INFO
log.info "starting"

$config = Proxy::Config.new log
$config.read "config.yml"

log.level = $config.log_level

pool = Proxy::Cluster.new log

$config.proxy[0]["upstream"].each do |u|
  pool.add u, 1
end

server = Proxy::Server.new log, $config[0]["listen"], pool

scheduler = Rufus::Scheduler.new

scheduler.every "3s" do
  upstream = Proxy::Upstream.all
  pending  = upstream.select { |u| !u.connected? }

  log.info "#{self.class.to_s}##{__method__} upstream pending status #{pending.size}/#{upstream.size}"

  pending.each { |u| u.open }
end

scheduler.every "3s" do
  upstream = Proxy::Upstream.all
  active   = upstream.select { |u| u.connected? }

  log.info "#{self.class.to_s}##{__method__} upstream active status #{active.size}/#{upstream.size}"

  active.each { |u| u.check }
end

server.open
server.run

log.info "stopping"

scheduler.join
