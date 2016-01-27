require 'poseidon'
require 'json'
require 'cassandra'
require 'pry'
require 'bigdecimal'
require 'waterdrop'

$cluster = ::Cassandra.cluster
$session = $cluster.connect('rating')

WaterDrop.setup do |config|
  config.send_messages = true
  config.connection_pool_size = 20
  config.connection_pool_timeout = 1
  config.kafka_hosts = ['localhost:9092']
  config.raise_on_failure = true
end


Dir["#{Dir.pwd}/lib/*.rb"].each {|file| require file }


StreamConsumer.new.fetch
