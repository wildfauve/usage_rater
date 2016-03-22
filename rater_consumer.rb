#require 'poseidon'
require 'json'
require 'cassandra'
require 'pry'
require 'bigdecimal'
require 'any_port'

$cluster = ::Cassandra.cluster
$session = $cluster.connect('rating')

Dir["#{Dir.pwd}/lib/*.rb"].each {|file| require file }


StreamConsumer.new.fetch
