# replica.rb

require "xmlrpc/server"
require "./replicahelper.rb"
require "./replicarpcinterface.rb"
require "./DataStore.rb"

# usage
if ARGV.length != 2
  puts "Usage: ruby replica.rb <replicaname> <replicaport>"
  exit
end

# parse args
procName = ARGV[0]
port = ARGV[1]

# initialize key-value store
data_store = DataStore::KeyValueStore.new
data_store.connect( procName )

# bring up replica's rpc server
server = XMLRPC::Server.new( port )
server.add_handler( procName, ReplicaRPCInterface.new(data_store, procName) )
server.serve
