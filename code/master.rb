# master.rb

require "xmlrpc/server"
require "xmlrpc/client"
require "sqlite3" 
require "./masterhelper.rb"
require './replicaconnection.rb'
require './masterrpcinterface.rb'

# usage
if ARGV.length == 0
  puts "Usage: ruby master.rb <replicaname1> <replicaport1> <replicaname2> <replicaport2> <replicanameN> <replicaportN>\n** master runs on port 1234 so a replica cannot be run on port 1234 **"
  exit
end

# list of replicas
replicas = Array.new

# parse args and connect to replicas rpc servers
ARGV.each_slice(2) do |arg|
  replica = ReplicaConnection.new(arg[0], arg[1], XMLRPC::Client.new( "localhost", "/", arg[1]))
  replicas.push(replica)
end

# bring up master rpc server
server = XMLRPC::Server.new( 1234 )
server.add_handler( "MasterProcess", MasterRPCInterface.new(replicas) )
server.serve