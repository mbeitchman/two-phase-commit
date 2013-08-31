# replicaconnection.rb
# stores data about each replica connection

class ReplicaConnection

	attr_accessor :name, :port, :connection

	def initialize(name = nil, port = nil, connection = nil)
		@name = name
		@port = port
		@connection = connection
	end

end