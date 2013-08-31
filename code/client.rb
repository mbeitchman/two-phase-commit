# client.rb 
# client to connect to key-value store cluster

require "xmlrpc/client"

# usage
if ARGV.length == 0 || ARGV[0].casecmp("help") == 0
  puts "\nUsage: ruby client.rb <test id> or ruby client.rb --i for interactive mode"
  puts "\ntest cases:\n1: put and get functionality"
  puts "2: delete and get functionality"
  puts "3: replica timeout in vote phase while put value\n\n"
  exit
end

# connect to Master
master = XMLRPC::Client.new( "localhost", "/", 1234)
master.timeout = 40

# interactive mode
if ARGV[0].casecmp("--i") == 0

	puts "<<2PC Client>> Enter get(key), put(key, value), del(key) or exit."
	while cmd = STDIN.gets.chomp

		# parse values and make calls
		if cmd.downcase.include?("get")
			
			key = cmd.scan(/\d+/)[0]
			
			val = master.call( "MasterProcess.get", key.to_i )
			if val == false
				puts "<<2PC Client>> ERROR: failed to get value for key: #{key}"
			else
				puts "<<2PC Client>> get for key: #{key} returned value: #{val[0]}"
			end

		elsif cmd.downcase.include?("put")

			key_value = cmd.scan(/\d+/)

			val = master.call( "MasterProcess.put", key_value[0].to_i, key_value[1].to_i )
		 	puts "put #{val}"
		 	if val == true	
		 		puts "<<2PC Client>> put key: #{key_value[0]} and value: #{key_value[1]}"
		 	elsif 
		 		puts "<<2PC Client>> ERROR: failed to put key: #{key_value[0]} value: #{key_value[1]}"
		 	end

		elsif cmd.downcase.include?("del")

			key = cmd.scan(/\d+/)[0]

			val = master.call( "MasterProcess.del", key.to_i )
		 	if val == true
		 		puts "<<2PC Client>> deleted key: #{key}"
		 	else
		 		puts "<<2PC Client>> ERROR: unable to delete key: #{key}"
		 	end

		elsif cmd.casecmp("exit") == 0

			break

		elsif

			puts "Unknown command"

		end

		puts "<<2PC Client>> Enter get(key), put(key, value), del(key) or exit."
	end

# automated test mode
else

	begin

		replica_pids = Array.new
		replica_ports = [1235, 1236, 1237, 1238]

		# launch master
		master_pid = fork do
	  		exec "ruby master.rb proc0 #{replica_ports[0]} proc1 #{replica_ports[1]} proc2 #{replica_ports[2]} proc3 #{replica_ports[3]}"
		end
		
		Process.detach(master_pid)

		# launch replicas
		replica_ports.each_with_index do |replica_port, index|

			replica_proc = fork do
	  			exec "ruby replica.rb proc#{index} #{replica_port}"
			end

			replica_pids.push(replica_proc)

			Process.detach(replica_proc)
		end

		sleep 2
		puts "<<2PC Client>> Waiting for processes to start..."
		sleep 5

		# test cases
		case ARGV[0].to_i

			# test put functionality
			when 1

				# clear the data store
				master.call( "MasterProcess.del", 1 )

				res = true
				# put value to data store and retrieve the value
				val = master.call( "MasterProcess.put", 1, 2 )
			 	if val != true
			 		puts "<<2PC Client>> ERROR: failed to put key: 1 value: 2"
			 		res = false
			 	end
				
				val = master.call( "MasterProcess.get", 1 )
				if val[0] != 2
					puts "<<2PC Client>> ERROR: failed to get value for key: 1"
					res = false
				end

				# put same value - should be rejected
				val = master.call( "MasterProcess.put", 1, 2 )
				if val != false
					puts "<<2PC Client>> ERROR: Put existing for exisiting key value was not rejected."
					res = false
				end

				if res 
					puts "<<2PC Client>> Test 1 Passed"
				else
					puts "<<2PC Client>> Test 1 FAILED"
				end

			# test delete functionality
			when 2

				# make sure value is in the data store
				master.call( "MasterProcess.put", 1, 2 )

				res = true

				# del value
				val = master.call( "MasterProcess.del", 1 )
			 	if val != true
			 		puts "<<2PC Client>> ERROR: failed to delete key: 1"
			 		res = false
			 	end

			 	# make sure it doesn't exist
				val = master.call( "MasterProcess.get", 1 )
			 	if val != false
			 		puts "<<2PC Client>> ERROR: got value for deleted key: 1"
			 		res = false
			 	end

			 	# make sure the delete for a non existing value fails
			 	val = master.call( "MasterProcess.del", 1 )
			 	if val != false
			 		puts "<<2PC Client>> ERROR: deleted key that shouldn't have existed"
			 		res = false
			 	end

				if res 
					puts "<<2PC Client>> Test 2 Passed"
				else
					puts "<<2PC Client>> Test 2 FAILED"
				end

			# replica timeout in vote phase
			when 3

				# clear the data store
				master.call( "MasterProcess.del", 1 )

				res = true

				# put value to data store and retrieve the value, timeout in vote phase - should fail
				val = master.call( "MasterProcess.put", 1, 2, false, :vote_timeout)
			 	if val != true
			 		puts "<<2PC Client>> ERROR: failed to put key: 1 value: 2"
			 		res = false
			 	end
				
				# should fail
				val = master.call( "MasterProcess.get", 1 )
				if val == true
			 		puts "<<2PC Client>> ERROR: Get value for key 1. This should have failed."
			 		res = false
			 	end

				if res 
					puts "<<2PC Client>> Test 3 Passed"
				else
					puts "<<2PC Client>> Test 3 FAILED"
				end

			else
				puts "Unrecognized test case! Please enter a test (1-8) or run: ruby client.rb help to get test descriptions."
			end

	rescue Timeout::Error, Errno::ECONNREFUSED

		puts "<<2PC Client>> Unable to connect to master! Client shutting down."
		puts "<<2PC Client>> Shutting down master..."
		Process.kill "KILL", master_pid
		sleep 4
		
		replica_pids.each do |replica_pid|
			puts "<<2PC Client>> Shutting down #{replica_pid}..."
			Process.kill "KILL", replica_pid
			sleep 4
		end
	end

	puts "<<2PC Client>> Shutting down master..."
	Process.kill "KILL", master_pid
	sleep 7

	replica_pids.each do |replica_pid|
		puts "<<2PC Client>> Shutting down replica #{replica_pid}..."
		Process.kill "KILL", replica_pid
		sleep 7
	end
end



