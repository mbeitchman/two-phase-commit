# replicahelper.rb

class ReplicaRecover

	public 

		def initialize(log_path, data_store)
			@log_path = log_path
			@log = File.open(log_path, "r+")
			@data_store = data_store
			@transactions = Hash.new
		end

		def restore

			parselog

			restore_data_store

			clean_log 
		end

	private

		def parselog
			@log.each do |line|
				vals = line.split(" ")
				transaction_id = vals[vals.length-1]
				@transactions[transaction_id] = line
			end
		end

		def restore_data_store
			@transactions.each do |key, value| 
				#puts "#{key}: #{value}"
				vals = value.split(" ")
				if(vals[2] == "COMMIT")
					if(vals[1] == "put")
						if @data_store.get(vals[3].to_i) != vals[4].to_i  
							@data_store.put(vals[3].to_i, vals[4].to_i)
						end
					elsif(vals[1] == "delete")
						if @data_store.get(vals[3].to_i) != false
							@data_store.del(vals[3].to_i)
						end
					end
				end
			end
		end

		def clean_log
			File.delete(@log_path)
		end
end