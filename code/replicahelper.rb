# replicahelper.rb

class ReplicaRecover

	#constants
	ACTION_TYPE = 1
	ACTION = 2
	KEY = 3
	VALUE = 4

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
				@vals = value.split(" ")

				if(@vals[ACTION] == "COMMIT")
					restore_action
				end
			end
		end

		def restore_action
			if(@vals[ACTION_TYPE] == "put")
				restore_put(vals[KEY].to_i, vals[VALUE].to_i)
			elsif(vals[ACTION_TYPE] == "delete")
				restore_delete(vals[KEY].to_i)
			end
		end

		def restore_put(key, value)
			@data_store.put(key, value)
		end

		def restore_delete(key, value)
			@data_store.del(key, value)
		end

		def clean_log
			File.delete(@log_path)
		end
end