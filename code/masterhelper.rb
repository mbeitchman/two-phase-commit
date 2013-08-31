# masterhelper.rb

require './masterrpcinterface.rb'

class MasterRecover

	public 
		def initialize(log_path, data_store, master)
			@log_path = log_path
			@log = File.open(log_path, "r+")
			@data_store = data_store
			@master = master
			@transactions = Hash.new
		end

		def restore

			# parse log contents
			parselog

			# replay actions needed to do recovery
			restart_phase

			# clean the log
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

		def restart_phase
			@transactions.each do |key, value| 
				vals = value.split(" ")
				if vals[1] == "start2PC"
					if vals[2] == "put"
						@master.put(key, value)
					elsif vals[2] == "del"
						@master.del(key)
					end
				elsif vals[1].include?("commit")
					if vals[1] == "commit_put"
						@master.put(key, value, true)
					elsif vals[1] == "commit_del"
						@master.del(key, true)
					end
				end 
			end
		end

		def clean_log
			File.delete(@log_path)
		end
end