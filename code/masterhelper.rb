# masterhelper.rb

require './masterrpcinterface.rb'

class MasterRecover

	#constants
	ACTION_TYPE = 1
	ACTION = 2

	public 
		def initialize(log_path, data_store, master)
			@log_path = log_path
			@log = File.open(log_path, "r+")
			@data_store = data_store
			@master = master
			@transactions = Hash.new
		end

		def restore

			parselog

			restart_phase

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
				@vals = value.split(" ")
				if @vals[ACTION_TYPE] == "start2PC"
					redo_vote_phase(key, value)
				elsif @vals[ACTION_TYPE].include?("commit")
					redo_commit_phase(key, value)
				end 
			end
		end

		def redo_vote_phase(key, value)
			if @vals[ACTION] == "put"
				@master.put(key, value)
			elsif @vals[ACTION] == "del"
				@master.del(key, value)
			end
		end

		def redo_commit_phase
			if @vals[ACTION_TYPE] == "commit_put"
				@master.put(key, value, true)
			elsif @vals[ACTION_TYPE] == "commit_del"
				@master.del(key, true)
			end
		end

		def clean_log
			File.delete(@log_path)
		end
end