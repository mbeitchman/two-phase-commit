#replicarpcinterface.rb

# helper class to store transaction data
class Transaction

	attr_accessor :id, :type, :key, :value, :state

	def initialize(id, type, key, value, state)
		@id = id
		@type = type
		@key = key
		@value = value
		@state = state
	end
end

class ReplicaRPCInterface

	def initialize(data_store, procName)
		@data_store = data_store
		@procName = procName
		@transaction_list = Array.new
		@active_transactions = Hash.new
		
		@file = File.open("#{procName}.log", "a+")

		 # recover state on replica initialization
		if File.exists?("#{procName}.log")
			recover = ReplicaRecover.new("#{procName}.log", @data_store)
			recover.restore()
		end
			
		@file = File.open("#{procName}.log", "w+")
		
	end

	def vote_request_put(key, value, id, timeout = false)

		# for testing purposes
		if timeout
			sleep 1000
		end

		# check if we have seen or responded to this request
		@transaction_list.each do |transaction|
			if(transaction.id == id && transaction.state == :vote_yes)
				return true
			end
		end

		# check if the transaction can be serviced and respond appropriately
		if(@data_store.get(key) != false || @active_transactions.has_key?(key))
			@file.puts("#{@procName} vr_put ABORT #{key} #{value} #{id}")
			@file.flush()
			return false
		end

		@file.puts("#{@procName} vr_put COMMIT #{key} #{value} #{id}")
		@file.flush()

		@active_transactions[key] = true

		# add transaction to list
		transaction = Transaction.new(id, :put, key, value, :vote_yes)
		@transaction_list.push(transaction)

		return true
	end

	def vote_request_del(key, id, timeout = false)

		# for testing purposes
		if timeout
			sleep 1000
		end

		# check if we have seen or responded to this request
		@transaction_list.each do |transaction|
			if(transaction.id == id && transaction.state == :vote_del)
				return true
			end
		end

		# check if the transaction can be serviced and respond appropriately
		if(@data_store.get(key) == false || @active_transactions.has_key?(key))
			@file.puts("#{@procName} vr_delete ABORT #{key} #{id}")
			@file.flush()
			return false
		end

		@file.puts("#{@procName} vr_delete COMMIT #{key} #{id}")
		@file.flush()

		@active_transactions[key] = true

		# add transaction to list
		transaction = Transaction.new(id, :del, key, nil, :vote_del)
		@transaction_list.push(transaction)

		return true
	end

	def commit(id)
		
		# handle commiting the transaction
		@transaction_list.each do |transaction|

			if(transaction.id == id)
				# todo: factor these into function
				if(transaction.type == :put)
					@file.puts("#{@procName} put COMMIT #{transaction.key} #{transaction.value} #{id}")
					@file.flush

					@active_transactions.delete(transaction.key)

					@data_store.put(transaction.key, transaction.value)
					@transaction_list.pop(@transaction_list.index(transaction))
					
					@file.puts("#{@procName} completed #{id} ")
					@file.flush
				 	return true
				else
					@file.puts("#{@procName} delete COMMIT #{transaction.key} #{transaction.value} #{id}")
					@file.flush

					@active_transactions.delete(transaction.key)

					@data_store.del(transaction.key)
					@transaction_list.pop(@transaction_list.index(transaction))

					@file.puts("#{@procName} completed #{id}")
					@file.flush
				 	return true
				end
			end
		end
	end

	def get(key)
		@data_store.get(key)
	end
end