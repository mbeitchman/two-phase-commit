# datastore.rb
# defines a key-value store implemented on top of sqlite
# keys and values must be integers and keys must be unique
# the store will create a new db file if it cannot find one it it's working directory

require "sqlite3" 

module DataStore

	class KeyValueStore

		public

			def initialize
			end

			# create a new db if needed and connect to it
			# db file is named procname.db
			def connect(processName)
				@procName = processName
				@db_name = processName + ".db"
				
				# create the new db if it doesn't exist
				if(!File.exists?(@db_name))
					create_db
				end

				@db = SQLite3::Database.open(@db_name) 
			end

			def put(key, value)

				# make sure key and values are integers
				if !key.is_a? Integer or !value.is_a? Integer
					return false
				end

				# reject the key if it exists in the datastore
				if key_exists(key)
					return false
				end

				@db.transaction do |db|
					db.execute "INSERT INTO Key_value_store VALUES(#{key}, #{value})"
				end

				return true

			end

			def del(key)

				# make sure key is an integer
				if !key.is_a? Integer
					return false
				end

				# replicas always check if the key exists before deleting it
				@db.transaction do |db|
					db.execute "DELETE FROM Key_value_store WHERE(key = #{key})"
				end
			end

			def get(key)

				# make sure key is an integer
				if !key.is_a? Integer
					return false
				end

				rows = 0
				@db.transaction do |db|
					rows = db.execute "SELECT VALUE FROM Key_value_store WHERE key = #{key}"
				end

				val = rows[0]

				# return value if it was retrieved
				if val.nil?
					false
				else
					val
				end
			end

			def close
				@db.close
			end

		private

			def create_db
				db = SQLite3::Database.new(@db_name)
				db.execute "CREATE TABLE IF NOT EXISTS Key_value_store (key int, value int)"
				db.close
			end

			def key_exists(key)
				
				rows = 0
				@db.transaction do |db|
					rows = db.execute "SELECT Value FROM Key_value_store WHERE key = #{key}"
				end

				val = rows[0]
				if val.nil?
					false
				else
					true
				end
			end
	end
end