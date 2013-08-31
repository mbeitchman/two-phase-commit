# masterrpcintrface.rb

class MasterRPCInterface

  def initialize(replicas)
    @replicas = replicas
    @id = 0

    @file = File.open("master.log", "a+")

    # recover state on master initialization
    if File.exists?("master.log")
      recover = MasterRecover.new("master.log", @data_store, self)
      recover.restore()
    end
      
    @file = File.open("master.log", "w+")

  end

  def get(key)
    # pick a random replica and get the value from it
    index = Random.rand(@replicas.length)
    begin
      @replicas[index].connection.call( "#{@replicas[index].name}.get", key)
    rescue Timeout::Error
      "Timeout trying to get(#{key}) from replica #{index}"
    end
  end

  def put(key, value, skipvote = false, timeout = :false)
    
    result = true;
    
    # vote phase
    @file.puts("master start2PC put #{@id}")
    @file.flush

    if !skipvote
      begin
        @replicas.each do |replica|
          
          if timeout == :vote_timeout # test hook to timeout a replica
            replica.connection.call( "#{replica.name}.vote_request_put", key, value, @id, true)
          elsif replica.connection.call( "#{replica.name}.vote_request_put", key, value, @id) == false
            result = false
            break
          end
        
        end
      rescue Timeout::Error
          @file.puts("master abort_put #{@id} TIMEOUT")
          @file.flush
          @id = @id + 1
          return false
      end
    end

    # commit phase
    if result == true
      @file.puts("master commit_put #{@id}")
      @file.flush

      begin
        @replicas.each do |replica|
          result = replica.connection.call( "#{replica.name}.commit", @id)
        end
      rescue Timeout::Error
        @file.puts("CATASTROPHIC ERROR master commit_put #{@id} TIMEOUT ")
        @file.flush
        @id = @id + 1
        return nil
      end
    else
      @file.puts("master abort_put #{@id}")
      @file.flush
    end

    @file.puts("master complete #{@id}")
    @file.flush

    @id = @id + 1
    return result

  end


  def del(key, skipvote = false, timeout = :false)

    result = true;

    @file.puts("master start2PC del #{@id}")
    @file.flush

    # vote phase
    if !skipvote
      begin
        @replicas.each do |replica|
          if timeout == :vote_timeout # test hook to timeout a replica
            replica.connection.call( "#{replica.name}.vote_request_del", key, @id, true)
          elsif replica.connection.call( "#{replica.name}.vote_request_del", key, @id) == false
            result = false
            break
          end
        end
      rescue Timeout::Error
          @file.puts("master abort_del #{@id} TIMEOUT")
          @file.flush
          @id = @id + 1
          return false
      end
    end

    # commit phase
    if result == true
      @file.puts("master commit_del #{@id}")
      @file.flush

      begin
        @replicas.each do |replica|
          result = replica.connection.call( "#{replica.name}.commit", @id)
        end
      rescue
        @file.puts("CATASTROPHIC ERROR master commit_del #{@id} TIMEOUT ")
        @file.flush
        @id = @id + 1
        return nil        
      end
    else
      @file.puts("master abort_del #{@id}")
      @file.flush
    end

    @file.puts("master complete #{@id}")
    @file.flush

    @id = @id + 1
    return result
    
  end

end