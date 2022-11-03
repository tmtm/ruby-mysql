class Mysql
  # @!visibility public
  # Prepared statement
  # @!attribute [r] affected_rows
  #   @return [Integer]
  # @!attribute [r] insert_id
  #   @return [Integer]
  # @!attribute [r] server_status
  #   @return [Integer]
  # @!attribute [r] warning_count
  #   @return [Integer]
  # @!attribute [r] param_count
  #   @return [Integer]
  # @!attribute [r] fields
  #   @return [Array<Mysql::Field>]
  # @!attribute [r] sqlstate
  #   @return [String]
  class Stmt
    include Enumerable

    attr_reader :affected_rows, :info, :insert_id, :server_status, :warning_count
    attr_reader :param_count, :fields, :sqlstate

    # @private
    def self.finalizer(protocol, statement_id)
      proc do
        protocol.gc_stmt statement_id
      end
    end

    # @private
    # @param [Mysql::Protocol] protocol
    def initialize(protocol)
      @protocol = protocol
      @statement_id = nil
      @affected_rows = @insert_id = @server_status = @warning_count = 0
      @sqlstate = "00000"
      @param_count = nil
    end

    # @private
    # parse prepared-statement and return {Mysql::Stmt} object
    # @param [String] str query string
    # @return self
    def prepare(str)
      raise ClientError, 'MySQL client is not connected' unless @protocol
      close
      begin
        @sqlstate = "00000"
        @statement_id, @param_count, @fields = @protocol.stmt_prepare_command(str)
      rescue ServerError => e
        @last_error = e
        @sqlstate = e.sqlstate
        raise
      end
      ObjectSpace.define_finalizer(self, self.class.finalizer(@protocol, @statement_id))
      self
    end

    # Execute prepared statement.
    # @param [Object] values values passed to query
    # @return [Mysql::Result] if return_result is true and the query returns result set.
    # @return [nil] if return_results is true and the query does not return result set.
    # @return [self] if return_result is false or block is specified.
    def execute(*values, return_result: true, yield_null_result: true, bulk_retrieve: true, &block)
      raise ClientError, "not prepared" unless @param_count
      raise ClientError, "parameter count mismatch" if values.length != @param_count
      values = values.map{|v| @protocol.charset.convert v}
      begin
        @sqlstate = "00000"
        @protocol.stmt_execute_command @statement_id, values
        @fields = @result = nil
        if block
          while true
            get_result
            res = store_result(bulk_retrieve: bulk_retrieve)
            block.call res if res || yield_null_result
            break unless more_results?
          end
          return self
        end
        get_result
        return self unless return_result
        return store_result(bulk_retrieve: bulk_retrieve)
      rescue ServerError => e
        @last_error = e
        @sqlstate = e.sqlstate
        raise
      end
    end

    def get_result
      @protocol.get_result
      @affected_rows, @insert_id, @server_status, @warning_count, @info =
        @protocol.affected_rows, @protocol.insert_id, @protocol.server_status, @protocol.warning_count, @protocol.message
    end

    def store_result(bulk_retrieve: true)
      return nil if @protocol.field_count.nil? || @protocol.field_count == 0
      @fields = @protocol.retr_fields
      @result = StatementResult.new(@fields, @protocol, bulk_retrieve: bulk_retrieve)
    end

    def more_results?
      @protocol.more_results?
    end

    # execute next query if precedure is called.
    # @return [Mysql::StatementResult] result set of query if return_result is true.
    # @return [true] if return_result is false and result exists.
    # @return [nil] query returns no results or no more results.
    def next_result(return_result: true)
      return nil unless more_results?
      @fields = @result = nil
      get_result
      return self unless return_result
      return store_result
    rescue ServerError => e
      @last_error = e
      @sqlstate = e.sqlstate
      raise
    end

    # Close prepared statement
    # @return [void]
    def close
      ObjectSpace.undefine_finalizer(self)
      @protocol.stmt_close_command @statement_id if @statement_id
      @statement_id = nil
    end

    # @return [Array] current record data
    def fetch
      @result.fetch
    end

    # Return data of current record as Hash.
    # The hash key is field name.
    # @param [Boolean] with_table if true, hash key is "table_name.field_name".
    # @return [Hash] record data
    def fetch_hash(with_table=nil)
      @result.fetch_hash with_table
    end

    # Iterate block with record.
    # @yield [Array] record data
    # @return [Mysql::Stmt] self
    # @return [Enumerator] If block is not specified
    def each(&block)
      return enum_for(:each) unless block
      while rec = fetch
        block.call rec
      end
      self
    end

    # Iterate block with record as Hash.
    # @param [Boolean] with_table if true, hash key is "table_name.field_name".
    # @yield [Hash] record data
    # @return [Mysql::Stmt] self
    # @return [Enumerator] If block is not specified
    def each_hash(with_table=nil, &block)
      return enum_for(:each_hash, with_table) unless block
      while rec = fetch_hash(with_table)
        block.call rec
      end
      self
    end

    # @return [Integer] number of record
    def size
      @result.size
    end
    alias num_rows size

    # Set record position
    # @param [Integer] n record index
    # @return [void]
    def data_seek(n)
      @result.data_seek(n)
    end

    # @return [Integer] current record position
    def row_tell
      @result.row_tell
    end

    # Set current position of record
    # @param [Integer] n record index
    # @return [Integer] previous position
    def row_seek(n)
      @result.row_seek(n)
    end

    # @return [Integer] number of columns for last query
    def field_count
      @fields.length
    end

    # ignore
    # @return [void]
    def free_result
    end

    # Returns Mysql::Result object that is empty.
    # Use fields to get list of fields.
    # @return [Mysql::Result]
    def result_metadata
      return nil if @fields.empty?
      Result.new @fields
    end
  end
end
