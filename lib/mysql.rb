# coding: ascii-8bit
# Copyright (C) 2008 TOMITA Masahiro
# mailto:tommy@tmtm.org

require 'uri'

# MySQL connection class.
# @example
#  my = Mysql.connect('hostname', 'user', 'password', 'dbname')
#  res = my.query 'select col1,col2 from tbl where id=123'
#  res.each do |c1, c2|
#    p c1, c2
#  end
class Mysql

  require_relative "mysql/constants"
  require_relative "mysql/error"
  require_relative "mysql/charset"
  require_relative "mysql/protocol"
  require_relative "mysql/packet.rb"

  VERSION            = '3.0.1'             # Version number of this library
  MYSQL_UNIX_PORT    = "/tmp/mysql.sock"   # UNIX domain socket filename
  MYSQL_TCP_PORT     = 3306                # TCP socket port number

  # @!attribute [rw] host
  #   @return [String, nil]
  # @!attribute [rw] username
  #   @return [String, nil]
  # @!attribute [rw] password
  #   @return [String, nil]
  # @!attribute [rw] database
  #   @return [String, nil]
  # @!attribute [rw] port
  #   @return [Integer, String, nil]
  # @!attribute [rw] socket
  #   @return [String, nil] socket filename
  # @!attribute [rw] flags
  #   @return [Integer, nil]
  # @!attribute [rw] connect_timeout
  #   @return [Numeric, nil]
  # @!attribute [rw] read_timeout
  #   @return [Numeric, nil]
  # @!attribute [rw] write_timeout
  #   @return [Numeric, nil]
  # @!attribute [rw] init_command
  #   @return [String, nil]
  # @!attribute [rw] local_infile
  #   @return [Boolean]
  # @!attribute [rw] load_data_local_dir
  #   @return [String, nil]
  # @!attribute [rw] ssl_mode
  #   @return [String, Integer] 1 or "disabled" / 2 or "preferred" / 3 or "required"
  # @!attribute [rw] get_server_public_key
  #   @return [Boolean]
  # @!attribute [rw] connect_attrs
  #   @return [Hash]
  DEFAULT_OPTS = {
    host: nil,
    username: nil,
    password: nil,
    database: nil,
    port: nil,
    socket: nil,
    flags: 0,
    charset: nil,
    connect_timeout: nil,
    read_timeout: nil,
    write_timeout: nil,
    init_command: nil,
    local_infile: nil,
    load_data_local_dir: nil,
    ssl_mode: SSL_MODE_PREFERRED,
    get_server_public_key: false,
    connect_attrs: {},
  }.freeze

  # @private
  attr_reader :protocol

  # @return [Array<Mysql::Field>] fields of result set
  attr_reader :fields

  # @return [Mysql::Result]
  attr_reader :result

  class << self
    # Make Mysql object and connect to mysqld.
    # parameter is same as arguments for {#initialize}.
    # @return [Mysql]
    def connect(*args, **opts)
      self.new(*args, **opts).connect
    end

    # Escape special character in string.
    # @param [String] str
    # @return [String]
    def escape_string(str)
      str.gsub(/[\0\n\r\\\'\"\x1a]/) do |s|
        case s
        when "\0" then "\\0"
        when "\n" then "\\n"
        when "\r" then "\\r"
        when "\x1a" then "\\Z"
        else "\\#{s}"
        end
      end
    end
    alias quote escape_string
  end

  # @overload initialize(uri, **opts)
  #   @param uri [String, URI] "mysql://username:password@host:port/database?param=value&..." / "mysql://username:password@%2Ftmp%2Fmysql.sock/database" / "mysql://username:password@/database?socket=/tmp/mysql.sock"
  #   @param opts [Hash] options
  # @overload initialize(host, username, password, database, port, socket, flags, **opts)
  #   @param host [String] hostname mysqld running
  #   @param username [String] username to connect to mysqld
  #   @param password [String] password to connect to mysqld
  #   @param database [String] initial database name
  #   @param port [String] port number (used if host is not 'localhost' or nil)
  #   @param socket [String] socket filename (used if host is 'localhost' or nil)
  #   @param flags [Integer] connection flag. Mysql::CLIENT_* ORed
  #   @param opts [Hash] options
  # @overload initialize(host: nil, username: nil, password: nil, database: nil, port: nil, socket: nil, flags: nil, **opts)
  #   @param host [String] hostname mysqld running
  #   @param username [String] username to connect to mysqld
  #   @param password [String] password to connect to mysqld
  #   @param database [String] initial database name
  #   @param port [String] port number (used if host is not 'localhost' or nil)
  #   @param socket [String] socket filename (used if host is 'localhost' or nil)
  #   @param flags [Integer] connection flag. Mysql::CLIENT_* ORed
  #   @param opts [Hash] options
  #   @option opts :host [String] hostname mysqld running
  #   @option opts :username [String] username to connect to mysqld
  #   @option opts :password [String] password to connect to mysqld
  #   @option opts :database [String] initial database name
  #   @option opts :port [String] port number (used if host is not 'localhost' or nil)
  #   @option opts :socket [String] socket filename (used if host is 'localhost' or nil)
  #   @option opts :flags [Integer] connection flag. Mysql::CLIENT_* ORed
  #   @option opts :charset [Mysql::Charset, String] character set
  #   @option opts :connect_timeout [Numeric, nil]
  #   @option opts :read_timeout [Numeric, nil]
  #   @option opts :write_timeout [Numeric, nil]
  #   @option opts :local_infile [Boolean]
  #   @option opts :load_data_local_dir [String]
  #   @option opts :ssl_mode [Integer]
  #   @option opts :get_server_public_key [Boolean]
  #   @option opts :connect_attrs [Hash]
  def initialize(*args, **opts)
    @fields = nil
    @result = nil
    @protocol = nil
    @sqlstate = "00000"
    @host_info = nil
    @last_error = nil
    @opts = DEFAULT_OPTS.dup
    parse_args(args, opts)
  end

  # Connect to mysqld.
  # parameter is same as arguments for {#initialize}.
  # @return [Mysql] self
  def connect(*args, **opts)
    parse_args(args, opts)
    if @opts[:flags] & CLIENT_COMPRESS != 0
      warn 'unsupported flag: CLIENT_COMPRESS' if $VERBOSE
      @opts[:flags] &= ~CLIENT_COMPRESS
    end
    @protocol = Protocol.new(@opts)
    @protocol.authenticate
    @host_info = (@opts[:host].nil? || @opts[:host] == "localhost") ? 'Localhost via UNIX socket' : "#{@opts[:host]} via TCP/IP"
    query @opts[:init_command] if @opts[:init_command]
    return self
  end

  def parse_args(args, opts)
    case args[0]
    when URI
      uri = args[0]
    when /\Amysql:\/\//
      uri = URI.parse(args[0])
    when String
      @opts[:host], user, passwd, dbname, port, socket, flags = *args
      @opts[:username] = user if user
      @opts[:password] = passwd if passwd
      @opts[:database] = dbname if dbname
      @opts[:port] = port if port
      @opts[:socket] = socket if socket
      @opts[:flags] = flags if flags
    when Hash
      # skip
    when nil
      # skip
    end
    if uri
      host = uri.hostname.to_s
      host = URI.decode_www_form_component(host)
      if host.start_with?('/')
        @opts[:socket] = host
        host = ''
      end
      @opts[:host] = host
      @opts[:username] = URI.decode_www_form_component(uri.user.to_s)
      @opts[:password] = URI.decode_www_form_component(uri.password.to_s)
      @opts[:database] = uri.path.sub(/\A\/+/, '')
      @opts[:port] = uri.port
      opts = URI.decode_www_form(uri.query).to_h.transform_keys(&:intern).merge(opts) if uri.query
      opts[:flags] = opts[:flags].to_i if opts[:flags]
    end
    if args.last.kind_of? Hash
      opts = opts.merge(args.last)
    end
    @opts.update(opts)
  end

  DEFAULT_OPTS.each_key do |var|
    next if var == :charset
    define_method(var){@opts[var]}
    define_method("#{var}="){|val| @opts[var] = val}
  end

  # Disconnect from mysql.
  # @return [Mysql] self
  def close
    if @protocol
      @protocol.quit_command
      @protocol = nil
    end
    return self
  end

  # Disconnect from mysql without QUIT packet.
  # @return [Mysql] self
  def close!
    if @protocol
      @protocol.close
      @protocol = nil
    end
    return self
  end

  # Escape special character in MySQL.
  #
  # @param [String] str
  # return [String]
  def escape_string(str)
    self.class.escape_string str
  end
  alias quote escape_string

  # @return [Mysql::Charset] character set of MySQL connection
  def charset
    @opts[:charset]
  end

  # Set charset of MySQL connection.
  # @param [String, Mysql::Charset] cs
  def charset=(cs)
    charset = cs.is_a?(Charset) ? cs : Charset.by_name(cs)
    if @protocol
      @protocol.charset = charset
      query "SET NAMES #{charset.name}"
    end
    @opts[:charset] = charset
    cs
  end

  # @return [String] charset name
  def character_set_name
    @protocol.charset.name
  end

  # @return [Integer] last error number
  def errno
    @last_error ? @last_error.errno : 0
  end

  # @return [String] last error message
  def error
    @last_error && @last_error.error
  end

  # @return [String] sqlstate for last error
  def sqlstate
    @last_error ? @last_error.sqlstate : "00000"
  end

  # @return [Integer] number of columns for last query
  def field_count
    @fields.size
  end

  # @return [String] connection type
  def host_info
    @host_info
  end

  # @return [String] server version
  def server_info
    check_connection
    @protocol.server_info
  end

  # @return [Integer] server version
  def server_version
    check_connection
    @protocol.server_version
  end

  # @return [String] information for last query
  def info
    @protocol && @protocol.message
  end

  # @return [Integer] number of affected records by insert/update/delete.
  def affected_rows
    @protocol ? @protocol.affected_rows : 0
  end

  # @return [Integer] latest auto_increment value
  def insert_id
    @protocol ? @protocol.insert_id : 0
  end

  # @return [Integer] number of warnings for previous query
  def warning_count
    @protocol ? @protocol.warning_count : 0
  end

  # Kill query.
  # @param [Integer] pid thread id
  # @return [Mysql] self
  def kill(pid)
    check_connection
    @protocol.kill_command pid
    self
  end

  # Execute query string.
  # @param str [String] Query.
  # @param return_result [Boolean]
  # @param yield_null_result [Boolean]
  # @return [Mysql::Result] if return_result is true and the query returns result set.
  # @return [nil] if return_results is true and the query does not return result set.
  # @return [self] if return_result is false or block is specified.
  # @example
  #  my.query("select 1,NULL,'abc'").fetch  # => [1, nil, "abc"]
  #  my.query("select 1,NULL,'abc'"){|res| res.fetch}
  def query(str, return_result: true, yield_null_result: true, &block)
    check_connection
    @fields = nil
    begin
      res = nil
      nfields = @protocol.query_command str
      if block
        while true
          if nfields
            @fields = @protocol.retr_fields(nfields)
            block.call Result.new(@fields, @protocol)
          elsif yield_null_result
            block.call nil
          end
          break unless more_results?
          nfields = @protocol.get_result
        end
        return self
      end
      if nfields
        @fields = @protocol.retr_fields(nfields)
        @result = Result.new(@fields, @protocol)
      end
      return self unless return_result
      return nil unless nfields
      return @result
    rescue ServerError => e
      @last_error = e
      @sqlstate = e.sqlstate
      raise
    end
  end

  # Get all data for last query.
  # @return [Mysql::Result]
  def store_result
    @result
  end

  # @return [Integer] Thread ID
  def thread_id
    check_connection
    @protocol.thread_id
  end

  # Set server option.
  # @param [Integer] opt {Mysql::OPTION_MULTI_STATEMENTS_ON} or {Mysql::OPTION_MULTI_STATEMENTS_OFF}
  # @return [Mysql] self
  def set_server_option(opt)
    check_connection
    @protocol.set_option_command opt
    self
  end

  # @return [Boolean] true if multiple queries are specified and unexecuted queries exists.
  def more_results?
    @protocol.more_results?
  end

  # execute next query if multiple queries are specified.
  # @return [Mysql::Result] result set of query if return_result is true.
  # @return [true] if return_result is false and result exists.
  # @return [nil] query returns no results.
  def next_result(return_result: true)
    return nil unless more_results?
    @fields = nil
    nfields = @protocol.get_result
    if nfields
      @fields = @protocol.retr_fields nfields
      @result = Result.new(@fields, @protocol)
    end
    return true unless return_result
    return nil unless nfields
    @result
  end

  # Parse prepared-statement.
  # @param [String] str query string
  # @return [Mysql::Stmt] Prepared-statement object
  def prepare(str)
    st = Stmt.new @protocol
    st.prepare str
    st
  end

  # @private
  # Make empty prepared-statement object.
  # @return [Mysql::Stmt] If block is not specified.
  def stmt
    Stmt.new @protocol
  end

  # Check whether the  connection is available.
  # @return [Mysql] self
  def ping
    check_connection
    @protocol.ping_command
    self
  end

  # Flush tables or caches.
  # @param [Integer] op operation. Use Mysql::REFRESH_* value.
  # @return [Mysql] self
  def refresh(op)
    check_connection
    @protocol.refresh_command op
    self
  end

  # Reload grant tables.
  # @return [Mysql] self
  def reload
    refresh Mysql::REFRESH_GRANT
  end

  # Select default database
  # @return [Mysql] self
  def select_db(db)
    query "use #{db}"
    self
  end

  # shutdown server.
  # @return [Mysql] self
  def shutdown(level=0)
    check_connection
    @protocol.shutdown_command level
    self
  end

  # @return [String] statistics message
  def stat
    @protocol ? @protocol.statistics_command : 'MySQL server has gone away'
  end

  # Commit transaction
  # @return [Mysql] self
  def commit
    query 'commit'
    self
  end

  # Rollback transaction
  # @return [Mysql] self
  def rollback
    query 'rollback'
    self
  end

  # Set autocommit mode
  # @param [Boolean] flag
  # @return [Mysql] self
  def autocommit(flag)
    query "set autocommit=#{flag ? 1 : 0}"
    self
  end

  # session track
  # @return [Hash]
  def session_track
    @protocol.session_track
  end

  private

  def check_connection
    raise ClientError, 'MySQL client is not connected' unless @protocol
  end

  # @!visibility public
  # Field class
  class Field
    # @return [String] database name
    attr_reader :db
    # @return [String] table name
    attr_reader :table
    # @return [String] original table name
    attr_reader :org_table
    # @return [String] field name
    attr_reader :name
    # @return [String] original field name
    attr_reader :org_name
    # @return [Integer] charset id number
    attr_reader :charsetnr
    # @return [Integer] field length
    attr_reader :length
    # @return [Integer] field type
    attr_reader :type
    # @return [Integer] flag
    attr_reader :flags
    # @return [Integer] number of decimals
    attr_reader :decimals
    # @return [String] defualt value
    attr_reader :default
    alias :def :default

    # @private
    attr_accessor :result

    # @attr [Protocol::FieldPacket] packet
    def initialize(packet)
      @db, @table, @org_table, @name, @org_name, @charsetnr, @length, @type, @flags, @decimals, @default =
        packet.db, packet.table, packet.org_table, packet.name, packet.org_name, packet.charsetnr, packet.length, packet.type, packet.flags, packet.decimals, packet.default
      @flags |= NUM_FLAG if is_num_type?
      @max_length = nil
    end

    # @return [Hash] field information
    def to_hash
      {
        "name"       => @name,
        "table"      => @table,
        "def"        => @default,
        "type"       => @type,
        "length"     => @length,
        "max_length" => max_length,
        "flags"      => @flags,
        "decimals"   => @decimals
      }
    end

    # @private
    def inspect
      "#<Mysql::Field:#{@name}>"
    end

    # @return [Boolean] true if numeric field.
    def is_num?
      @flags & NUM_FLAG != 0
    end

    # @return [Boolean] true if not null field.
    def is_not_null?
      @flags & NOT_NULL_FLAG != 0
    end

    # @return [Boolean] true if primary key field.
    def is_pri_key?
      @flags & PRI_KEY_FLAG != 0
    end

    # @return [Integer] maximum width of the field for the result set
    def max_length
      return @max_length if @max_length
      @max_length = 0
      @result.calculate_field_max_length if @result
      @max_length
    end

    attr_writer :max_length

    private

    def is_num_type?
      [TYPE_DECIMAL, TYPE_TINY, TYPE_SHORT, TYPE_LONG, TYPE_FLOAT, TYPE_DOUBLE, TYPE_LONGLONG, TYPE_INT24].include?(@type) || (@type == TYPE_TIMESTAMP && (@length == 14 || @length == 8))
    end

  end

  # @!visibility public
  # Result set
  class ResultBase
    include Enumerable

    # @return [Array<Mysql::Field>] field list
    attr_reader :fields

    # @return [Mysql::StatementResult]
    attr_reader :result

    # @param [Array of Mysql::Field] fields
    def initialize(fields)
      @fields = fields
      @field_index = 0             # index of field
      @records = []                # all records
      @index = 0                   # index of record
      @fieldname_with_table = nil
      @fetched_record = nil
    end

    # ignore
    # @return [void]
    def free
    end

    # @return [Integer] number of record
    def size
      @records.size
    end
    alias num_rows size

    # @return [Array] current record data
    def fetch
      @fetched_record = nil
      return nil if @index >= @records.size
      @records[@index] = @records[@index].to_a unless @records[@index].is_a? Array
      @fetched_record = @records[@index]
      @index += 1
      return @fetched_record
    end
    alias fetch_row fetch

    # Return data of current record as Hash.
    # The hash key is field name.
    # @param [Boolean] with_table if true, hash key is "table_name.field_name".
    # @return [Hash] current record data
    def fetch_hash(with_table=nil)
      row = fetch
      return nil unless row
      if with_table and @fieldname_with_table.nil?
        @fieldname_with_table = @fields.map{|f| [f.table, f.name].join(".")}
      end
      ret = {}
      @fields.each_index do |i|
        fname = with_table ? @fieldname_with_table[i] : @fields[i].name
        ret[fname] = row[i]
      end
      ret
    end

    # Iterate block with record.
    # @yield [Array] record data
    # @return [self] self. If block is not specified, this returns Enumerator.
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
    # @return [self] self. If block is not specified, this returns Enumerator.
    def each_hash(with_table=nil, &block)
      return enum_for(:each_hash, with_table) unless block
      while rec = fetch_hash(with_table)
        block.call rec
      end
      self
    end

    # Set record position
    # @param [Integer] n record index
    # @return [self] self
    def data_seek(n)
      @index = n
      self
    end

    # @return [Integer] current record position
    def row_tell
      @index
    end

    # Set current position of record
    # @param [Integer] n record index
    # @return [Integer] previous position
    def row_seek(n)
      ret = @index
      @index = n
      ret
    end
  end

  # @!visibility public
  # Result set for simple query
  class Result < ResultBase
    # @private
    # @param [Array<Mysql::Field>] fields
    # @param [Mysql::Protocol] protocol
    def initialize(fields, protocol=nil)
      super fields
      return unless protocol
      @records = protocol.retr_all_records fields
      fields.each{|f| f.result = self}  # for calculating max_field
    end

    # @private
    # calculate max_length of all fields
    def calculate_field_max_length
      max_length = Array.new(@fields.size, 0)
      @records.each_with_index do |rec, i|
        rec = @records[i] = rec.to_a if rec.is_a? RawRecord
        max_length.each_index do |j|
          max_length[j] = rec[j].length if rec[j] && rec[j].length > max_length[j]
        end
      end
      max_length.each_with_index do |len, i|
        @fields[i].max_length = len
      end
    end

    # @return [Mysql::Field] current field
    def fetch_field
      return nil if @field_index >= @fields.length
      ret = @fields[@field_index]
      @field_index += 1
      ret
    end

    # @return [Integer] current field position
    def field_tell
      @field_index
    end

    # Set field position
    # @param [Integer] n field index
    # @return [Integer] previous position
    def field_seek(n)
      ret = @field_index
      @field_index = n
      ret
    end

    # Return specified field
    # @param [Integer] n field index
    # @return [Mysql::Field] field
    def fetch_field_direct(n)
      raise ClientError, "invalid argument: #{n}" if n < 0 or n >= @fields.length
      @fields[n]
    end

    # @return [Array<Mysql::Field>] all fields
    def fetch_fields
      @fields
    end

    # @return [Array<Integer>] length of each fields
    def fetch_lengths
      return nil unless @fetched_record
      @fetched_record.map{|c|c.nil? ? 0 : c.length}
    end

    # @return [Integer] number of fields
    def num_fields
      @fields.size
    end
  end

  # @!visibility private
  # Result set for prepared statement
  class StatementResult < ResultBase
    # @private
    # @param [Array<Mysql::Field>] fields
    # @param [Mysql::Protocol] protocol
    def initialize(fields, protocol)
      super fields
      @records = protocol.stmt_retr_all_records @fields, protocol.charset
    end
  end

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
    def execute(*values, return_result: true, yield_null_result: true, &block)
      raise ClientError, "not prepared" unless @param_count
      raise ClientError, "parameter count mismatch" if values.length != @param_count
      values = values.map{|v| @protocol.charset.convert v}
      begin
        @sqlstate = "00000"
        @protocol.stmt_execute_command @statement_id, values
        @fields = @result = nil
        nfields = @protocol.get_result
        if block
          while true
            if nfields
              @fields = @protocol.retr_fields nfields
              block.call StatementResult.new(@fields, @protocol)
            elsif yield_null_result
              @affected_rows, @insert_id, @server_status, @warning_count, @info =
                @protocol.affected_rows, @protocol.insert_id, @protocol.server_status, @protocol.warning_count, @protocol.message
              block.call nil
            end
            break unless more_results?
            nfields = @protocol.get_result
          end
          return self
        end
        if nfields
          @fields = @protocol.retr_fields nfields
          @result = StatementResult.new(@fields, @protocol)
        else
          @affected_rows, @insert_id, @server_status, @warning_count, @info =
            @protocol.affected_rows, @protocol.insert_id, @protocol.server_status, @protocol.warning_count, @protocol.message
        end
        return self unless return_result
        return nil unless nfields
        return @result
      rescue ServerError => e
        @last_error = e
        @sqlstate = e.sqlstate
        raise
      end
    end

    def more_results?
      @protocol.more_results?
    end

    # execute next query if precedure is called.
    # @return [Mysql::Result] result set of query if return_result is true.
    # @return [true] if return_result is false and result exists.
    # @return [nil] query returns no results or no more results.
    def next_result(return_result: true)
      return nil unless more_results?
      @fields = @result = nil
      nfields = @protocol.get_result
      if nfields
        @fields = @protocol.retr_fields nfields
        @result = StatementResult.new(@fields, @protocol)
      else
        @affected_rows, @insert_id, @server_status, @warning_count, @info =
          @protocol.affected_rows, @protocol.insert_id, @protocol.server_status, @protocol.warning_count, @protocol.message
      end
      return true unless return_result
      return nil unless nfields
      return @result
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
    # Use fetch_fields to get list of fields.
    # @return [Mysql::Result]
    def result_metadata
      return nil if @fields.empty?
      Result.new @fields
    end
  end
end
