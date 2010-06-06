# Copyright (C) 2008-2010 TOMITA Masahiro
# mailto:tommy@tmtm.org

# MySQL connection class.
# === Example
#  my = Mysql.connect('hostname', 'user', 'password', 'dbname')
#  res = my.query 'select col1,col2 from tbl where id=123'
#  res.each do |c1, c2|
#    p c1, c2
#  end
class Mysql

  dir = File.dirname __FILE__
  require "#{dir}/mysql/constants"
  require "#{dir}/mysql/error"
  require "#{dir}/mysql/charset"
  require "#{dir}/mysql/protocol"

  VERSION            = 20903               # Version number of this library
  MYSQL_UNIX_PORT    = "/tmp/mysql.sock"   # UNIX domain socket filename
  MYSQL_TCP_PORT     = 3306                # TCP socket port number

  attr_reader :charset               # character set of MySQL connection
  attr_reader :affected_rows         # number of affected records by insert/update/delete.
  attr_reader :warning_count         # number of warnings for previous query
  attr_reader :protocol              # :nodoc:

  attr_accessor :query_with_result

  class << self
    # Make Mysql object without connecting.
    def init
      my = self.allocate
      my.instance_eval{initialize}
      my
    end

    # Make Mysql object and connect to mysqld.
    # Arguments are same as Mysql#connect.
    def new(*args)
      my = self.init
      my.connect(*args)
    end

    alias real_connect new
    alias connect new

    # Escape special character in string.
    # === Argument
    # str :: [String]
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

    # Return client version as String.
    # This value is dummy.
    def client_info
      "5.0.0"
    end
    alias get_client_info client_info

    # Return client version as Integer.
    # This value is dummy. If you want to get version of this library, use Mysql::VERSION.
    def client_version
      50000
    end
    alias get_client_version client_version
  end

  def initialize  # :nodoc:
    @fields = nil
    @protocol = nil
    @charset = nil
    @connect_timeout = nil
    @read_timeout = nil
    @write_timeout = nil
    @init_command = nil
    @affected_rows = nil
    @warning_count = 0
    @sqlstate = "00000"
    @query_with_result = true
    @host_info = nil
    @info = nil
    @last_error = nil
    @result_exist = false
    @local_infile = nil
  end

  # Connect to mysqld.
  # === Argument
  # host   :: [String / nil] hostname mysqld running
  # user   :: [String / nil] username to connect to mysqld
  # passwd :: [String / nil] password to connect to mysqld
  # db     :: [String / nil] initial database name
  # port   :: [Integer / nil] port number (used if host is not 'localhost' or nil)
  # socket :: [String / nil] socket file name (used if host is 'localhost' or nil)
  # flag   :: [Integer / nil] connection flag. Mysql::CLIENT_* ORed
  # === Return
  # self
  def connect(host=nil, user=nil, passwd=nil, db=nil, port=nil, socket=nil, flag=nil)
    @protocol = Protocol.new host, port, socket, @connect_timeout, @read_timeout, @write_timeout
    @protocol.authenticate user, passwd, db, (@local_infile ? CLIENT_LOCAL_FILES : 0) | (flag || 0), @charset
    @charset ||= @protocol.charset
    @host_info = (host.nil? || host == "localhost") ? 'Localhost via UNIX socket' : "#{host} via TCP/IP"
    query @init_command if @init_command
    return self
  end
  alias real_connect connect

  # Disconnect from mysql.
  def close
    if @protocol
      @protocol.quit_command
      @protocol = nil
    end
    return self
  end

  # Set option for connection.
  #
  # Available options:
  #   Mysql::INIT_COMMAND, Mysql::OPT_CONNECT_TIMEOUT, Mysql::OPT_READ_TIMEOUT,
  #   Mysql::OPT_WRITE_TIMEOUT, Mysql::SET_CHARSET_NAME
  # === Argument
  # opt   :: [Integer] option
  # value :: option value that is depend on opt
  # === Return
  # self
  def options(opt, value=nil)
    case opt
    when Mysql::INIT_COMMAND
      @init_command = value.to_s
#    when Mysql::OPT_COMPRESS
    when Mysql::OPT_CONNECT_TIMEOUT
      @connect_timeout = value
#    when Mysql::GUESS_CONNECTION
    when Mysql::OPT_LOCAL_INFILE
      @local_infile = value
#    when Mysql::OPT_NAMED_PIPE
#    when Mysql::OPT_PROTOCOL
    when Mysql::OPT_READ_TIMEOUT
      @read_timeout = value.to_i
#    when Mysql::OPT_RECONNECT
#    when Mysql::SET_CLIENT_IP
#    when Mysql::OPT_SSL_VERIFY_SERVER_CERT
#    when Mysql::OPT_USE_EMBEDDED_CONNECTION
#    when Mysql::OPT_USE_REMOTE_CONNECTION
    when Mysql::OPT_WRITE_TIMEOUT
      @write_timeout = value.to_i
#    when Mysql::READ_DEFAULT_FILE
#    when Mysql::READ_DEFAULT_GROUP
#    when Mysql::REPORT_DATA_TRUNCATION
#    when Mysql::SECURE_AUTH
#    when Mysql::SET_CHARSET_DIR
    when Mysql::SET_CHARSET_NAME
      @charset = Charset.by_name value.to_s
#    when Mysql::SHARED_MEMORY_BASE_NAME
    else
      warn "option not implemented: #{opt}"
    end
    self
  end

  # Escape special character in MySQL.
  # === Note
  # In Ruby 1.8, this is not safe for multibyte charset such as 'SJIS'.
  # You should use place-holder in prepared-statement.
  def escape_string(str)
    if not defined? Encoding and @charset.unsafe
      raise ClientError, 'Mysql#escape_string is called for unsafe multibyte charset'
    end
    self.class.escape_string str
  end
  alias quote escape_string

  # === Return
  # [String] client version
  def client_info
    self.class.client_info
  end
  alias get_client_info client_info

  # === Return
  # [Integer] client version
  def client_version
    self.class.client_version
  end
  alias get_client_version client_version

  # Set charset of MySQL connection.
  # === Argument
  # cs :: [String / Mysql::Charset]
  # === Return
  # cs
  def charset=(cs)
    charset = cs.is_a?(Charset) ? cs : Charset.by_name(cs)
    if @protocol
      @protocol.charset = charset
      query "SET NAMES #{charset.name}"
    end
    @charset = charset
    cs
  end

  # === Return
  # [String] charset name
  def character_set_name
    @charset.name
  end

  # === Return
  # [Integer] last error number
  def errno
    @last_error ? @last_error.errno : 0
  end

  # === Return
  # [String] last error message
  def error
    @last_error && @last_error.error
  end

  # === Return
  # [String] sqlstate for last error
  def sqlstate
    @last_error ? @last_error.sqlstate : "00000"
  end

  # === Return
  # [Integer] number of columns for last query
  def field_count
    @fields.size
  end

  # === Return
  # [String] connection type
  def host_info
    @host_info
  end
  alias get_host_info host_info

  # === Return
  # [Integer] protocol version
  def proto_info
    Mysql::Protocol::VERSION
  end
  alias get_proto_info proto_info

  # === Return
  # [String] server version
  def server_info
    @protocol.server_info
  end
  alias get_server_info server_info

  # === Return
  # [Integer] server version
  def server_version
    @protocol.server_version
  end
  alias get_server_version server_version

  # === Return
  # [String] information for last query
  def info
    @info
  end

  # === Return
  # [Integer] latest auto_increment value
  def insert_id
    @insert_id
  end

  # Kill query.
  # === Argument
  # pid :: [Integer] thread id
  # === Return
  # self
  def kill(pid)
    @protocol.kill_command pid
    self
  end

  # Return database list.
  # === Argument
  # db :: [String] database name that may contain wild card.
  # === Return
  # [Array of String] database list
  def list_dbs(db=nil)
    db &&= db.gsub(/[\\\']/){"\\#{$&}"}
    query(db ? "show databases like '#{db}'" : "show databases").map(&:first)
  end

  # Execute query string.
  # === Argument
  # str :: [String] Query.
  # block :: If it is given then it is evaluated with Result object as argument.
  # === Return
  # Mysql::Result :: If result set exist.
  # nil :: If the query does not return result set.
  # self :: If block is specified.
  # === Block parameter
  # [Mysql::Result]
  # === Example
  #  my.query("select 1,NULL,'abc'").fetch  # => [1, nil, "abc"]
  def query(str, &block)
    @fields = nil
    begin
      nfields = @protocol.query_command str
      if nfields
        @fields = @protocol.retr_fields nfields
        @result_exist = true
      else
        @affected_rows, @insert_id, @server_status, @warning_count, @info =
          @protocol.affected_rows, @protocol.insert_id, @protocol.server_status, @protocol.warning_count, @protocol.message
      end
      if block
        while true
          block.call store_result if @fields
          break unless next_result
        end
        return self
      end
      if @query_with_result
        return @fields ? store_result : nil
      else
        return self
      end
    rescue ServerError => e
      @last_error = e
      @sqlstate = e.sqlstate
      raise
    end
  end
  alias real_query query

  # Get all data for last query if query_with_result is false.
  # === Return
  # [Mysql::Result]
  def store_result
    raise ClientError, 'invalid usage' unless @result_exist
    res = Result.new @fields, @protocol
    @server_status = @protocol.server_status
    @result_exist = false
    res
  end

  # Returns thread ID.
  # === Return
  # [Integer] Thread ID
  def thread_id
    @protocol.thread_id
  end

  # Use result of query. The result data is retrieved when you use Mysql::Result#fetch_row.
  def use_result
    store_result
  end

  # Set server option.
  # === Argument
  # opt :: [Integer] Mysql::OPTION_MULTI_STATEMENTS_ON or Mysql::OPTION_MULTI_STATEMENTS_OFF
  # === Return
  # self
  def set_server_option(opt)
    @protocol.set_option_command opt
    self
  end

  # true if multiple queries are specified and unexecuted queries exists.
  def more_results
    @server_status & SERVER_MORE_RESULTS_EXISTS != 0
  end
  alias more_results? more_results

  # execute next query if multiple queries are specified.
  # === Return
  # true if next query exists.
  def next_result
    return false unless more_results
    @fields = nil
    nfields = @protocol.get_result
    if nfields
      @fields = @protocol.retr_fields nfields
      @result_exist = true
    end
    return true
  end

  # Parse prepared-statement.
  # === Argument
  # str :: [String] query string
  # === Return
  # Mysql::Statement :: Prepared-statement object
  def prepare(str)
    st = Stmt.new @protocol, @charset
    st.prepare str
    st
  end

  # Make empty prepared-statement object.
  # === Return
  # Mysql::Stmt :: If block is not specified.
  def stmt_init
    Stmt.new @protocol, @charset
  end

  # Returns Mysql::Result object that is empty.
  # Use fetch_fields to get list of fields.
  # === Argument
  # table :: [String] table name.
  # field :: [String] field name that may contain wild card.
  # === Return
  # [Mysql::Result]
  def list_fields(table, field=nil)
    begin
      fields = @protocol.field_list_command table, field
      return Result.new fields
    rescue ServerError => e
      @last_error = e
      @sqlstate = e.sqlstate
      raise
    end
  end

  # Returns Mysql::Result object containing process list.
  # === Return
  # [Mysql::Result]
  def list_processes
    @fields = @protocol.process_info_command
    @result_exist = true
    store_result
  end

  # Returns list of table name.
  #
  # NOTE for Ruby 1.8: This is not multi-byte safe. Don't use for
  # multi-byte charset such as cp932.
  # === Argument
  # table :: [String] database name that may contain wild card.
  # === Return
  # [Array of String]
  def list_tables(table=nil)
    q = table ? "show tables like '#{quote table}'" : "show tables"
    query(q).map(&:first)
  end

  # Check whether the  connection is available.
  # === Return
  # self
  def ping
    @protocol.ping_command
    self
  end

  # Flush tables or caches.
  # === Argument
  # op :: [Integer] operation. Use Mysql::REFRESH_* value.
  # === Return
  # self
  def refresh(op)
    @protocol.refresh_command op
    self
  end

  # Reload grant tables.
  # === Return
  # self
  def reload
    refresh Mysql::REFRESH_GRANT
  end

  # Select default database
  # === Return
  # self
  def select_db(db)
    query "use #{db}"
    self
  end

  # shutdown server.
  # === Return
  # self
  def shutdown(level=0)
    @protocol.shutdown_command level
    self
  end

  # === Return
  # [String] statistics message
  def stat
    @protocol.statistics_command
  end

  # Commit transaction
  # === Return
  # self
  def commit
    query 'commit'
    self
  end

  # Rollback transaction
  # === Return
  # self
  def rollback
    query 'rollback'
    self
  end

  # Set autocommit mode
  # === Argument
  # flag :: [true / false]
  # === Return
  # self
  def autocommit(flag)
    query "set autocommit=#{flag ? 1 : 0}"
    self
  end

  # Field class
  class Field
    attr_reader :db             # database name
    attr_reader :table          # table name
    attr_reader :org_table      # original table name
    attr_reader :name           # field name
    attr_reader :org_name       # original field name
    attr_reader :charsetnr      # charset id number
    attr_reader :length         # field length
    attr_reader :type           # field type
    attr_reader :flags          # flag
    attr_reader :decimals       # number of decimals
    attr_reader :default        # defualt value
    alias :def :default
    attr_accessor :max_length   # maximum width of the field for the result set

    # === Argument
    # [Protocol::FieldPacket]
    def initialize(packet)
      @db, @table, @org_table, @name, @org_name, @charsetnr, @length, @type, @flags, @decimals, @default =
        packet.db, packet.table, packet.org_table, packet.name, packet.org_name, packet.charsetnr, packet.length, packet.type, packet.flags, packet.decimals, packet.default
      @flags |= NUM_FLAG if is_num_type?
    end

    def hash
      {
        "name"       => @name,
        "table"      => @table,
        "def"        => @default,
        "type"       => @type,
        "length"     => @length,
        "max_length" => @max_length,
        "flags"      => @flags,
        "decimals"   => @decimals
      }
    end

    def inspect
      "#<Mysql::Field:#{@name}>"
    end

    # Return true if numeric field.
    def is_num?
      @flags & NUM_FLAG != 0
    end

    # Return true if not null field.
    def is_not_null?
      @flags & NOT_NULL_FLAG != 0
    end

    # Return true if primary key field.
    def is_pri_key?
      @flags & PRI_KEY_FLAG != 0
    end

    private

    def is_num_type?
      [TYPE_DECIMAL, TYPE_TINY, TYPE_SHORT, TYPE_LONG, TYPE_FLOAT, TYPE_DOUBLE, TYPE_LONGLONG, TYPE_INT24].include?(@type) || (@type == TYPE_TIMESTAMP && (@length == 14 || @length == 8))
    end

  end

  # Result set
  class ResultBase
    include Enumerable

    attr_reader :fields

    # === Argument
    # fields :: [Array of Mysql::Field]
    def initialize(fields)
      @fields = fields
      @field_index = 0             # index of field
      @records = []                # all records
      @index = 0                   # index of record
      @fieldname_with_table = nil
    end

    # ignore
    def free
    end

    # === Return
    # [Integer] number of record
    def size
      @records.size
    end
    alias num_rows size

    # Return current record.
    # === Return
    # [Array] record data
    def fetch
      @fetched_record = nil
      return nil if @index >= @records.size
      rec = @records[@index]
      @index += 1
      @fetched_record = rec
      return rec
    end
    alias fetch_row fetch

    # Return data of current record as Hash.
    # The hash key is field name.
    # === Argument
    # with_table :: if true, hash key is "table_name.field_name".
    # === Return
    # [Array of Hash] record data
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
    # === Block parameter
    # [Array] record data
    # === Return
    # self. If block is not specified, this returns Enumerator.
    def each(&block)
      return enum_for(:each) unless block
      while rec = fetch
        block.call rec
      end
      self
    end

    # Iterate block with record as Hash.
    # === Argument
    # with_table :: if true, hash key is "table_name.field_name".
    # === Block parameter
    # [Array of Hash] record data
    # === Return
    # self. If block is not specified, this returns Enumerator.
    def each_hash(with_table=nil, &block)
      return enum_for(:each_hash, with_table) unless block
      while rec = fetch_hash(with_table)
        block.call rec
      end
      self
    end

    # Set record position
    # === Argument
    # n :: [Integer] record index
    # === Return
    # self
    def data_seek(n)
      @index = n
      self
    end

    # Return current record position
    # === Return
    # [Integer] record position
    def row_tell
      @index
    end

    # Set current position of record
    # === Argument
    # n :: [Integer] record index
    # === Return
    # [Integer] previous position
    def row_seek(n)
      ret = @index
      @index = n
      ret
    end
  end

  # Result set for simple query
  class Result < ResultBase
    def initialize(fields, protocol=nil)
      super fields
      return unless protocol
      @records = protocol.retr_all_records @fields
      # for Field#max_length
      @records.each do |rec|
        rec.zip(fields) do |v, f|
          f.max_length = [v ? v.length : 0, f.max_length || 0].max
        end
      end
    end

    # Return current field
    # === Return
    # [Mysql::Field] field object
    def fetch_field
      return nil if @field_index >= @fields.length
      ret = @fields[@field_index]
      @field_index += 1
      ret
    end

    # Return current position of field
    # === Return
    # [Integer] field position
    def field_tell
      @field_index
    end

    # Set field position
    # === Argument
    # n :: [Integer] field index
    # === Return
    # [Integer] previous position
    def field_seek(n)
      ret = @field_index
      @field_index = n
      ret
    end

    # Return field
    # === Argument
    # n :: [Integer] field index
    # === Return
    # [Mysql::Field] field
    def fetch_field_direct(n)
      raise ClientError, "invalid argument: #{n}" if n < 0 or n >= @fields.length
      @fields[n]
    end

    # Return all fields
    # === Return
    # [Array of Mysql::Field] all fields
    def fetch_fields
      @fields
    end

    # Return length of each fields
    # === Return
    # [Array of Integer] length of each fields
    def fetch_lengths
      return nil unless @fetched_record
      @fetched_record.map{|c|c.nil? ? 0 : c.length}
    end

    # === Return
    # [Integer] number of fields
    def num_fields
      @fields.size
    end
  end

  # Result set for prepared statement
  class StatementResult < ResultBase
    def initialize(fields, protocol, charset)
      super fields
      @records = protocol.stmt_retr_all_records @fields, charset
    end
  end

  # Prepared statement
  class Stmt
    include Enumerable

    attr_reader :affected_rows, :insert_id, :server_status, :warning_count
    attr_reader :param_count, :fields, :sqlstate

    def self.finalizer(protocol, statement_id)
      proc do
        protocol.gc_stmt statement_id
      end
    end

    def initialize(protocol, charset)
      @protocol = protocol
      @charset = charset
      @statement_id = nil
      @affected_rows = @insert_id = @server_status = @warning_count = 0
      @sqlstate = "00000"
      @param_count = nil
    end

    # parse prepared-statement and return Mysql::Statement object
    # === Argument
    # str :: [String] query string
    # === Return
    # self
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
    # === Argument
    # values passed to query
    # === Return
    # self
    def execute(*values)
      raise ClientError, "not prepared" unless @param_count
      raise ClientError, "parameter count mismatch" if values.length != @param_count
      values = values.map{|v| @charset.convert v}
      begin
        @sqlstate = "00000"
        nfields = @protocol.stmt_execute_command @statement_id, values
        if nfields
          @fields = @protocol.retr_fields nfields
          @result = StatementResult.new @fields, @protocol, @charset
        else
          @affected_rows, @insert_id, @server_status, @warning_count, @info =
            @protocol.affected_rows, @protocol.insert_id, @protocol.server_status, @protocol.warning_count, @protocol.message
        end
        return self
      rescue ServerError => e
        @last_error = e
        @sqlstate = e.sqlstate
        raise
      end
    end

    # Close prepared statement
    def close
      ObjectSpace.undefine_finalizer(self)
      @protocol.stmt_close_command @statement_id if @statement_id
      @statement_id = nil
    end

    # Return current record
    # === Return
    # [Array] record data
    def fetch
      row = @result.fetch
      return row unless @bind_result
      row.zip(@bind_result).map do |col, type|
        if col.nil?
          nil
        elsif [Numeric, Integer, Fixnum].include? type
          col.to_i
        elsif type == String
          col.to_s
        elsif type == Float && !col.is_a?(Float)
          col.to_i.to_f
        elsif type == Mysql::Time && !col.is_a?(Mysql::Time)
          if col.to_s =~ /\A\d+\z/
            i = col.to_s.to_i
            if i < 100000000
              y = i/10000
              m = i/100%100
              d = i%100
              h, mm, s = 0
            else
              y = i/10000000000
              m = i/100000000%100
              d = i/1000000%100
              h = i/10000%100
              mm= i/100%100
              s = i%100
            end
            if y < 70
              y += 2000
            elsif y < 100
              y += 1900
            end
            Mysql::Time.new(y, m, d, h, mm, s)
          else
            Mysql::Time.new
          end
        else
          col
        end
      end
    end

    # Return data of current record as Hash.
    # The hash key is field name.
    # === Argument
    # with_table :: if true, hash key is "table_name.field_name".
    # === Return
    # [Array of Hash] record data
    def fetch_hash(with_table=nil)
      @result.fetch_hash with_table
    end

    # Set retrieve type of value
    # === Argument
    # [Numeric / Fixnum / Integer / Float / String / Mysql::Time / nil] value type
    # === Return
    # self
    def bind_result(*args)
      if @fields.length != args.length
        raise ClientError, "bind_result: result value count(#{@fields.length}) != number of argument(#{args.length})"
      end
      args.each do |a|
        raise TypeError unless [Numeric, Fixnum, Integer, Float, String, Mysql::Time, nil].include? a
      end
      @bind_result = args
      self
    end

    # Iterate block with record.
    # === Block parameter
    # [Array] record data
    # === Return
    # self. If block is not specified, this returns Enumerator.
    def each(&block)
      return enum_for(:each) unless block
      while rec = fetch
        block.call rec
      end
      self
    end

    # Iterate block with record as Hash.
    # === Argument
    # with_table :: if true, hash key is "table_name.field_name".
    # === Block parameter
    # [Array of Hash] record data
    # === Return
    # self. If block is not specified, this returns Enumerator.
    def each_hash(with_table=nil, &block)
      return enum_for(:each_hash, with_table) unless block
      while rec = fetch_hash(with_table)
        block.call rec
      end
      self
    end

    # === Return
    # [Integer] number of record
    def size
      @result.size
    end
    alias num_rows size

    # Set record position
    # === Argument
    # n :: [Integer] record index
    # === Return
    # self
    def data_seek(n)
      @result.data_seek(n)
    end

    # Return current record position
    # === Return
    # [Integer] record position
    def row_tell
      @result.row_tell
    end

    # Set current position of record
    # === Argument
    # n :: [Integer] record index
    # === Return
    # [Integer] previous position
    def row_seek(n)
      @result.row_seek(n)
    end

    # === Return
    # [Integer] number of columns for last query
    def field_count
      @fields.length
    end

    # ignore
    def free_result
    end

    # Returns Mysql::Result object that is empty.
    # Use fetch_fields to get list of fields.
    # === Return
    # [Mysql::Result]
    def result_metadata
      return nil if @fields.empty?
      Result.new @fields
    end
  end

  class Time
    # === Argument
    # year        :: [Integer] year
    # month       :: [Integer] month
    # day         :: [Integer] day
    # hour        :: [Integer] hour
    # minute      :: [Integer] minute
    # second      :: [Integer] second
    # neg         :: [true / false] negative flag
    def initialize(year=0, month=0, day=0, hour=0, minute=0, second=0, neg=false, second_part=0)
      @year, @month, @day, @hour, @minute, @second, @neg, @second_part =
        year.to_i, month.to_i, day.to_i, hour.to_i, minute.to_i, second.to_i, neg, second_part.to_i
    end
    attr_accessor :year, :month, :day, :hour, :minute, :second, :neg, :second_part
    alias mon month
    alias min minute
    alias sec second

    def ==(other) # :nodoc:
      other.is_a?(Mysql::Time) &&
        @year == other.year && @month == other.month && @day == other.day &&
        @hour == other.hour && @minute == other.minute && @second == other.second &&
        @neg == neg && @second_part == other.second_part
    end

    def eql?(other) # :nodoc:
      self == other
    end

    # === Return
    # [String] "yyyy-mm-dd HH:MM:SS"
    def to_s
      if year == 0 and mon == 0 and day == 0
        h = neg ? hour * -1 : hour
        sprintf "%02d:%02d:%02d", h, min, sec
      else
        sprintf "%04d-%02d-%02d %02d:%02d:%02d", year, mon, day, hour, min, sec
      end
    end

    # === Return
    # [Integer] yyyymmddHHMMSS
    def to_i
      sprintf("%04d%02d%02d%02d%02d%02d", year, mon, day, hour, min, sec).to_i
    end

    def inspect # :nodoc:
      sprintf "#<#{self.class.name}:%04d-%02d-%02d %02d:%02d:%02d>", year, mon, day, hour, min, sec
    end

  end

end
