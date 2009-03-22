# Copyright (C) 2008 TOMITA Masahiro
# mailto:tommy@tmtm.org

$LOAD_PATH.unshift File.dirname(__FILE__)
require "mysql/constants"
require "mysql/error"
require "mysql/charset"
require "mysql/protocol"
require "mysql/cache"

class Mysql

  VERSION            = 30000               # Version number of this library
  MYSQL_UNIX_PORT    = "/tmp/mysql.sock"   # UNIX domain socket filename
  MYSQL_TCP_PORT     = 3306                # TCP socket port number

  OPTIONS = {
    :connect_timeout         => Integer,
#    :compress                => x,
#    :named_pipe              => x,
    :init_command            => String,
#    :read_default_file       => x,
#    :read_default_group      => x,
    :charset                 => Object,
#    :local_infile            => x,
#    :shared_memory_base_name => x,
    :read_timeout            => Integer,
    :write_timeout           => Integer,
#    :use_result              => x,
#    :use_remote_connection   => x,
#    :use_embedded_connection => x,
#    :guess_connection        => x,
#    :client_ip               => x,
#    :secure_auth             => x,
#    :report_data_truncation  => x,
#    :reconnect               => x,
#    :ssl_verify_server_cert  => x,
    :prepared_statement_cache_size => Integer,
  }  # :nodoc:

  OPT2FLAG = {
#    :compress                => CLIENT_COMPRESS,
    :found_rows              => CLIENT_FOUND_ROWS,
    :ignore_sigpipe          => CLIENT_IGNORE_SIGPIPE,
    :ignore_space            => CLIENT_IGNORE_SPACE,
    :interactive             => CLIENT_INTERACTIVE,
    :local_files             => CLIENT_LOCAL_FILES,
#    :multi_results           => CLIENT_MULTI_RESULTS,
#    :multi_statements        => CLIENT_MULTI_STATEMENTS,
    :no_schema               => CLIENT_NO_SCHEMA,
#    :ssl                     => CLIENT_SSL,
  }  # :nodoc:

  attr_reader :charset               # character set of MySQL connection
  attr_reader :affected_rows         # number of affected records by insert/update/delete.
  attr_reader :insert_id             # latest auto_increment value.
  attr_reader :server_status         # :nodoc:
  attr_reader :warning_count         #
  attr_reader :server_version        #
  attr_reader :protocol              #

  def self.new(*args, &block)  # :nodoc:
    my = self.allocate
    my.instance_eval{initialize(*args)}
    return my unless block
    begin
      return block.call my
    ensure
      my.close
    end
  end

  # === Return
  # The value that block returns if block is specified.
  # Otherwise this returns Mysql object.
  def self.connect(*args, &block)
    my = self.new *args
    my.connect
    return my unless block
    begin
      return block.call my
    ensure
      my.close
    end
  end

  # :call-seq:
  # new(conninfo, opt={})
  # new(conninfo, opt={}) {|my| ...}
  #
  # Connect to mysqld.
  # If block is specified then the connection is closed when exiting the block.
  # === Argument
  # conninfo ::
  #   [String / URI / Hash] Connection information.
  #   If conninfo is String then it's format must be "mysql://user:password@hostname:port/dbname".
  #   If conninfo is URI then it's scheme must be "mysql".
  #   If conninfo is Hash then valid keys are :host, :user, :password, :db, :port, :socket and :flag.
  # opt :: [Hash] options.
  # === Options
  # :connect_timeout :: [Numeric] The number of seconds before connection timeout.
  # :init_command    :: [String] Statement to execute when connecting to the MySQL server.
  # :charset         :: [String / Mysql::Charset] The character set to use as the default character set.
  # :read_timeout    :: [The timeout in seconds for attempts to read from the server.
  # :write_timeout   :: [Numeric] The timeout in seconds for attempts to write to the server.
  # :found_rows      :: [Boolean] Return the number of found (matched) rows, not the number of changed rows.
  # :ignore_space    :: [Boolean] Allow spaces after function names.
  # :interactive     :: [Boolean] Allow `interactive_timeout' seconds (instead of `wait_timeout' seconds) of inactivity before closing the connection.
  # :local_files     :: [Boolean] Enable `LOAD DATA LOCAL' handling.
  # :no_schema       :: [Boolean] Don't allow the DB_NAME.TBL_NAME.COL_NAME syntax.
  # === Block parameter
  # my :: [ Mysql ]
  def initialize(*args)
    @fields = nil
    @protocol = nil
    @charset = nil
    @connect_timeout = nil
    @read_timeout = nil
    @write_timeout = nil
    @init_command = nil
    @affected_rows = nil
    @server_version = nil
    @param, opt = conninfo *args
    @connected = false
    set_option opt
  end

  def connect(*args)
    param, opt = conninfo *args
    set_option opt
    param = @param.merge param
    @protocol = Protocol.new param[:host], param[:port], param[:socket], @connect_timeout, @read_timeout, @write_timeout
    @protocol.synchronize do
      init_packet = @protocol.read_initial_packet
      @server_version = init_packet.server_version.split(/\D/)[0,3].inject{|a,b|a.to_i*100+b.to_i}
      client_flags = CLIENT_LONG_PASSWORD | CLIENT_LONG_FLAG | CLIENT_TRANSACTIONS | CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION
      client_flags |= CLIENT_CONNECT_WITH_DB if param[:db]
      client_flags |= param[:flag] if param[:flag]
      unless @charset
        @charset = Charset.by_number(init_packet.server_charset)
        @charset.encoding       # raise error if unsupported charset
      end
      netpw = init_packet.crypt_password param[:password]
      auth_packet = Protocol::AuthenticationPacket.new client_flags, 1024**3, @charset.number, param[:user], netpw, param[:db]
      @protocol.send_packet auth_packet
      @protocol.read            # skip OK packet
    end
    @stmt_cache = Cache.new(@prepared_statement_cache_size)
    simple_query @init_command if @init_command
    return self
  end

  def close
    if @protocol
      @protocol.synchronize do
        @protocol.send_packet Protocol::QuitPacket.new
        @protocol.close
        @protocol = nil
      end
    end
    return self
  end

  # set characterset of MySQL connection
  # === Argument
  # cs :: [String / Mysql::Charset]
  # === Return
  # cs
  def charset=(cs)
    charset = cs.is_a?(Charset) ? cs : Charset.by_name(cs)
    query "SET NAMES #{charset.name}" if @protocol
    @charset = charset
    cs
  end

  # Execute query string.
  # If str begin with "sel" or params is specified, then the query is executed as prepared-statement automatically.
  # So the values in result set are not only String.
  # === Argument
  # str :: [String] Query.
  # params :: Parameters corresponding to place holder (`?') in str.
  # === Return
  # Mysql::Statement :: If result set exist when str begin with "sel".
  # Mysql::Result :: If result set exist when str does not begin with "sel".
  # nil :: If result set does not exist.
  # === Example
  #  my.query("select 1,NULL,'abc'").fetch  # => [1, nil, "abc"]
  def query(str, *params)
    if not params.empty? or str =~ /\A\s*sel/i
      st = @stmt_cache.get str do |s|
        prepare s
      end
      st.execute(*params)
      if st.fields.empty?
        @affected_rows = st.affected_rows
        @insert_id = st.insert_id
        @server_status = st.server_status
        @warning_count = st.warning_count
        return nil
      end
      return st
    else
      return simple_query(str)
    end
  end

  # Execute query string.
  # The values in result set are String even if it is numeric.
  # === Argument
  # str :: [String] query string
  # === Return
  # Mysql::Result :: If result set is exist.
  # nil :: If result set is not eixst.
  # === Example
  #  my.simple_query("select 1,NULL,'abc'").fetch  # => ["1", nil, "abc"]
  def simple_query(str, &block)
    @affected_rows = @insert_id = @server_status = @warning_count = 0
    @fields = nil
    @protocol.synchronize do
      @protocol.reset
      @protocol.send_packet Protocol::QueryPacket.new @charset.convert(str)
      res_packet = @protocol.read_result_packet
      if res_packet.field_count == 0
        @affected_rows, @insert_id, @server_status, @warning_conut =
          res_packet.affected_rows, res_packet.insert_id, res_packet.server_status, res_packet.warning_count
      else
        @fields = (1..res_packet.field_count).map{Field.new @protocol.read_field_packet}
        @protocol.read_eof_packet
      end
      if block
        yield Result.new(self, @fields)
        return self
      end
      return @fields && Result.new(self, @fields)
    end
  end

  # Parse prepared-statement.
  # === Argument
  # str :: [String] query string
  # === Return
  # Mysql::Statement :: Prepared-statement object
  def prepare(str, &block)
    st = Statement.new self
    st.prepare str
    if block
      begin
        return block.call st
      ensure
        st.close
      end
    end
    return st
  end

  # Escape special character in MySQL.
  # === Note
  # In Ruby 1.8, this is not safe for multibyte charset such as 'SJIS'.
  # You should use place-holder in prepared-statement.
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

  # :call-seq:
  # statement()
  # statement() {|st| ... }
  #
  # Make empty prepared-statement object.
  # If block is specified then prepared-statement is closed when exiting the block.
  # === Block parameter
  # st :: [ Mysql::Stmt ] Prepared-statement object.
  # === Return
  # Mysql::Statement :: If block is not specified.
  # The value returned by block :: If block is specified.
  def statement(&block)
    st = Statement.new self
    if block
      begin
        return block.call st
      ensure
        st.close
      end
    end
    return st
  end

  private

  # analyze argument and returns connection-parameter and option.
  #
  # connection-parameter's key :: :host, :user, :password, :db, :port, :socket, :flag
  # === Return
  # Hash :: connection parameters
  # Hash :: option {:optname => value, ...}
  def conninfo(*args)
    paramkeys = [:host, :user, :password, :db, :port, :socket, :flag]
    opt = {}
    if args.empty?
      param = {}
    elsif args.size == 1 and args.first.is_a? Hash
      arg = args.first.dup
      param = {}
      [:host, :user, :password, :db, :port, :socket, :flag].each do |k|
        param[k] = arg.delete k if arg.key? k
      end
      opt = arg
    else
      if args.last.is_a? Hash
        args = args.dup
        opt = args.pop
      end
      if args.size > 1 || args.first.nil? || args.first.is_a?(String) && args.first !~ /\Amysql:/
        host, user, password, db, port, socket, flag = args
        param = {:host=>host, :user=>user, :password=>password, :db=>db, :port=>port, :socket=>socket, :flag=>flag}
      elsif args.first.is_a? Hash
        param = args.first.dup
        param.keys.each do |k|
          unless paramkeys.include? k
            raise ArgumentError, "Unknown parameter: #{k.inspect}"
          end
        end
      else
        if args.first =~ /\Amysql:/
          require "uri" unless defined? URI
          uri = URI.parse args.first
        elsif defined? URI and args.first.is_a? URI
          uri = args.first
        else
          raise ArgumentError, "Invalid argument: #{args.first.inspect}"
        end
        unless uri.scheme == "mysql"
          raise ArgumentError, "Invalid scheme: #{uri.scheme}"
        end
        param = {:host=>uri.host, :user=>uri.user, :password=>uri.password, :port=>uri.port||MYSQL_TCP_PORT}
        param[:db] = uri.path.split(/\/+/).reject{|a|a.empty?}.first
        if uri.query
          uri.query.split(/\&/).each do |a|
            k, v = a.split(/\=/, 2)
            if k == "socket"
              param[:socket] = v
            elsif k == "flag"
              param[:flag] = v.to_i
            else
              opt[k.intern] = v
            end
          end
        end
      end
    end
    param[:flag] = 0 unless param.key? :flag
    opt.keys.each do |k|
      if OPT2FLAG.key? k and opt[k]
        param[:flag] |= OPT2FLAG[k]
        next
      end
      unless OPTIONS.key? k
        raise ArgumentError, "Unknown option: #{k.inspect}"
      end
      opt[k] = opt[k].to_i if OPTIONS[k] == Integer
    end
    return param, opt
  end

  private

  def set_option(opt)
    opt.each do |k,v|
      raise ClientError, "unknown option: #{k.inspect}" unless OPTIONS.key? k
      type = OPTIONS[k]
      if type.is_a? Class
        raise ClientError, "invalid value for #{k.inspect}: #{v.inspect}" unless v.is_a? type
      end
    end

    charset = opt[:charset] if opt.key? :charset
    @connect_timeout = opt[:connect_timeout] || @connect_timeout
    @init_command = opt[:init_command] || @init_command
    @read_timeout = opt[:read_timeout] || @read_timeout
    @write_timeout = opt[:write_timeout] || @write_timeout
    @prepared_statement_cache_size = opt[:prepared_statement_cache_size] || @prepared_statement_cache_size || 10
  end

  class Field
    attr_reader :db, :table, :org_table, :name, :org_name, :charsetnr, :length, :type, :flags, :decimals, :default
    alias :def :default

    # === Argument
    # packet :: [Protocol::FieldPacket]
    def initialize(packet)
      @db, @table, @org_table, @name, @org_name, @charsetnr, @length, @type, @flags, @decimals, @default =
        packet.db, packet.table, packet.org_table, packet.name, packet.org_name, packet.charsetnr, packet.length, packet.type, packet.flags, packet.decimals, packet.default
      @flags |= NUM_FLAG if is_num_type?
    end

    def is_num?
      @flags & NUM_FLAG != 0
    end

    def is_not_null?
      @flags & NOT_NULL_FLAG != 0
    end

    def is_pri_key?
      @flags & PRI_KEY_FLAG != 0
    end

    private

    def is_num_type?
      [TYPE_DECIMAL, TYPE_TINY, TYPE_SHORT, TYPE_LONG, TYPE_FLOAT, TYPE_DOUBLE, TYPE_LONGLONG, TYPE_INT24].include?(@type) || (@type == TYPE_TIMESTAMP && (@length == 14 || @length == 8))
    end

  end

  class Result

    include Enumerable

    attr_reader :fields

    def initialize(mysql, fields)
      @mysql = mysql
      @fields = fields
      @fieldname_with_table = nil
      @field_index = 0
      @records = recv_all_records mysql.protocol, @fields, mysql.charset
      @index = 0
    end

    def fetch_row
      rec = @records[@index]
      @index += 1 if @index < @records.length
      return rec
    end
    alias fetch fetch_row

    def fetch_hash(with_table=nil)
      row = fetch_row
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

    def each(&block)
      return enum_for(:each) unless block
      while rec = fetch_row
        block.call rec
      end
      self
    end

    def each_hash(with_table=nil, &block)
      return enum_for(:each_hash, with_table) unless block
      while rec = fetch_hash(with_table)
        block.call rec
      end
      self
    end

    private

    def recv_all_records(protocol, fields, charset)
      ret = []
      while true
        data = protocol.read
        break if Protocol.eof_packet? data
        rec = fields.map do |f|
          v = Protocol.lcs2str! data
          v.nil? ? nil : f.flags & Field::BINARY_FLAG == 0 ? charset.force_encoding(v) : Charset.to_binary(v)
        end
        ret.push rec
      end
      ret
    end
  end

  class Time
    def initialize(year=0, month=0, day=0, hour=0, minute=0, second=0, neg=false, second_part=0)
      @year, @month, @day, @hour, @minute, @second, @neg, @second_part =
        year.to_i, month.to_i, day.to_i, hour.to_i, minute.to_i, second.to_i, neg, second_part.to_i
    end
    attr_accessor :year, :month, :day, :hour, :minute, :second, :neg, :second_part
    alias mon month
    alias min minute
    alias sec second

    def ==(other)
      other.is_a?(Mysql::Time) &&
        @year == other.year && @month == other.month && @day == other.day &&
        @hour == other.hour && @minute == other.minute && @second == other.second &&
        @neg == neg && @second_part == other.second_part
    end

    def eql?(other)
      self == other
    end

    def to_s
      if year == 0 and mon == 0 and day == 0
        sprintf "%02d:%02d:%02d", hour, min, sec
      else
        sprintf "%04d-%02d-%02d %02d:%02d:%02d", year, mon, day, hour, min, sec
      end
    end

  end

  class Statement

    include Enumerable

    attr_reader :affected_rows, :insert_id, :server_status, :warning_count
    attr_reader :param_count, :fields, :sqlstate
    attr_accessor :cursor_type

    def self.finalizer(protocol, statement_id)
      proc do
        Thread.new do
          protocol.synchronize do
            protocol.reset
            protocol.send_packet Protocol::StmtClosePacket.new statement_id
          end
        end
      end
    end

    def initialize(mysql)
      @mysql = mysql
      @protocol = mysql.protocol
      @statement_id = nil
      @affected_rows = @insert_id = @server_status = @warning_count = 0
      @eof = false
      @sqlstate = "00000"
      @cursor_type = CURSOR_TYPE_NO_CURSOR
      @param_count = nil
    end

    # parse prepared-statement and return Mysql::Statement object
    # === Argument
    # str :: [String] query string
    # === Return
    # self
    def prepare(str)
      close
      @protocol.synchronize do
        begin
          @sqlstate = "00000"
          @protocol.reset
          @protocol.send_packet Protocol::PreparePacket.new @mysql.charset.convert(str)
          res_packet = @protocol.read_prepare_result_packet
          if res_packet.param_count > 0
            res_packet.param_count.times{@protocol.read}   # skip parameter packet
            @protocol.read_eof_packet
          end
          if res_packet.field_count > 0
            fields = (1..res_packet.field_count).map{Field.new @protocol.read_field_packet}
            @protocol.read_eof_packet
          else
            fields = []
          end
          @statement_id = res_packet.statement_id
          @param_count = res_packet.param_count
          @fields = fields
        rescue ServerError => e
          @sqlstate = e.sqlstate
          raise
        end
      end
      ObjectSpace.define_finalizer(self, self.class.finalizer(@protocol, @statement_id))
      self
    end

    def execute(*values)
      raise ClientError, "not prepared" unless @param_count
      raise ClientError, "parameter count mismatch" if values.length != @param_count
      values = values.map{|v| @mysql.charset.convert v}
      @protocol.synchronize do
        begin
          @sqlstate = "00000"
          @protocol.reset
          cursor_type = @fields.empty? ? CURSOR_TYPE_NO_CURSOR : @cursor_type
          @protocol.send_packet Protocol::ExecutePacket.new @statement_id, cursor_type, values
          res_packet = @protocol.read_result_packet
          raise ProtocolError, "invalid field_count" unless res_packet.field_count == @fields.length
          @fieldname_with_table = nil
          if res_packet.field_count == 0
            @affected_rows, @insert_id, @server_status, @warning_conut =
              res_packet.affected_rows, res_packet.insert_id, res_packet.server_status, res_packet.warning_count
            @records = nil
          else
            @fields = (1..res_packet.field_count).map{Field.new @protocol.read_field_packet}
            @protocol.read_eof_packet
            @eof = false
            @index = 0
            if @cursor_type == CURSOR_TYPE_NO_CURSOR
              @records = []
              while rec = parse_data(@protocol.read)
                @records.push rec
              end
            end
          end
          return self
        rescue ServerError => e
          @sqlstate = e.sqlstate
          raise
        end
      end
    end

    def fetch_row
      return nil if @fields.empty?
      if @records
        rec = @records[@index]
        @index += 1 if @index < @records.length
        return rec
      end
      return nil if @eof
      @protocol.synchronize do
        @protocol.reset
        @protocol.send_packet Protocol::FetchPacket.new @statement_id, 1
        data = @protocol.read
        if Protocol.eof_packet? data
          @eof = true
          return nil
        end
        @protocol.read_eof_packet
        return parse_data data
      end
    end
    alias fetch fetch_row

    def fetch_hash(with_table=nil)
      row = fetch_row
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

    def each(&block)
      return enum_for(:each) unless block
      while rec = fetch_row
        block.call rec
      end
      self
    end

    def each_hash(with_table=nil, &block)
      return enum_for(:each_hash, with_table) unless block
      while rec = fetch_hash(with_table)
        block.call rec
      end
      self
    end

    def close
      ObjectSpace.undefine_finalizer(self)
      @protocol.synchronize do
        @protocol.reset
        if @statement_id
          @protocol.send_packet Protocol::StmtClosePacket.new @statement_id
          @statement_id = nil
        end
      end
    end

    private

    def parse_data(data)
      return nil if Protocol.eof_packet? data
      data.slice!(0)  # skip first byte
      null_bit_map = data.slice!(0, (@fields.length+7+2)/8).unpack("C*")
      ret = (0...@fields.length).map do |i|
        if null_bit_map[(i+2)/8][(i+2)%8] == 1
          nil
        else
          unsigned = @fields[i].flags & Field::UNSIGNED_FLAG != 0
          v = Protocol.net2value(data, @fields[i].type, unsigned)
          @fields[i].flags & Field::BINARY_FLAG == 0 ? @mysql.charset.force_encoding(v) : Charset.to_binary(v)
        end
      end
      ret
    end

  end
end
