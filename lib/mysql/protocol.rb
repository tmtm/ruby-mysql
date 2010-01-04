# Copyright (C) 2008-2009 TOMITA Masahiro
# mailto:tommy@tmtm.org

require "socket"
require "timeout"
require "digest/sha1"
require "thread"
require "stringio"

class Mysql
  # MySQL network protocol
  class Protocol

    VERSION = 10
    MAX_PACKET_LENGTH = 2**24-1

    # convert Numeric to LengthCodedBinary
    def self.lcb(num)
      return "\xfb" if num.nil?
      return [num].pack("C") if num < 251
      return [252, num].pack("Cv") if num < 65536
      return [253, num&0xffff, num>>16].pack("CvC") if num < 16777216
      return [254, num&0xffffffff, num>>32].pack("CVV")
    end

    # convert String to LengthCodedString
    def self.lcs(str)
      str = Charset.to_binary str
      lcb(str.length)+str
    end

    # convert LengthCodedBinary to Integer
    # === Argument
    # lcb :: [String] LengthCodedBinary. This value will be broken.
    # === Return
    # Integer or nil
    def self.lcb2int!(lcb)
      return nil if lcb.empty?
      case v = lcb.slice!(0)
      when ?\xfb
        return nil
      when ?\xfc
        return lcb.slice!(0,2).unpack("v").first
      when ?\xfd
        c, v = lcb.slice!(0,3).unpack("Cv")
        return (v << 8)+c
      when ?\xfe
        v1, v2 = lcb.slice!(0,8).unpack("VV")
        return (v2 << 32)+v1
      else
        return v.ord
      end
    end

    # convert LengthCodedString to String
    # === Argument
    # lcs :: [String] LengthCodedString. This value will be broken.
    # === Return
    # String or nil
    def self.lcs2str!(lcs)
      len = lcb2int! lcs
      return len && lcs.slice!(0, len)
    end

    def self.eof_packet?(data)
      data[0] == ?\xfe && data.length == 5
    end

    # Convert netdata to Ruby value
    # === Argument
    # data :: [String] packet data. This will be broken.
    # type :: [Integer] field type
    # unsigned :: [true or false] true if value is unsigned
    # === Return
    # Object :: converted value.
    def self.net2value(data, type, unsigned)
      case type
      when Field::TYPE_STRING, Field::TYPE_VAR_STRING, Field::TYPE_NEWDECIMAL, Field::TYPE_BLOB
        return lcs2str!(data)
      when Field::TYPE_TINY
        v = data.slice!(0).ord
        return unsigned ? v : v < 128 ? v : v-256
      when Field::TYPE_SHORT
        v = data.slice!(0,2).unpack("v").first
        return unsigned ? v : v < 32768 ? v : v-65536
      when Field::TYPE_INT24, Field::TYPE_LONG
        v = data.slice!(0,4).unpack("V").first
        return unsigned ? v : v < 2**32/2 ? v : v-2**32
      when Field::TYPE_LONGLONG
        n1, n2 = data.slice!(0,8).unpack("VV")
        v = (n2 << 32) | n1
        return unsigned ? v : v < 2**64/2 ? v : v-2**64
      when Field::TYPE_FLOAT
        return data.slice!(0,4).unpack("e").first
      when Field::TYPE_DOUBLE
        return data.slice!(0,8).unpack("E").first
      when Field::TYPE_DATE, Field::TYPE_DATETIME, Field::TYPE_TIMESTAMP
        len = data.slice!(0).ord
        y, m, d, h, mi, s, bs = data.slice!(0,len).unpack("vCCCCCV")
        return Mysql::Time.new(y, m, d, h, mi, s, bs)
      when Field::TYPE_TIME
        len = data.slice!(0).ord
        sign, d, h, mi, s, sp = data.slice!(0,len).unpack("CVCCCV")
        h = d.to_i * 24 + h.to_i
        return Mysql::Time.new(0, 0, 0, h, mi, s, sign!=0, sp)
      when Field::TYPE_YEAR
        return data.slice!(0,2).unpack("v").first
      when Field::TYPE_BIT
        return lcs2str!(data)
      else
        raise "not implemented: type=#{type}"
      end
    end

    # convert Ruby value to netdata
    # === Argument
    # v :: [Object] Ruby value.
    # === Return
    # String :: netdata
    # === Exception
    # ProtocolError :: value too large / value is not supported
    def self.value2net(v)
      case v
      when nil
        type = Field::TYPE_NULL
        val = ""
      when Integer
        if v >= 0
          if v < 256
            type = Field::TYPE_TINY | 0x8000
            val = [v].pack("C")
          elsif v < 256**2
            type = Field::TYPE_SHORT | 0x8000
            val = [v].pack("v")
          elsif v < 256**4
            type = Field::TYPE_LONG | 0x8000
            val = [v].pack("V")
          elsif v < 256**8
            type = Field::TYPE_LONGLONG | 0x8000
            val = [v&0xffffffff, v>>32].pack("VV")
          else
            raise ProtocolError, "value too large: #{v}"
          end
        else
          if -v <= 256/2
            type = Field::TYPE_TINY
            val = [v].pack("C")
          elsif -v <= 256**2/2
            type = Field::TYPE_SHORT
            val = [v].pack("v")
          elsif -v <= 256**4/2
            type = Field::TYPE_LONG
            val = [v].pack("V")
          elsif -v <= 256**8/2
            type = Field::TYPE_LONGLONG
            val = [v&0xffffffff, v>>32].pack("VV")
          else
            raise ProtocolError, "value too large: #{v}"
          end
        end
      when Float
        type = Field::TYPE_DOUBLE
        val = [v].pack("E")
      when String
        type = Field::TYPE_STRING
        val = lcs(v)
      when Mysql::Time, ::Time
        type = Field::TYPE_DATETIME
        val = [7, v.year, v.month, v.day, v.hour, v.min, v.sec].pack("CvCCCCC")
      else
        raise ProtocolError, "class #{v.class} is not supported"
      end
      return type, val
    end

    attr_reader :server_info
    attr_reader :server_version
    attr_reader :thread_id
    attr_reader :charset
    attr_reader :sqlstate
    attr_reader :affected_rows
    attr_reader :insert_id
    attr_reader :server_status
    attr_reader :warning_count
    attr_reader :message
    attr_accessor :reconnect

    # make socket connection to server.
    # === Argument
    # host :: [String] if "localhost" or "" nil then use UNIXSocket. Otherwise use TCPSocket
    # port :: [Integer] port number using by TCPSocket
    # socket :: [String] socket file name using by UNIXSocket
    # conn_timeout :: [Integer] connect timeout (sec).
    # read_timeout :: [Integer] read timeout (sec).
    # write_timeout :: [Integer] write timeout (sec).
    def initialize(host, port, socket, conn_timeout, read_timeout, write_timeout)
      begin
        Timeout.timeout conn_timeout do
          if host.nil? or host.empty? or host == "localhost"
            socket ||= ENV["MYSQL_UNIX_PORT"] || MYSQL_UNIX_PORT
            @sock = UNIXSocket.new socket
          else
            port ||= ENV["MYSQL_TCP_PORT"] || (Socket.getservbyname("mysql","tcp") rescue MYSQL_TCP_PORT)
            @sock = TCPSocket.new host, port
          end
        end
      rescue Timeout::Error
        raise ClientError, "connection timeout"
      end
      @read_timeout = read_timeout
      @write_timeout = write_timeout
      @seq = 0                # packet counter. reset by each command
      @mutex = Mutex.new
      @reconnect = false
      @conninfo = [host, port, socket, conn_timeout]  # for auto reconnection
    end

    def close
      @sock.close
    end

    def synchronize
      @mutex.synchronize do
        return yield
      end
    end

    # Reset sequence number
    def reset
      @seq = 0
    end

    # Read one packet data
    # === Return
    # String
    # === Exception
    # ProtocolError :: invalid packet sequence number
    def read
      ret = ""
      len = nil
      begin
        Timeout.timeout @read_timeout do
          header = @sock.read(4)
          len1, len2, seq = header.unpack("CvC")
          len = (len2 << 8) + len1
          raise ProtocolError, "invalid packet: sequence number mismatch(#{seq} != #{@seq}(expected))" if @seq != seq
          @seq = (@seq + 1) % 256
          ret.concat @sock.read(len)
        end
      rescue Timeout::Error
        raise ClientError, "read timeout"
      end while len == MAX_PACKET_LENGTH

      @sqlstate = "00000"

      # Error packet
      if ret[0] == ?\xff
        f, errno, marker, @sqlstate, message = ret.unpack("Cvaa5a*")
        unless marker == "#"
          f, errno, message = ret.unpack("Cva*")    # Version 4.0 Error
          @sqlstate = ""
        end
        if Mysql::ServerError::ERROR_MAP.key? errno
          raise Mysql::ServerError::ERROR_MAP[errno].new(message, @sqlstate)
        end
        raise Mysql::ServerError.new(message, @sqlstate)
      end
      ret
    end

    # Write one packet data
    # === Argument
    # data [String / IO] :: packet data. If data is nil, write empty packet.
    def write(data)
      begin
        @sock.sync = false
        if data.nil?
          Timeout.timeout @write_timeout do
            @sock.write [0, 0, @seq].pack("CvC")
          end
          @seq = (@seq + 1) % 256
        else
          data = StringIO.new data if data.is_a? String
          while d = data.read(MAX_PACKET_LENGTH)
            Timeout.timeout @write_timeout do
              @sock.write [d.length%256, d.length/256, @seq].pack("CvC")
              @sock.write d
            end
            @seq = (@seq + 1) % 256
          end
        end
        @sock.sync = true
        Timeout.timeout @write_timeout do
          @sock.flush
        end
      rescue Errno::EPIPE
        raise unless @reconnect
        arg = @conninfo + [@read_timeout, @write_timeout]
        initialize *arg
        retry
      rescue Timeout::Error
        raise ClientError, "write timeout"
      end
    end

    # Send one packet
    # === Argument
    # packet :: [*Packet]
    def send_packet(packet)
      write packet.serialize
    end

    # Read EOF packet
    # === Exception
    # ProtocolError :: packet is not EOF
    def read_eof_packet
      data = read
      raise ProtocolError, "packet is not EOF" unless self.class.eof_packet? data
    end

    # Read initial packet
    # === Return
    # InitialPacket ::
    # === Exception
    # ProtocolError :: invalid packet
    def read_initial_packet
      InitialPacket.parse read
    end

    # Read result packet
    # === Return
    # ResultPacket ::
    def read_result_packet
      ResultPacket.parse read
    end

    # Read field packet
    # === Return
    # FieldPacket :: packet data
    # === Exception
    # ProtocolError :: invalid packet
    def read_field_packet
      FieldPacket.parse read
    end

    # Read prepare result packet
    # === Return
    # PrepareResultPacket ::
    # === Exception
    # ProtocolError :: invalid packet
    def read_prepare_result_packet
      PrepareResultPacket.parse read
    end

    # Send simple command
    # === Argument
    # packet :: TxPacket class
    # args   :: packet parameter
    # === Return
    # [String] received data
    def simple_command(packet, *args)
      synchronize do
        reset
        send_packet packet.new(*args)
        read
      end
    end

    # initial negotiate and authenticate.
    # === Argument
    # user    :: [String / nil] username
    # passwd  :: [String / nil] password
    # db      :: [String / nil] default database name. nil: no default.
    # flag    :: [Integer] client flag
    # charset :: [Mysql::Charset / nil] charset for connection. nil: use server's charset
    def authenticate(user, passwd, db, flag, charset)
      synchronize do
        init_packet = read_initial_packet
        @server_info = init_packet.server_version
        @server_version = init_packet.server_version.split(/\D/)[0,3].inject{|a,b|a.to_i*100+b.to_i}
        @thread_id = init_packet.thread_id
        client_flags = CLIENT_LONG_PASSWORD | CLIENT_LONG_FLAG | CLIENT_TRANSACTIONS | CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION
        client_flags |= CLIENT_CONNECT_WITH_DB if db
        client_flags |= flag
        @charset = charset
        unless @charset
          @charset = Charset.by_number(init_packet.server_charset)
          @charset.encoding       # raise error if unsupported charset
        end
        netpw = encrypt_password passwd, init_packet.scramble_buff
        auth_packet = Protocol::AuthenticationPacket.new client_flags, 1024**3, @charset.number, user, netpw, db
        send_packet auth_packet
        read            # skip OK packet
      end
    end

    # Quit command
    def quit_command
      synchronize do
        reset
        send_packet QuitPacket.new
        close
      end
    end

    # Query command
    # === Argument
    # query :: [String] query string
    # === Return
    # [Integer / nil] number of fields of results. nil if no results.
    def query_command(query)
      synchronize do
        reset
        send_packet QueryPacket.new(@charset.convert(query))
        get_result
      end
    end

    # get result of query.
    # === Return
    # [integer / nil] number of fields of results. nil if no results.
    def get_result
      res_packet = read_result_packet
      if res_packet.field_count.to_i > 0  # result data exists
        return res_packet.field_count
      end
      if res_packet.field_count.nil?      # LOAD DATA LOCAL INFILE
        filename = res_packet.message
        File.open(filename){|f| write f}
        write nil  # EOF mark
        read
      end
      @affected_rows, @insert_id, @server_status, @warning_count, @message =
        res_packet.affected_rows, res_packet.insert_id, res_packet.server_status, res_packet.warning_count, res_packet.message
      return nil
    end

    def retr_fields(n)
      fields = n.times.map{Field.new read_field_packet}
      read_eof_packet
      fields
    end

    def retr_all_records(fields)
      all_recs = []
      until self.class.eof_packet?(data = read)
        all_recs.push fields.map{self.class.lcs2str! data}
      end
      @server_status = data[3].ord
      all_recs
    end

    # Field list command
    # === Argument
    # table :: [String] table name.
    # field :: [String / nil] field name that may contain wild card.
    # === Return
    # [Array of Field] field list
    def field_list_command(table, field)
      synchronize do
        reset
        send_packet FieldListPacket.new(table, field)
        fields = []
        until self.class.eof_packet?(data = read)
          fields.push Field.new(FieldPacket.parse(data))
        end
        return fields
      end
    end

    # Process info command
    # === Return
    # [Array of Field] field list
    def process_info_command
      synchronize do
        reset
        send_packet ProcessInfoPacket.new
        field_count = self.class.lcb2int!(read)
        fields = field_count.times.map{Field.new read_field_packet}
        read_eof_packet
        return fields
      end
    end

    # Ping command
    def ping_command
      simple_command PingPacket
    end

    # Kill command
    def kill_command(pid)
      simple_command ProcessKillPacket, pid
    end

    # Refresh command
    def refresh_command(op)
      simple_command RefreshPacket, op
    end

    # Set option command
    def set_option_command(opt)
      simple_command SetOptionPacket, opt
    end

    # Shutdown command
    def shutdown_command(level)
      simple_command ShutdownPacket, level
    end

    # Statistics command
    def statistics_command
      simple_command StatisticsPacket
    end

    # Stmt prepare command
    # === Argument
    # stmt :: [String] prepared statement
    # === Return
    # [Integer] statement id
    # [Integer] number of parameters
    # [Array of Field] field list
    def stmt_prepare_command(stmt)
      synchronize do
        reset
        send_packet PreparePacket.new(charset.convert(stmt))
        res_packet = read_prepare_result_packet
        if res_packet.param_count > 0
          res_packet.param_count.times{read}    # skip parameter packet
          read_eof_packet
        end
        if res_packet.field_count > 0
          fields = res_packet.field_count.times.map{Field.new read_field_packet}
          read_eof_packet
        else
          fields = []
        end
        return res_packet.statement_id, res_packet.param_count, fields
      end
    end

    # Stmt execute command
    # === Argument
    # stmt_id :: [Integer] statement id
    # values  :: [Array] parameters
    # === Return
    # [Integer] number of fields
    def stmt_execute_command(stmt_id, values)
      synchronize do
        reset
        send_packet ExecutePacket.new(stmt_id, Mysql::Stmt::CURSOR_TYPE_NO_CURSOR, values)
        return get_result
      end
    end

    # Stmt close command
    # === Argument
    # stmt_id :: [Integer] statement id
    def stmt_close_command(stmt_id)
      synchronize do
        reset
        send_packet StmtClosePacket.new(stmt_id)
      end
    end

    private

    # encrypt password
    # === Argument
    # plain    :: [String] plain password.
    # scramble :: [String] scramble code from initial packet.
    # === Return
    # [String] encrypted password
    def encrypt_password(plain, scramble)
      return "" if plain.nil? or plain.empty?
      hash_stage1 = Digest::SHA1.digest plain
      hash_stage2 = Digest::SHA1.digest hash_stage1
      return hash_stage1.unpack("C*").zip(Digest::SHA1.digest(scramble+hash_stage2).unpack("C*")).map{|a,b| a^b}.pack("C*")
    end

    # client->server packet base class
    class TxPacket
    end

    # server->client packet base class
    class RxPacket
    end

    # Initial packet
    class InitialPacket < RxPacket
      def self.parse(data)
        protocol_version, server_version, thread_id, scramble_buff, f0,
        server_capabilities, server_charset, server_status, f1,
        rest_scramble_buff = data.unpack("CZ*Va8CvCva13Z13")
        raise ProtocolError, "unsupported version: #{protocol_version}" unless protocol_version == VERSION
        raise ProtocolError, "invalid packet: f0=#{f0}" unless f0 == 0
        raise ProtocolError, "invalid packet: f1=#{f1.inspect}" unless f1 == "\0\0\0\0\0\0\0\0\0\0\0\0\0"
        scramble_buff.concat rest_scramble_buff
        self.new protocol_version, server_version, thread_id, server_capabilities, server_charset, server_status, scramble_buff
      end

      attr_accessor :protocol_version, :server_version, :thread_id, :server_capabilities, :server_charset, :server_status, :scramble_buff

      def initialize(*args)
        @protocol_version, @server_version, @thread_id, @server_capabilities, @server_charset, @server_status, @scramble_buff = args
      end

      def crypt_password(plain)
        return "" if plain.nil? or plain.empty?
        hash_stage1 = Digest::SHA1.digest plain
        hash_stage2 = Digest::SHA1.digest hash_stage1
        return hash_stage1.unpack("C*").zip(Digest::SHA1.digest(@scramble_buff+hash_stage2).unpack("C*")).map{|a,b| a^b}.pack("C*")
      end
    end

    # Authentication packet
    class AuthenticationPacket < TxPacket
      attr_accessor :client_flags, :max_packet_size, :charset_number, :username, :scrambled_password, :databasename

      def initialize(*args)
        @client_flags, @max_packet_size, @charset_number, @username, @scrambled_password, @databasename = args
      end

      def serialize
        [
          client_flags,
          max_packet_size,
          Protocol.lcb(charset_number),
          "",                   # always 0x00 * 23
          username,
          Protocol.lcs(scrambled_password),
          databasename
        ].pack("VVa*a23Z*A*Z*")
      end
    end

    # Quit packet
    class QuitPacket < TxPacket
      def serialize
        [COM_QUIT].pack("C")
      end
    end

    # Query packet
    class QueryPacket < TxPacket
      attr_accessor :query

      def initialize(*args)
        @query, = args
      end

      def serialize
        [COM_QUERY, query].pack("Ca*")
      end
    end

    # Result packet
    class ResultPacket < RxPacket
      def self.parse(data)
        field_count = Protocol.lcb2int! data
        if field_count == 0
          affected_rows = Protocol.lcb2int! data
          insert_id = Protocol.lcb2int!(data)
          server_status, warning_count, message = data.unpack("vva*")
          return self.new(field_count, affected_rows, insert_id, server_status, warning_count, Protocol.lcs2str!(message))
        elsif field_count.nil?   # LOAD DATA LOCAL INFILE
          return self.new(nil, nil, nil, nil, nil, data)
        else
          return self.new(field_count)
        end
      end

      attr_accessor :field_count, :affected_rows, :insert_id, :server_status, :warning_count, :message

      def initialize(*args)
        @field_count, @affected_rows, @insert_id, @server_status, @warning_count, @message = args
      end
    end

    # Field packet
    class FieldPacket < RxPacket
      def self.parse(data)
        first = Protocol.lcs2str! data
        db = Protocol.lcs2str! data
        table = Protocol.lcs2str! data
        org_table = Protocol.lcs2str! data
        name = Protocol.lcs2str! data
        org_name = Protocol.lcs2str! data
        f0, charsetnr, length, type, flags, decimals, f1, data = data.unpack("CvVCvCva*")
        raise ProtocolError, "invalid packet: f1=#{f1}" unless f1 == 0
        default = Protocol.lcs2str! data
        return self.new(db, table, org_table, name, org_name, charsetnr, length, type, flags, decimals, default)
      end

      attr_accessor :db, :table, :org_table, :name, :org_name, :charsetnr, :length, :type, :flags, :decimals, :default

      def initialize(*args)
        @db, @table, @org_table, @name, @org_name, @charsetnr, @length, @type, @flags, @decimals, @default = args
      end
    end

    # Prepare packet
    class PreparePacket < TxPacket
      attr_accessor :query

      def initialize(*args)
        @query, = args
      end

      def serialize
        [COM_STMT_PREPARE, query].pack("Ca*")
      end
    end

    # Prepare result packet
    class PrepareResultPacket < RxPacket
      def self.parse(data)
        raise ProtocolError, "invalid packet" unless data.slice!(0) == ?\0
        statement_id, field_count, param_count, f, warning_count = data.unpack("VvvCv")
        raise ProtocolError, "invalid packet" unless f == 0x00
        self.new statement_id, field_count, param_count, warning_count
      end

      attr_accessor :statement_id, :field_count, :param_count, :warning_count

      def initialize(*args)
        @statement_id, @field_count, @param_count, @warning_count = args
      end
    end

    # Execute packet
    class ExecutePacket < TxPacket
      attr_accessor :statement_id, :cursor_type, :values

      def initialize(*args)
        @statement_id, @cursor_type, @values = args
      end

      def serialize
        nbm = null_bitmap values
        netvalues = ""
        types = values.map do |v|
          t, n = Protocol.value2net v
          netvalues.concat n if v
          t
        end
        [Mysql::COM_STMT_EXECUTE, statement_id, cursor_type, 1, nbm, 1, types.pack("v*"), netvalues].pack("CVCVa*Ca*a*")
      end

      private

      # make null bitmap
      #
      # If values is [1, nil, 2, 3, nil] then returns "\x12"(0b10010).
      def null_bitmap(values)
        bitmap = values.enum_for(:each_slice,8).map do |vals|
          vals.reverse.inject(0){|b, v|(b << 1 | (v ? 0 : 1))}
        end
        return bitmap.pack("C*")
      end

    end

    # Fetch packet
    class FetchPacket < TxPacket
      attr_accessor :statement_id, :fetch_length

      def initialize(*args)
        @statement_id, @fetch_length = args
      end

      def serialize
        [Mysql::COM_STMT_FETCH, statement_id, fetch_length].pack("CVV")
      end
    end

    # Stmt close packet
    class StmtClosePacket < TxPacket
      attr_accessor :statement_id

      def initialize(*args)
        @statement_id, = args
      end

      def serialize
        [Mysql::COM_STMT_CLOSE, statement_id].pack("CV")
      end
    end

    class FieldListPacket < TxPacket
      def initialize(table, field=nil)
        @table, @field = table, field
      end

      def serialize
        [Mysql::COM_FIELD_LIST, @table, 0, @field].pack("Ca*Ca*")
      end
    end

    class SetOptionPacket < TxPacket
      def initialize(*args)
        @option, = args
      end

      def serialize
        [Mysql::COM_SET_OPTION, @option].pack("Cv")
      end
    end

    class CreateDbPacket < TxPacket
      def initialize(db)
        @db = db
      end

      def serialize
        [Mysql::COM_CREATE_DB, @db].pack("Ca*")
      end
    end

    class DropDbPacket < TxPacket
      def initialize(db)
        @db = db
      end

      def serialize
        [Mysql::COM_DROP_DB, @db].pack("Ca*")
      end
    end

    class RefreshPacket < TxPacket
      def initialize(opt)
        @opt = opt
      end

      def serialize
        [Mysql::COM_REFRESH, @opt].pack("CC")
      end
    end

    class ShutdownPacket < TxPacket
      def initialize(level)
        @level = level
      end
      def serialize
        [Mysql::COM_SHUTDOWN, level].pack("CC")
      end
    end

    class StatisticsPacket < TxPacket
      def serialize
        [Mysql::COM_STATISTICS].pack("C")
      end
    end

    class ProcessInfoPacket < TxPacket
      def serialize
        [Mysql::COM_PROCESS_INFO].pack("C")
      end
    end

    class ProcessKillPacket < TxPacket
      def initialize(pid)
        @pid = pid
      end
      def serialize
        [Mysql::COM_PROCESS_KILL, @pid].pack("CV")
      end
    end

    class PingPacket < TxPacket
      def serialize
        [Mysql::COM_PING].pack("C")
      end
    end

  end
end
