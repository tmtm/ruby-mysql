# Copyright (C) 2008 TOMITA Masahiro
# mailto:tommy@tmtm.org

# for compatibility

class Mysql
  class << self
    alias connect new
    alias real_connect new

    def init
      self.allocate
    end

    def client_version
      50067
    end

    def client_info
      "5.0.67"
    end
    alias get_client_info client_info

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

  alias orig_initialize initialize
  alias stmt_init stmt
  alias query simple_query

  def initialize(*args)
    if args.first.is_a? Hash || defined?(URI) && args.first.is_a?(URI) || args.first =~ /\Amysql:\/\//
      orig_initialize *args
    else
      host, user, password, db, port, socket, flag = args
      orig_initialize :host=>host, :user=>user, :password=>password, :db=>db, :port=>port, :socket=>socket, :flag=>flag
    end
  end

  def connect(host, user, password, db, port, socket, flag)
    initialize :host=>host, :user=>user, :password=>password, :db=>db, :port=>port, :socket=>socket, :flag=>flag
    self
  end
  alias real_connect connect

  def client_version
    self.class.client_version
  end

  def options(opt, val=nil)
    case opt
    when INIT_COMMAND
      @init_command = val
    when OPT_COMPRESS
      raise ClientError, "not implemented"
    when OPT_CONNECT_TIMEOUT
      @connect_timeout = val
    when OPT_GUESS_CONNECTION
      raise ClientError, "not implemented"
    when OPT_LOCAL_INFILE
      @local_infile = val
    when OPT_NAMED_PIPE
      raise ClientError, "not implemented"
    when OPT_PROTOCOL
      raise ClientError, "not implemented"
    when OPT_READ_TIMEOUT
      @read_timeout = val
    when OPT_USE_EMBEDDED_CONNECTION
      raise ClientError, "not implemented"
    when OPT_USE_REMOTE_CONNECTION
      raise ClientError, "not implemented"
    when OPT_WRITE_TIMEOUT
      @write_timeout = val
    when READ_DEFAULT_FILE
      raise ClientError, "not implemented"
    when READ_DEFAULT_GROUP
      raise ClientError, "not implemented"
    when SECURE_AUTH
      raise ClientError, "not implemented"
    when SET_CHARSET_DIR
      raise ClientError, "not implemented"
    when SET_CHARSET_NAME
      self.charset = val
    when SET_CLIENT_IP
      raise ClientError, "not implemented"
    when SHARED_MEMORY_BASE_NAME
      raise ClientError, "not implemented"
    else
      raise ClientError, "unknown option: #{opt}"
    end
    self
  end

  def sqlstate
    @stream ? @stream.sqlstate : "00000"
  end

  def store_result
    raise ClientError, "no result set" unless @fields
    Result.new @fields, @stream
  end

  def use_result
    raise ClientError, "no result set" unless @fields
    Result.new @fields, @stream, false
  end

  class Result
    def num_rows
      @records.length
    end

    def data_seek(n)
      @index = n
    end

    def row_tell
      @index
    end

    def row_seek(n)
      ret = @index
      @index = n
      ret
    end

    def free
      # do nothing
    end

    def fetch_field
      return nil if @field_index >= @fields.length
      ret = @fields[@field_index]
      @field_index += 1
      ret
    end

    def field_tell
      @field_index
    end

    def field_seek(n)
      @field_index = n
    end

    def fetch_field_direct(n)
      raise ClientError, "invalid argument: #{n}" if n < 0 or n >= @fields.length
      @fields[n]
    end

    def fetch_fields
      @fields
    end

    def fetch_lengths
      return nil unless @fetched_record
      @fetched_record.map{|c|c.nil? ? 0 : c.length}
    end

    def num_fields
      @fields.length
    end
  end

  class Statement
    def num_rows
      @records.length
    end

    def data_seek(n)
      @index = n
    end

    def row_tell
      @index
    end

    def row_seek(n)
      ret = @index
      @index = n
      ret
    end

    def field_count
      @fields.length
    end

    def free_result
      # do nothing
    end

    def result_metadata
      return nil if @fields.empty?
      Result.new @fields, nil, false
    end
  end
  Stmt = Statement
end
