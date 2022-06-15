# coding: ascii-8bit
# Copyright (C) 2003 TOMITA Masahiro
# mailto:tommy@tmtm.org

class Mysql
  # Command
  COM_SLEEP               = 0
  COM_QUIT                = 1
  COM_INIT_DB             = 2
  COM_QUERY               = 3
  COM_FIELD_LIST          = 4
  COM_CREATE_DB           = 5
  COM_DROP_DB             = 6
  COM_REFRESH             = 7
  COM_SHUTDOWN            = 8
  COM_STATISTICS          = 9
  COM_PROCESS_INFO        = 10
  COM_CONNECT             = 11
  COM_PROCESS_KILL        = 12
  COM_DEBUG               = 13
  COM_PING                = 14
  COM_TIME                = 15
  COM_DELAYED_INSERT      = 16
  COM_CHANGE_USER         = 17
  COM_BINLOG_DUMP         = 18
  COM_TABLE_DUMP          = 19
  COM_CONNECT_OUT         = 20
  COM_REGISTER_SLAVE      = 21
  COM_STMT_PREPARE        = 22
  COM_STMT_EXECUTE        = 23
  COM_STMT_SEND_LONG_DATA = 24
  COM_STMT_CLOSE          = 25
  COM_STMT_RESET          = 26
  COM_SET_OPTION          = 27
  COM_STMT_FETCH          = 28
  COM_DAEMON              = 29
  COM_BINLOG_DUMP_GTID    = 30
  COM_RESET_CONNECTION    = 31
  COM_CLONE               = 32

  # Client flag
  CLIENT_LONG_PASSWORD                  = 1         # new more secure passwords
  CLIENT_FOUND_ROWS                     = 1 << 1    # Found instead of affected rows
  CLIENT_LONG_FLAG                      = 1 << 2    # Get all column flags
  CLIENT_CONNECT_WITH_DB                = 1 << 3    # One can specify db on connect
  CLIENT_NO_SCHEMA                      = 1 << 4    # Don't allow database.table.column
  CLIENT_COMPRESS                       = 1 << 5    # Can use compression protocol
  CLIENT_ODBC                           = 1 << 6    # Odbc client
  CLIENT_LOCAL_FILES                    = 1 << 7    # Can use LOAD DATA LOCAL
  CLIENT_IGNORE_SPACE                   = 1 << 8    # Ignore spaces before '('
  CLIENT_PROTOCOL_41                    = 1 << 9    # New 4.1 protocol
  CLIENT_INTERACTIVE                    = 1 << 10   # This is an interactive client
  CLIENT_SSL                            = 1 << 11   # Switch to SSL after handshake
  CLIENT_IGNORE_SIGPIPE                 = 1 << 12   # IGNORE sigpipes
  CLIENT_TRANSACTIONS                   = 1 << 13   # Client knows about transactions
  CLIENT_RESERVED                       = 1 << 14   # Old flag for 4.1 protocol
  CLIENT_SECURE_CONNECTION              = 1 << 15   # New 4.1 authentication
  CLIENT_MULTI_STATEMENTS               = 1 << 16   # Enable/disable multi-stmt support
  CLIENT_MULTI_RESULTS                  = 1 << 17   # Enable/disable multi-results
  CLIENT_PS_MULTI_RESULTS               = 1 << 18   # Multi-results in PS-protocol
  CLIENT_PLUGIN_AUTH                    = 1 << 19   # Client supports plugin authentication
  CLIENT_CONNECT_ATTRS                  = 1 << 20   # Client supports connection attribute
  CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA = 1 << 21   # Enable authentication response packet to be larger than 255 bytes.
  CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS   = 1 << 22   # Don't close the connection for a connection with expired password.
  CLIENT_SESSION_TRACK                  = 1 << 23   # Capable of handling server state change information. Its a hint to the server to include the state change information in Ok packet.
  CLIENT_DEPRECATE_EOF                  = 1 << 24   # Client no longer needs EOF packet
  CLIENT_OPTIONAL_RESULTSET_METADATA    = 1 << 25   # The client can handle optional metadata information in the resultset.
  CLIENT_ZSTD_COMPRESSION_ALGORITHM     = 1 << 26   # Compression protocol extended to support zstd compression method
  CLIENT_CAPABILITY_EXTENSION           = 1 << 29   # This flag will be reserved to extend the 32bit capabilities structure to 64bits.
  CLIENT_SSL_VERIFY_SERVER_CERT         = 1 << 30   # Verify server certificate.
  CLIENT_REMEMBER_OPTIONS               = 1 << 31   # Don't reset the options after an unsuccessful connect

  # Connection Option
  OPT_CONNECT_TIMEOUT              = 0
  OPT_COMPRESS                     = 1
  OPT_NAMED_PIPE                   = 2
  INIT_COMMAND                     = 3
  READ_DEFAULT_FILE                = 4
  READ_DEFAULT_GROUP               = 5
  SET_CHARSET_DIR                  = 6
  SET_CHARSET_NAME                 = 7
  OPT_LOCAL_INFILE                 = 8
  OPT_PROTOCOL                     = 9
  SHARED_MEMORY_BASE_NAME          = 10
  OPT_READ_TIMEOUT                 = 11
  OPT_WRITE_TIMEOUT                = 12
  OPT_USE_RESULT                   = 13
  REPORT_DATA_TRUNCATION           = 14
  OPT_RECONNECT                    = 15
  PLUGIN_DIR                       = 16
  DEFAULT_AUTH                     = 17
  OPT_BIND                         = 18
  OPT_SSL_KEY                      = 19
  OPT_SSL_CERT                     = 20
  OPT_SSL_CA                       = 21
  OPT_SSL_CAPATH                   = 22
  OPT_SSL_CIPHER                   = 23
  OPT_SSL_CRL                      = 24
  OPT_SSL_CRLPATH                  = 25
  OPT_CONNECT_ATTR_RESET           = 26
  OPT_CONNECT_ATTR_ADD             = 27
  OPT_CONNECT_ATTR_DELETE          = 28
  SERVER_PUBLIC_KEY                = 29
  ENABLE_CLEARTEXT_PLUGIN          = 30
  OPT_CAN_HANDLE_EXPIRED_PASSWORDS = 31
  OPT_MAX_ALLOWED_PACKET           = 32
  OPT_NET_BUFFER_LENGTH            = 33
  OPT_TLS_VERSION                  = 34
  OPT_SSL_MODE                     = 35
  OPT_GET_SERVER_PUBLIC_KEY        = 36
  OPT_RETRY_COUNT                  = 37
  OPT_OPTIONAL_RESULTSET_METADATA  = 38
  OPT_SSL_FIPS_MODE                = 39
  OPT_TLS_CIPHERSUITES             = 40
  OPT_COMPRESSION_ALGORITHMS       = 41
  OPT_ZSTD_COMPRESSION_LEVEL       = 42
  OPT_LOAD_DATA_LOCAL_DIR          = 43

  # SSL Mode
  SSL_MODE_DISABLED        = 1
  SSL_MODE_PREFERRED       = 2
  SSL_MODE_REQUIRED        = 3
  SSL_MODE_VERIFY_CA       = 4
  SSL_MODE_VERIFY_IDENTITY = 5

  # Server Option
  OPTION_MULTI_STATEMENTS_ON  = 0
  OPTION_MULTI_STATEMENTS_OFF = 1

  # Server Status
  SERVER_STATUS_IN_TRANS             = 1
  SERVER_STATUS_AUTOCOMMIT           = 1 << 1
  SERVER_MORE_RESULTS_EXISTS         = 1 << 3
  SERVER_QUERY_NO_GOOD_INDEX_USED    = 1 << 4
  SERVER_QUERY_NO_INDEX_USED         = 1 << 5
  SERVER_STATUS_CURSOR_EXISTS        = 1 << 6
  SERVER_STATUS_LAST_ROW_SENT        = 1 << 7
  SERVER_STATUS_DB_DROPPED           = 1 << 8
  SERVER_STATUS_NO_BACKSLASH_ESCAPES = 1 << 9
  SERVER_STATUS_METADATA_CHANGED     = 1 << 10
  SERVER_QUERY_WAS_SLOW              = 1 << 11
  SERVER_PS_OUT_PARAMS               = 1 << 12
  SERVER_STATUS_IN_TRANS_READONLY    = 1 << 13
  SERVER_SESSION_STATE_CHANGED       = 1 << 14

  # Refresh parameter
  REFRESH_GRANT            = 1
  REFRESH_LOG              = 1 << 1
  REFRESH_TABLES           = 1 << 2
  REFRESH_HOSTS            = 1 << 3
  REFRESH_STATUS           = 1 << 4
  REFRESH_THREADS          = 1 << 5
  REFRESH_SLAVE            = 1 << 6
  REFRESH_MASTER           = 1 << 7
  REFRESH_ERROR_LOG        = 1 << 8
  REFRESH_ENGINE_LOG       = 1 << 9
  REFRESH_BINARY_LOG       = 1 << 10
  REFRESH_RELAY_LOG        = 1 << 11
  REFRESH_GENERAL_LOG      = 1 << 12
  REFRESH_SLOW_LOG         = 1 << 13
  REFRESH_READ_LOCK        = 1 << 14
  REFRESH_FAST             = 1 << 15
  REFRESH_QUERY_CACHE      = 1 << 16
  REFRESH_QUERY_CACHE_FREE = 1 << 17
  REFRESH_DES_KEY_FILE     = 1 << 18
  REFRESH_USER_RESOURCES   = 1 << 19
  REFRESH_FOR_EXPORT       = 1 << 20
  REFRESH_OPTIMIZER_COSTS  = 1 << 21
  REFRESH_PERSIST          = 1 << 22

  SESSION_TRACK_SYSTEM_VARIABLES            = 0  # Session system variables
  SESSION_TRACK_SCHEMA                      = 1  # Current schema
  SESSION_TRACK_STATE_CHANGE                = 2  # track session state changes
  SESSION_TRACK_GTIDS                       = 3  # See also: session_track_gtids
  SESSION_TRACK_TRANSACTION_CHARACTERISTICS = 4  # Transaction chistics
  SESSION_TRACK_TRANSACTION_STATE           = 5  # Transaction state

  class Field
    # Field type
    TYPE_DECIMAL     = 0
    TYPE_TINY        = 1
    TYPE_SHORT       = 2
    TYPE_LONG        = 3
    TYPE_FLOAT       = 4
    TYPE_DOUBLE      = 5
    TYPE_NULL        = 6
    TYPE_TIMESTAMP   = 7
    TYPE_LONGLONG    = 8
    TYPE_INT24       = 9
    TYPE_DATE        = 10
    TYPE_TIME        = 11
    TYPE_DATETIME    = 12
    TYPE_YEAR        = 13
    TYPE_NEWDATE     = 14
    TYPE_VARCHAR     = 15
    TYPE_BIT         = 16
    TYPE_TIMESTAMP2  = 17
    TYPE_DATETIME2   = 18
    TYPE_TIME2       = 19
    TYPE_TYPED_ARRAY = 20
    TYPE_INVALID     = 243
    TYPE_BOOL        = 244
    TYPE_JSON        = 245
    TYPE_NEWDECIMAL  = 246
    TYPE_ENUM        = 247
    TYPE_SET         = 248
    TYPE_TINY_BLOB   = 249
    TYPE_MEDIUM_BLOB = 250
    TYPE_LONG_BLOB   = 251
    TYPE_BLOB        = 252
    TYPE_VAR_STRING  = 253
    TYPE_STRING      = 254
    TYPE_GEOMETRY    = 255
    TYPE_CHAR        = TYPE_TINY
    TYPE_INTERVAL    = TYPE_ENUM

    # Flag
    NOT_NULL_FLAG                  = 1
    PRI_KEY_FLAG                   = 2
    UNIQUE_KEY_FLAG                = 4
    MULTIPLE_KEY_FLAG              = 8
    BLOB_FLAG                      = 16
    UNSIGNED_FLAG                  = 32
    ZEROFILL_FLAG                  = 64
    BINARY_FLAG                    = 128
    ENUM_FLAG                      = 256
    AUTO_INCREMENT_FLAG            = 512
    TIMESTAMP_FLAG                 = 1024
    SET_FLAG                       = 2048
    NO_DEFAULT_VALUE_FLAG          = 4096
    ON_UPDATE_NOW_FLAG             = 8192
    NUM_FLAG                       = 32768
    PART_KEY_FLAG                  = 16384
    GROUP_FLAG                     = 32768
    UNIQUE_FLAG                    = 65536
    BINCMP_FLAG                    = 131072
    GET_FIXED_FIELDS_FLAG          = 1 << 18
    FIELD_IN_PART_FUNC_FLAG        = 1 << 19
    FIELD_IN_ADD_INDEX             = 1 << 20
    FIELD_IS_RENAMED               = 1 << 21
    FIELD_FLAGS_STORAGE_MEDIA_MASK = 3 << 22
    FIELD_FLAGS_COLUMN_FORMAT_MASK = 3 << 24
    FIELD_IS_DROPPED               = 1 << 26
    EXPLICIT_NULL_FLAG             = 1 << 27
    FIELD_IS_MARKED                = 1 << 28
    NOT_SECONDARY_FLAG             = 1 << 29
  end

  class Stmt
    # Cursor type
    CURSOR_TYPE_NO_CURSOR  = 0
    CURSOR_TYPE_READ_ONLY  = 1
    CURSOR_TYPE_FOR_UPDATE = 2
    CURSOR_TYPE_SCROLLABLE = 4
  end
end
