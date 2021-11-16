## [3.0.0] - 2021-11-16

- `Mysql.new` no longer connect. use `Mysql.connect` or `Mysql#connect`.

- `Mysql.init` is removed. use `Mysql.new` instead.

- `Mysql.new`, `Mysql.conncet` and `Mysql#connect` takes URI object or URI string or Hash object.
  example:
      Mysql.connect('mysql://user:password@hostname:port/dbname?charset=ascii')
      Mysql.connect('mysql://user:password@%2Ftmp%2Fmysql.sock/dbname?charset=ascii') # for UNIX socket
      Mysql.connect('hostname', 'user', 'password', 'dbname')
      Mysql.connect(host: 'hostname', username: 'user', password: 'password', database: 'dbname')

- `Mysql.options` is removed. use `Mysql#param = value` instead.
  For example:
      m = Mysql.init
      m.options(Mysql::OPT_LOCAL_INFILE, true)
      m.connect(host, user, passwd)
  change to
      m = Mysql.new
      m.local_infile = true
      m.connect(host, user, passwd)
  or
      m = Mysql.connect(host, user, passwd, local_infile: true)

- `Mysql::Time` is removed.
  Instead, `Time` object is returned for the DATE, DATETIME, TIMESTAMP data,
  and `Integer` object is returned for the TIME data.
  If DATE, DATETIME, TIMESTAMP are invalid values for Time, nil is returned.

- meaningless methods are removed:
  * `bind_result`
  * `client_info`
  * `client_version`
  * `get_proto_info`
  * `get_server_info`
  * `get_server_version`
  * `proto_info`
  * `query_with_result`

- alias method are removed:
  * `get_host_info`: use `host_info`
  * `real_connect`: use `connect`
  * `real_query`: use `query`

- methods corresponding to deprecated APIs in MySQL are removed:
  * `list_dbs`: use `SHOW DATABASES`
  * `list_fields`: use `SHOW COLUMNS`
  * `list_processes`: use `SHOW PROCESSLIST`
  * `list_tables`: use `SHOW TABLES`
