my = Mysql.connect(MYSQL_SERVER, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT, MYSQL_SOCKET)
my.query(<<~SQL)
  create temporary table test (
    tiny tinyint,
    utiny tinyint unsigned,
    small smallint,
    usmall smallint unsigned,
    medium mediumint,
    umedium mediumint unsigned,
    i int,
    ui int unsigned,
    big bigint,
    ubig bigint unsigned,
    f float,
    d double,
    c char(100),
    vc varchar(100),
    date date,
    datetime datetime,
    timestamp timestamp
  ) charset utf8 engine blackhole
SQL

t = Time.now
p = my.prepare('insert into test values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)')
100000.times do
  p.execute(123,
            123,
            12345,
            12345,
            1234567,
            1234567,
            123456789,
            123456789,
            12345678900,
            12345678900,
            123.456,
            123.456,
            'abcdefghijklmnopqrstuvwxyz',
            'abcdefghijklmnopqrstuvwxyz',
            t,
            t,
            t)
end
