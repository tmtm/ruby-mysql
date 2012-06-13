m = Mysql.real_connect(MYSQL_SERVER, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT, MYSQL_SOCKET)
100000.times do
  m.query('set @a=1')
end
