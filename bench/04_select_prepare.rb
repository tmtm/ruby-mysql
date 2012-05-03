m = Mysql.real_connect(MYSQL_SERVER, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT, MYSQL_SOCKET)
p = m.prepare 'select * from bench_test'
1000.times do
  p.execute.each{}
end
