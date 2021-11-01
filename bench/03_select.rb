m = Mysql.connect(MYSQL_SERVER, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT, MYSQL_SOCKET)
10.times do
  m.query((['select * from bench_test']*100).join(' union all ')).each{}
end
