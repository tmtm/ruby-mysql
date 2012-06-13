m = Mysql.real_connect(MYSQL_SERVER, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT, MYSQL_SOCKET)
p = m.prepare((['select * from bench_test']*100).join(' union all '))
10.times do
  p.execute.each{}
end
