require 'mysql'
class Mysql::Result
  include Enumerable
end

MYSQL_SERVER   = ENV['MYSQL_SERVER']
MYSQL_USER     = ENV['MYSQL_USER']
MYSQL_PASSWORD = ENV['MYSQL_PASSWORD']
MYSQL_DATABASE = ENV['MYSQL_DATABASE'] || 'test'
MYSQL_PORT     = ENV['MYSQL_PORT']
MYSQL_SOCKET   = ENV['MYSQL_SOCKET']

m = Mysql.real_connect(MYSQL_SERVER, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT, MYSQL_SOCKET)
unless m.query('show tables').any?{|tbl,| tbl == 'bench_test'}
  m.query <<EOS
create table bench_test (
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
) charset utf8 engine innodb
EOS
  t = Time.now.strftime('%Y%m%d%H%M%S')
  100.times do
    m.query("insert into bench_test values (123,123,123456,123456,1234567,1234567,123456789,123456789,12345678900,12345678900,123.456,123.456,'abcdefghijklmnopqrstuvwxyz','abcdefghijklmnopqrstuvwxyz','#{t}','#{t}','#{t}')")
  end
end

Dir.chdir(File.dirname(__FILE__))
Dir.glob('[0-9]*.rb').sort.each do |f|
  puts f
  now = Time.now
  load f
  p Time.now - now
end
