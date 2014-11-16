# @example
#   rake test MYSQL_DATABASE=test MYSQL_USER=testuser MYSQL_UNIX_PORT=/var/run/mysqld/mysqld.sock
#
# environments:
# * MYSQL_SERVER    - default: 'localhost'
# * MYSQL_USER      - default: login user
# * MYSQL_PASSWORD  - default: no password
# * MYSQL_DATABASE  - default: 'test_for_mysql_ruby'
# * MYSQL_PORT      - default: 3306
# * MYSQL_SOCKET    - defualt: '/tmp/mysql.sock'
#
require 'rake/testtask'
Rake::TestTask.new do |t|
  t.pattern = 'test/test*.rb'
end
