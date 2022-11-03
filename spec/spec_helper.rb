require 'rspec'

# for power_assert
Fixnum = Integer unless defined? Fixnum
Bignum = Integer unless defined? Bignum

require 'rspec-power_assert'
require 'mysql'

# MYSQL_USER must have ALL privilege for MYSQL_DATABASE.* and RELOAD privilege for *.*
MYSQL_SERVER   = ENV['MYSQL_SERVER']
MYSQL_USER     = ENV['MYSQL_USER']
MYSQL_PASSWORD = ENV['MYSQL_PASSWORD']
MYSQL_DATABASE = ENV['MYSQL_DATABASE'] || "test_for_mysql_ruby"
MYSQL_PORT     = ENV['MYSQL_PORT']
MYSQL_SOCKET   = ENV['MYSQL_SOCKET']

RSpec::PowerAssert.example_assertion_alias :assert
