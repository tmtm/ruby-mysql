require 'rspec'

# for power_assert
Fixnum = Integer unless defined? Fixnum
Bignum = Integer unless defined? Bignum
require 'rspec-power_assert'
RSpec::PowerAssert.example_assertion_alias :assert

require 'mysql'

conf = "#{__dir__}/config.rb"
File.write(conf, File.read("#{conf}.example")) unless File.exist? conf
load conf
