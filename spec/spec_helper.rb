require 'rspec'
require 'power_assert'
require 'mysql'

class RSpec::Core::ExampleGroup  # rubocop:disable Style/ClassAndModuleChildren
  def assert(&block)
    PowerAssert.start(block, assertion_method: __callee__) do |pa|
      result = pa.yield
      message = pa.message_proc.call
      unless result
        ex = RSpec::Expectations::ExpectationNotMetError.new(message)
        RSpec::Support.notify_failure(ex)
      end
    end
  end
end

conf = "#{__dir__}/config.rb"
File.write(conf, File.read("#{conf}.example")) unless File.exist? conf
load conf
