require_relative 'lib/mysql'

Gem::Specification.new do |s|
  s.name = 'ruby-mysql'
  s.version = Mysql::VERSION
  s.summary = 'MySQL connector'
  s.authors = ['Tomita Masahiro']
  s.description = 'This is MySQL connector. pure Ruby version'
  s.email = 'tommy@tmtm.org'
  s.homepage = 'http://github.com/tmtm/ruby-mysql'
  s.files = ['README.md', 'CHANGELOG.md', 'lib/mysql.rb', 'lib/mysql/constants.rb', 'lib/mysql/protocol.rb', 'lib/mysql/charset.rb', 'lib/mysql/error.rb', 'lib/mysql/packet.rb', 'lib/mysql/authenticator.rb'] + Dir.glob('lib/mysql/authenticator/*.rb')
  s.license = 'Ruby'
  s.required_ruby_version = '>= 2.6.0'
  s.metadata['homepage_uri'] = 'http://github.com/tmtm/ruby-mysql'
  s.metadata['documentation_uri'] = 'https://www.rubydoc.info/gems/ruby-mysql'
  s.metadata['source_code_uri'] = 'http://github.com/tmtm/ruby-mysql'
  s.metadata['rubygems_mfa_required'] = 'true'
  s.add_development_dependency 'rspec'
  s.add_development_dependency 'rspec-power_assert'
  s.add_development_dependency 'rubocop'
end
