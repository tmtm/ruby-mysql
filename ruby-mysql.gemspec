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
  s.test_files = Dir.glob('test/**/*.rb')
  s.license = 'Ruby'
  s.metadata['homepage_uri'] = 'http://github.com/tmtm/ruby-mysql'
  s.metadata['documentation_uri'] = 'https://www.rubydoc.info/github/tmtm/ruby-mysql/'
  s.metadata['source_code_uri'] = 'http://github.com/tmtm/ruby-mysql'
end
