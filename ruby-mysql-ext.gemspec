Gem::Specification.new do |s|
  s.name = 'ruby-mysql-ext'
  s.version = '2.9.13'
  s.summary = 'MySQL connector with extension'
  s.authors = ['Tomita Masahiro']
  s.date = '2014-11-16'
  s.description = 'This is MySQL connector with C extension.'
  s.email = 'tommy@tmtm.org'
  s.extensions = ['ext/mysql/extconf.rb']
  s.homepage = 'http://github.com/tmtm/ruby-mysql'
  s.files = ['README.rdoc', 'lib/mysql.rb', 'lib/mysql/constants.rb', 'lib/mysql/protocol.rb', 'lib/mysql/charset.rb', 'lib/mysql/error.rb', 'lib/mysql/packet.rb', 'ext/mysql/ext.c']
  s.extra_rdoc_files = ['README.rdoc']
  s.test_files = Dir.glob('test/**/*.rb')
  s.has_rdoc = true
  s.license = 'Ruby\'s'
end
