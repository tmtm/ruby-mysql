Gem::Specification.new do |s|
  s.name = 'ruby-mysql'
  s.version = '2.9.4'
  s.summary = 'pure Ruby MySQL connector'
  s.authors = ['Tomita Masahiro']
  s.date = '2010-12-29'
  s.description = 'This is pure Ruby MySQL connector.'
  s.email = 'tommy@tmtm.org'
  s.homepage = 'http://github.com/tmtm/ruby-mysql'
  s.files = ['README.rdoc', 'ChangeLog', 'lib/mysql.rb', 'lib/mysql/constants.rb', 'lib/mysql/protocol.rb', 'lib/mysql/charset.rb', 'lib/mysql/error.rb']
  s.extra_rdoc_files = ['README.rdoc', 'ChangeLog']
  s.test_files = ['spec/mysql_spec.rb']
  s.has_rdoc = true
  s.license = 'Ruby\'s'
end
