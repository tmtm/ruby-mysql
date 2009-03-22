Gem::Specification.new do |s|
  s.name = %q{ruby-mysql}
  s.version = "0.0.0"

  s.required_rubygems_version = Gem::Requirement.new(">= 0") if s.respond_to? :required_rubygems_version=
  s.authors = ["tommy"]
  s.date = %q{2009-03-22}
  s.description = %q{MySQL connector for Ruby}
  s.email = %q{tommy@tmtm.org}
  s.extra_rdoc_files = ["README", "ChangeLog"]
  s.files = ["README", "ChangeLog", "Rakefile", "lib/mysql", "lib/mysql/constants.rb", "lib/mysql/compat.rb", "lib/mysql/protocol.rb", "lib/mysql/cache.rb", "lib/mysql/charset.rb", "lib/mysql/error.rb", "lib/mysql.rb~", "lib/mysql.rb"]
  s.has_rdoc = true
  s.homepage = %q{http://github.com/tmtm/ruby-mysql}
  s.rdoc_options = ["--title", "ruby-mysql documentation", "--charset", "utf-8", "--opname", "index.html", "--line-numbers", "--main", "README", "--inline-source", "--exclude", "^(examples|extras)/"]
  s.require_paths = ["lib"]
  s.required_ruby_version = Gem::Requirement.new(">= 1.8.7")
  s.rubyforge_project = %q{rubymysql}
  s.rubygems_version = %q{1.2.0}
  s.summary = %q{MySQL connector for Ruby}

  if s.respond_to? :specification_version then
    current_version = Gem::Specification::CURRENT_SPECIFICATION_VERSION
    s.specification_version = 2

    if current_version >= 3 then
    else
    end
  else
  end
end
