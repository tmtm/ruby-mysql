# ruby-mysql

## Description

MySQL connector for Ruby.

## Installation

```
gem install ruby-mysql
```

## Synopsis

```ruby
my = Mysql.connect('mysql://username:password@hostname:port/dbname?charset=utf8mb4')
my.query("select col1, col2 from tblname").each do |col1, col2|
  p col1, col2
end
stmt = my.prepare('insert into tblname (col1,col2) values (?,?)')
stmt.execute 123, 'abc'
```

## Copyright

* Author: TOMITA Masahiro <tommy@tmtm.org>
* Copyright: Copyright 2008 TOMITA Masahiro
* License: MIT
