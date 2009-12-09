require "rubygems"
require "spec"
require "uri"

require "#{File.dirname __FILE__}/../lib/mysql"

MYSQL_SERVER   = "localhost"
MYSQL_USER     = "test"
MYSQL_PASSWORD = "hogehoge"
MYSQL_DATABASE = "test"
MYSQL_PORT     = 3306
MYSQL_SOCKET   = "/var/run/mysqld/mysqld.sock"

URL = "mysql://#{MYSQL_USER}:#{MYSQL_PASSWORD}@#{MYSQL_SERVER}/#{MYSQL_DATABASE}"

describe 'Mysql::VERSION' do
  it 'returns client version' do
    Mysql::VERSION.should == 20900
  end
end

describe 'Mysql.init' do
  it 'returns Mysql object' do
    Mysql.init.should be_kind_of Mysql
  end
end

describe 'Mysql.real_connect' do
  after do
    @m.close
  end
  it 'connect to mysqld' do
    @m = Mysql.real_connect(MYSQL_SERVER, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT, MYSQL_SOCKET)
    @m.should be_kind_of Mysql
  end
end

describe 'Mysql.connect' do
  after do
    @m.close if @m
  end
  it 'connect to mysqld' do
    @m = Mysql.connect(MYSQL_SERVER, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT, MYSQL_SOCKET)
    @m.should be_kind_of Mysql
  end
end

describe 'Mysql.new' do
  after do
    @m.close if @m
  end
  it 'connect to mysqld' do
    @m = Mysql.new(MYSQL_SERVER, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT, MYSQL_SOCKET)
    @m.should be_kind_of Mysql
  end
end

describe 'Mysql.escape_string' do
  it 'escape special character' do
    Mysql.escape_string("abc'def\"ghi\0jkl%mno").should == "abc\\'def\\\"ghi\\0jkl%mno"
  end
end

describe 'Mysql.quote' do
  it 'escape special character' do
    Mysql.quote("abc'def\"ghi\0jkl%mno").should == "abc\\'def\\\"ghi\\0jkl%mno"
  end
end

describe 'Mysql.get_client_info' do
  it 'returns version as string' do
    Mysql.get_client_info.should =~ /^\d.\d+.\d+[a-z]?(-.*)?$/
  end
end

describe 'Mysql.client_info' do
  it 'returns version as string' do
    Mysql.client_info.should =~ /^\d.\d+.\d+[a-z]?(-.*)?$/
  end
end

describe 'Mysql#options' do
  before do
    @m = Mysql.init
  end
  it 'INIT_COMMAND' do
    @m.options(Mysql::INIT_COMMAND, "SET AUTOCOMMIT=0").should == @m
  end
  it 'OPT_CONNECT_TIMEOUT' do
    @m.options(Mysql::OPT_CONNECT_TIMEOUT, 10).should == @m
  end
  it 'OPT_READ_TIMEOUT' do
    @m.options(Mysql::OPT_READ_TIMEOUT, 10).should == @m
  end
  it 'OPT_WRITE_TIMEOUT' do
    @m.options(Mysql::OPT_WRITE_TIMEOUT, 10).should == @m
  end
end

describe 'Mysql#real_connect' do
  after do
    @m.close if @m
  end
  it 'connect to mysqld' do
    @m = Mysql.init
    @m.real_connect(MYSQL_SERVER, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT, MYSQL_SOCKET).should == @m
  end
end

describe 'Mysql#connect' do
  after do
    @m.close if @m
  end
  it 'connect to mysqld' do
    @m = Mysql.init
    @m.connect(MYSQL_SERVER, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT, MYSQL_SOCKET).should == @m
  end
end

describe 'Mysql' do
  before do
    @m = Mysql.new(MYSQL_SERVER, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT, MYSQL_SOCKET)
  end

  after do
    @m.close if @m
  end

  describe '#affected_rows' do
    it 'returns number of affected rows' do
      @m.query 'create temporary table t (id int)'
      @m.query 'insert into t values (1),(2)'
      @m.affected_rows.should == 2
    end
  end

  describe '#autocommit' do
    it 'change auto-commit mode'
  end

  describe '#more_results, #next_result' do
    it ''
  end

  describe '#query with block' do
    it ''
  end

  describe '#set_server_optoin' do
    it ''
  end

  describe '#sqlstate' do
    it ''
  end

  describe '#query_with_result' do
    it ''
  end

  describe '#reconnect' do
    it ''
  end
end

describe 'Mysql::Result' do
  before do
    @m = Mysql.new(MYSQL_SERVER, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT, MYSQL_SOCKET)
    @m.query 'create temporary table t (id int, str char(10), primary key (id))'
    @m.query "insert into t values (1,'abc'),(2,'defg'),(3,'hi'),(4,null)"
    @res = @m.query 'select * from t'
  end

  after do
    @m.close if @m
  end

  it '#num_fields returns number of fields' do
    @res.num_fields.should == 2
  end

  it '#num_rows returns number of records' do
    @res.num_rows.should == 4
  end

  it '#fetch_row returns one record as array for current record' do
    @res.fetch_row.should == ['1', 'abc']
    @res.fetch_row.should == ['2', 'defg']
    @res.fetch_row.should == ['3', 'hi']
    @res.fetch_row.should == ['4', nil]
    @res.fetch_row.should == nil
  end

  it '#fetch_hash returns one record as hash for current record' do
    @res.fetch_hash.should == {'id'=>'1', 'str'=>'abc'}
    @res.fetch_hash.should == {'id'=>'2', 'str'=>'defg'}
    @res.fetch_hash.should == {'id'=>'3', 'str'=>'hi'}
    @res.fetch_hash.should == {'id'=>'4', 'str'=>nil}
    @res.fetch_hash.should == nil
  end

  it '#fetch_hash(true) returns with table name' do
    @res.fetch_hash(true).should == {'t.id'=>'1', 't.str'=>'abc'}
    @res.fetch_hash(true).should == {'t.id'=>'2', 't.str'=>'defg'}
    @res.fetch_hash(true).should == {'t.id'=>'3', 't.str'=>'hi'}
    @res.fetch_hash(true).should == {'t.id'=>'4', 't.str'=>nil}
    @res.fetch_hash(true).should == nil
  end

  it '#each iterate block with a record' do
    ret = []
    @res.each do |a|
      ret.push a
    end
    ret.should == [["1","abc"], ["2","defg"], ["3","hi"], ["4",nil]]
  end

  it '#each_hash iterate block with a hash' do
    ret = []
    @res.each_hash do |a|
      ret.push a
    end
    ret.should == [{"id"=>"1","str"=>"abc"}, {"id"=>"2","str"=>"defg"}, {"id"=>"3","str"=>"hi"}, {"id"=>"4","str"=>nil}]
  end

  it '#data_seek set position of current record' do
    @res.fetch_row.should == ['1', 'abc']
    @res.fetch_row.should == ['2', 'defg']
    @res.fetch_row.should == ['3', 'hi']
    @res.data_seek 1
    @res.fetch_row.should == ['2', 'defg']
  end

  it '#row_tell returns position of current record, #row_seek set position of current record' do
    @res.fetch_row.should == ['1', 'abc']
    pos = @res.row_tell
    @res.fetch_row.should == ['2', 'defg']
    @res.fetch_row.should == ['3', 'hi']
    @res.row_seek pos
    @res.fetch_row.should == ['2', 'defg']
  end

  it '#field_tell returns position of current field, #field_seek set position of current field' do
    @res.field_tell.should == 0
    @res.fetch_field
    @res.field_tell.should == 1
    @res.fetch_field
    @res.field_tell.should == 2
    @res.field_seek 1
    @res.field_tell.should == 1
  end

  it '#fetch_field return current field' do
    f = @res.fetch_field
    f.name.should == 'id'
    f.table.should == 't'
    f.def.should == nil
    f.type.should == Mysql::Field::TYPE_LONG
    f.length.should == 11
    f.max_length == 1
    f.flags.should == Mysql::Field::NUM_FLAG|Mysql::Field::PRI_KEY_FLAG|Mysql::Field::PART_KEY_FLAG|Mysql::Field::NOT_NULL_FLAG
    f.decimals.should == 0

    f = @res.fetch_field
    f.name.should == 'str'
    f.table.should == 't'
    f.def.should == nil
    f.type.should == Mysql::Field::TYPE_STRING
    f.length.should == 10
    f.max_length == 4
    f.flags.should == 0
    f.decimals.should == 0

    @res.fetch_field.should == nil
  end

  it '#fetch_fields returns array of fields' do
    ret = @res.fetch_fields
    ret.size.should == 2
    ret[0].name.should == 'id'
    ret[1].name.should == 'str'
  end

  it '#fetch_field_direct returns field' do
    f = @res.fetch_field_direct 0
    f.name.should == 'id'
    f = @res.fetch_field_direct 1
    f.name.should == 'str'
    proc{@res.fetch_field_direct -1}.should raise_error Mysql::ClientError, 'invalid argument: -1'
    proc{@res.fetch_field_direct 2}.should raise_error Mysql::ClientError, 'invalid argument: 2'
  end

  it '#fetch_lengths returns array of length of field data' do
    @res.fetch_lengths.should == nil
    @res.fetch_row
    @res.fetch_lengths.should == [1, 3]
    @res.fetch_row
    @res.fetch_lengths.should == [1, 4]
    @res.fetch_row
    @res.fetch_lengths.should == [1, 2]
    @res.fetch_row
    @res.fetch_lengths.should == [1, 0]
    @res.fetch_row
    @res.fetch_lengths.should == nil
  end
end

describe 'Mysql::Field' do
  before do
    @m = Mysql.new(MYSQL_SERVER, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT, MYSQL_SOCKET)
    @m.query 'create temporary table t (id int, str char(10), primary key (id))'
    @m.query "insert into t values (1,'abc'),(2,'defg'),(3,'hi'),(4,null)"
    @res = @m.query 'select * from t'
  end

  after do
    @m.close if @m
  end

  it '#hash return field as hash' do
    @res.fetch_field.hash.should == {
      'name'       => 'id',
      'table'      => 't',
      'def'        => nil,
      'type'       => Mysql::Field::TYPE_LONG,
      'length'     => 11,
      'max_length' => 1,
      'flags'      => Mysql::Field::NUM_FLAG|Mysql::Field::PRI_KEY_FLAG|Mysql::Field::PART_KEY_FLAG|Mysql::Field::NOT_NULL_FLAG,
      'decimals'   => 0,
    }
    @res.fetch_field.hash.should == {
      'name'       => 'str',
      'table'      => 't',
      'def'        => nil,
      'type'       => Mysql::Field::TYPE_STRING,
      'length'     => 10,
      'max_length' => 4,
      'flags'      => 0,
      'decimals'   => 0,
    }
  end

  it '#inspect returns "#<Mysql::Field:name>"' do
    @res.fetch_field.inspect.should == '#<Mysql::Field:id>'
    @res.fetch_field.inspect.should == '#<Mysql::Field:str>'
  end

  it '#is_num? returns true if the field is numeric' do
    @res.fetch_field.is_num?.should == true
    @res.fetch_field.is_num?.should == false
  end

  it '#is_not_null? returns true if the field is not null' do
    @res.fetch_field.is_not_null?.should == true
    @res.fetch_field.is_not_null?.should == false
  end

  it '#is_pri_key? returns true if the field is primary key' do
    @res.fetch_field.is_pri_key?.should == true
    @res.fetch_field.is_pri_key?.should == false
  end
end

describe 'create Mysql::Stmt object:' do
  before do
    @m = Mysql.new(MYSQL_SERVER, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT, MYSQL_SOCKET)
  end

  after do
    @m.close if @m
  end

  it 'Mysql#stmt_init returns Mysql::Stmt object' do
    @m.stmt_init.should be_kind_of Mysql::Stmt
  end

  it 'Mysq;#prepare returns Mysql::Stmt object' do
    @m.prepare("select 1").should be_kind_of Mysql::Stmt
  end
end

describe 'Mysql::Stmt' do
  before do
    @m = Mysql.new(MYSQL_SERVER, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT, MYSQL_SOCKET)
    @s = @m.stmt_init
  end

  after do
    @s.close if @s
    @m.close if @m
  end

  it '#prepare returns self' do
    @s.prepare('select 1').should == @s
  end

  it '#prepare with invalid query raises error' do
    proc{@s.prepare 'invalid query'}.should raise_error Mysql::ParseError
  end

  it '#execute returns self' do
    @s.prepare 'select 1'
    @s.execute.should == @s
  end

  it '#affected_rows returns number of affected records' do
    @m.query 'create temporary table t (i int, c char(10))'
    @s.prepare 'insert into t values (?,?)'
    @s.execute 1, 'hoge'
    @s.affected_rows.should == 1
    @s.execute 2, 'hoge'
    @s.execute 3, 'hoge'
    @s.prepare 'update t set c=?'
    @s.execute 'fuga'
    @s.affected_rows.should == 3
  end

  describe '#bind_result' do
    before do
      @m.query 'create temporary table t (i int, c char(10), d double, t datetime)'
      @m.query 'insert into t values (123,"9abcdefg",1.2345,20091208100446)'
      @s.prepare 'select * from t'
    end

    it '(nil) make result format to be standard value' do
      @s.bind_result nil, nil, nil, nil
      @s.execute
      @s.fetch.should == [123, '9abcdefg', 1.2345, Mysql::Time.new(2009,12,8,10,4,46)]
    end

    it '(Numeric) make result format to be Integer value' do
      @s.bind_result Numeric, Numeric, Numeric, Numeric
      @s.execute
      @s.fetch.should == [123, 9, 1, 20091208100446]
    end

    it '(Integer) make result format to be Integer value' do
      @s.bind_result Integer, Integer, Integer, Integer
      @s.execute
      @s.fetch.should == [123, 9, 1, 20091208100446]
    end

    it '(Fixnum) make result format to be Integer value' do
      @s.bind_result Fixnum, Fixnum, Fixnum, Fixnum
      @s.execute
      @s.fetch.should == [123, 9, 1, 20091208100446]
    end

    it '(String) make result format to be String value' do
      @s.bind_result String, String, String, String
      @s.execute
      @s.fetch.should == ["123", "9abcdefg", "1.2345", "2009-12-08 10:04:46"]
    end

    it '(Float) make result format to be Float value' do
      @s.bind_result Float, Float, Float, Float
      @s.execute
      @s.fetch.should == [123.0, 9.0, 1.2345 , 20091208100446.0]
    end

    it '(Mysql::Time) make result format to be Mysql::Time value' do
      @s.bind_result Mysql::Time, Mysql::Time, Mysql::Time, Mysql::Time
      @s.execute
      @s.fetch.should == [Mysql::Time.new(2000,1,23), Mysql::Time.new, Mysql::Time.new, Mysql::Time.new(2009,12,8,10,4,46)]
    end

    it '(invalid) raises error' do
      proc{@s.bind_result(Time, nil, nil, nil)}.should raise_error(TypeError)
    end

    it 'mismatch argument count' do
      proc{@s.bind_result(nil)}.should raise_error(Mysql::Error, 'bind_result: result value count(4) != number of argument(1)')
    end
  end

  it '#data_seek set position of current record' do
    @m.query 'create temporary table t (i int)'
    @m.query 'insert into t values (0),(1),(2),(3),(4),(5),(6)'
    @s.prepare 'select i from t'
    @s.execute
    @s.fetch.should == [0]
    @s.fetch.should == [1]
    @s.fetch.should == [2]
    @s.data_seek 5
    @s.fetch.should == [5]
    @s.data_seek 1
    @s.fetch.should == [1]
  end

  it '#execute with an argument' do
    @m.query 'create temporary table t (i int)'
    @s.prepare 'insert into t values (?)'
    @s.execute 123
    @s.execute '456'
    @m.query('select * from t').to_a.should == [['123'], ['456']]
  end

  it '#execute with various arguments' do
    @m.query 'create temporary table t (i int, c char(255), t timestamp)'
    @s.prepare 'insert into t values (?,?,?)'
    @s.execute 123, 'hoge', Time.local(2009,12,8,19,56,21)
    @m.query('select * from t').fetch_row.should == ['123', 'hoge', '2009-12-08 19:56:21']
  end

  it '#execute with arguments that is invalid count' do
    @s.prepare 'select ?'
    proc{@s.execute 123, 456}.should raise_error(Mysql::Error, 'parameter count mismatch')
  end

  it '#execute with huge value' do
    [30, 31, 32, 62, 63].each do |i|
      @m.prepare('select cast(? as signed)').execute(2**i-1).fetch.should == [2**i-1]
      @m.prepare('select cast(? as signed)').execute(-(2**i)).fetch.should == [-2**i]
    end
  end

  it '#fetch returns result-record' do
    @s.prepare 'select 123, "abc", null'
    @s.execute
    @s.fetch.should == [123, 'abc', nil]
  end

  it '#fetch bit column (8bit)' do
    @m.query 'create temporary table t (i bit(8))'
    @m.query 'insert into t values (0),(-1),(127),(-128),(255),(-255),(256)'
    @s.prepare 'select i from t'
    @s.execute
    @s.entries.should == [["\x00"], ["\xff"], ["\x7f"], ["\xff"], ["\xff"], ["\xff"], ["\xff"]]
  end

  it '#fetch bit column (64bit)' do
    @m.query 'create temporary table t (i bit(64))'
    @m.query 'insert into t values (0),(-1),(4294967296),(18446744073709551615),(18446744073709551616)'
    @s.prepare 'select i from t'
    @s.execute
    @s.entries.should == [
      ["\x00\x00\x00\x00\x00\x00\x00\x00"],
      ["\xff\xff\xff\xff\xff\xff\xff\xff"],
      ["\x00\x00\x00\x01\x00\x00\x00\x00"],
      ["\xff\xff\xff\xff\xff\xff\xff\xff"],
      ["\xff\xff\xff\xff\xff\xff\xff\xff"],
    ]
  end

  it '#fetch tinyint column' do
    @m.query 'create temporary table t (i tinyint)'
    @m.query 'insert into t values (0),(-1),(127),(-128),(255),(-255)'
    @s.prepare 'select i from t'
    @s.execute
    @s.entries.should == [[0], [-1], [127], [-128], [127], [-128]]
  end

  it '#fetch tinyint unsigned column' do
    @m.query 'create temporary table t (i tinyint unsigned)'
    @m.query 'insert into t values (0),(-1),(127),(-128),(255),(-255),(256)'
    @s.prepare 'select i from t'
    @s.execute
    @s.entries.should == [[0], [0], [127], [0], [255], [0], [255]]
  end

  it '#fetch smallint column' do
    @m.query 'create temporary table t (i smallint)'
    @m.query 'insert into t values (0),(-1),(32767),(-32768),(65535),(-65535),(65536)'
    @s.prepare 'select i from t'
    @s.execute
    @s.entries.should == [[0], [-1], [32767], [-32768], [32767], [-32768], [32767]]
  end

  it '#fetch smallint unsigned column' do
    @m.query 'create temporary table t (i smallint unsigned)'
    @m.query 'insert into t values (0),(-1),(32767),(-32768),(65535),(-65535),(65536)'
    @s.prepare 'select i from t'
    @s.execute
    @s.entries.should == [[0], [0], [32767], [0], [65535], [0], [65535]]
  end

  it '#fetch mediumint column' do
    @m.query 'create temporary table t (i mediumint)'
    @m.query 'insert into t values (0),(-1),(8388607),(-8388608),(16777215),(-16777215),(16777216)'
    @s.prepare 'select i from t'
    @s.execute
    @s.entries.should == [[0], [-1], [8388607], [-8388608], [8388607], [-8388608], [8388607]]
  end

  it '#fetch mediumint unsigned column' do
    @m.query 'create temporary table t (i mediumint unsigned)'
    @m.query 'insert into t values (0),(-1),(8388607),(-8388608),(16777215),(-16777215),(16777216)'
    @s.prepare 'select i from t'
    @s.execute
    @s.entries.should == [[0], [0], [8388607], [0], [16777215], [0], [16777215]]
  end

  it '#fetch int column' do
    @m.query 'create temporary table t (i int)'
    @m.query 'insert into t values (0),(-1),(2147483647),(-2147483648),(4294967295),(-4294967295),(4294967296)'
    @s.prepare 'select i from t'
    @s.execute
    @s.entries.should == [[0], [-1], [2147483647], [-2147483648], [2147483647], [-2147483648], [2147483647]]
  end

  it '#fetch int unsigned column' do
    @m.query 'create temporary table t (i int unsigned)'
    @m.query 'insert into t values (0),(-1),(2147483647),(-2147483648),(4294967295),(-4294967295),(4294967296)'
    @s.prepare 'select i from t'
    @s.execute
    @s.entries.should == [[0], [0], [2147483647], [0], [4294967295], [0], [4294967295]]
  end

  it '#fetch bigint column' do
    @m.query 'create temporary table t (i bigint)'
    @m.query 'insert into t values (0),(-1),(9223372036854775807),(-9223372036854775808),(18446744073709551615),(-18446744073709551615),(18446744073709551616)'
    @s.prepare 'select i from t'
    @s.execute
    @s.entries.should == [[0], [-1], [9223372036854775807], [-9223372036854775808], [9223372036854775807], [-9223372036854775808], [9223372036854775807]]
  end

  it '#fetch bigint unsigned column' do
    @m.query 'create temporary table t (i bigint unsigned)'
    @m.query 'insert into t values (0),(-1),(9223372036854775807),(-9223372036854775808),(18446744073709551615),(-18446744073709551615),(18446744073709551616)'
    @s.prepare 'select i from t'
    @s.execute
    @s.entries.should == [[0], [0], [9223372036854775807], [0], [18446744073709551615], [0], [18446744073709551615]]
  end

  it '#fetch float column' do
    @m.query 'create temporary table t (i float)'
    @m.query 'insert into t values (0),(-3.402823466E+38),(-1.175494351E-38),(1.175494351E-38),(3.402823466E+38)'
    @s.prepare 'select i from t'
    @s.execute
    @s.fetch[0].should == 0.0
    (@s.fetch[0] - -3.402823466E+38).abs.should < 0.000000001E+38
    (@s.fetch[0] - -1.175494351E-38).abs.should < 0.000000001E-38
    (@s.fetch[0] -  1.175494351E-38).abs.should < 0.000000001E-38
    (@s.fetch[0] -  3.402823466E+38).abs.should < 0.000000001E+38
  end

  it '#fetch float unsigned column' do
    @m.query 'create temporary table t (i float unsigned)'
    @m.query 'insert into t values (0),(-3.402823466E+38),(-1.175494351E-38),(1.175494351E-38),(3.402823466E+38)'
    @s.prepare 'select i from t'
    @s.execute
    @s.fetch[0].should == 0.0
    @s.fetch[0].should == 0.0
    @s.fetch[0].should == 0.0
    (@s.fetch[0] -  1.175494351E-38).abs.should < 0.000000001E-38
    (@s.fetch[0] -  3.402823466E+38).abs.should < 0.000000001E+38
  end

  it '#fetch double column' do
    @m.query 'create temporary table t (i double)'
    @m.query 'insert into t values (0),(-1.7976931348623157E+308),(-2.2250738585072014E-308),(2.2250738585072014E-308),(1.7976931348623157E+308)'
    @s.prepare 'select i from t'
    @s.execute
    @s.fetch[0].should == 0.0
    (@s.fetch[0] - -Float::MAX).abs.should < Float::EPSILON
    (@s.fetch[0] - -Float::MIN).abs.should < Float::EPSILON
    (@s.fetch[0] -  Float::MIN).abs.should < Float::EPSILON
    (@s.fetch[0] -  Float::MAX).abs.should < Float::EPSILON
  end

  it '#fetch double unsigned column' do
    @m.query 'create temporary table t (i double unsigned)'
    @m.query 'insert into t values (0),(-1.7976931348623157E+308),(-2.2250738585072014E-308),(2.2250738585072014E-308),(1.7976931348623157E+308)'
    @s.prepare 'select i from t'
    @s.execute
    @s.fetch[0].should == 0.0
    @s.fetch[0].should == 0.0
    @s.fetch[0].should == 0.0
    (@s.fetch[0] - Float::MIN).abs.should < Float::EPSILON
    (@s.fetch[0] - Float::MAX).abs.should < Float::EPSILON
  end

  it '#fetch decimal column' do
    @m.query 'create temporary table t (i decimal)'
    @m.query 'insert into t values (0),(9999999999),(-9999999999),(10000000000),(-10000000000)'
    @s.prepare 'select i from t'
    @s.execute
    @s.entries.should == [["0"], ["9999999999"], ["-9999999999"], ["9999999999"], ["-9999999999"]]
  end

  it '#fetch decimal unsigned column' do
    @m.query 'create temporary table t (i decimal unsigned)'
    @m.query 'insert into t values (0),(9999999998),(9999999999),(-9999999998),(-9999999999),(10000000000),(-10000000000)'
    @s.prepare 'select i from t'
    @s.execute
    @s.entries.should == [["0"], ["9999999998"], ["9999999999"], ["0"], ["0"], ["9999999999"], ["0"]]
  end

  it '#fetch date column' do
    @m.query 'create temporary table t (i date)'
    @m.query "insert into t values ('0000-00-00'),('1000-01-01'),('9999-12-31')"
    @s.prepare 'select i from t'
    @s.execute
    @s.fetch.should == [Mysql::Time.new]
    @s.fetch.should == [Mysql::Time.new(1000,1,1)]
    @s.fetch.should == [Mysql::Time.new(9999,12,31)]
  end

  it '#fetch datetime column' do
    @m.query 'create temporary table t (i datetime)'
    @m.query "insert into t values ('0000-00-00 00:00:00'),('1000-01-01 00:00:00'),('9999-12-31 23:59:59')"
    @s.prepare 'select i from t'
    @s.execute
    @s.fetch.should == [Mysql::Time.new]
    @s.fetch.should == [Mysql::Time.new(1000,1,1)]
    @s.fetch.should == [Mysql::Time.new(9999,12,31,23,59,59)]
  end

  it '#fetch timestamp column' do
    @m.query 'create temporary table t (i timestamp)'
    @m.query("insert into t values ('1970-01-02 00:00:00'),('2037-12-30 23:59:59')")
    @s.prepare 'select i from t'
    @s.execute
    @s.fetch.should == [Mysql::Time.new(1970,1,2)]
    @s.fetch.should == [Mysql::Time.new(2037,12,30,23,59,59)]
  end

  it '#fetch time column' do
    @m.query 'create temporary table t (i time)'
    @m.query "insert into t values ('-838:59:59'),(0),('838:59:59')"
    @s.prepare 'select i from t'
    @s.execute
    @s.fetch.should == [Mysql::Time.new(0,0,0,838,59,59,true)]
    @s.fetch.should == [Mysql::Time.new(0,0,0,0,0,0,false)]
    @s.fetch.should == [Mysql::Time.new(0,0,0,838,59,59,false)]
  end

  it '#fetch year column' do
    @m.query 'create temporary table t (i year)'
    @m.query 'insert into t values (0),(70),(69),(1901),(2155)'
    @s.prepare 'select i from t'
    @s.execute
    @s.entries.should == [[0], [1970], [2069], [1901], [2155]]
  end

  it '#fetch char column' do
    @m.query 'create temporary table t (i char(10))'
    @m.query "insert into t values (null),('abc')"
    @s.prepare 'select i from t'
    @s.execute
    @s.entries.should == [[nil], ['abc']]
  end

  it '#fetch varchar column' do
    @m.query 'create temporary table t (i varchar(10))'
    @m.query "insert into t values (null),('abc')"
    @s.prepare 'select i from t'
    @s.execute
    @s.entries.should == [[nil], ['abc']]
  end

  it '#fetch binary column' do
    @m.query 'create temporary table t (i binary(10))'
    @m.query "insert into t values (null),('abc')"
    @s.prepare 'select i from t'
    @s.execute
    @s.entries.should == [[nil], ["abc\0\0\0\0\0\0\0"]]
  end

  it '#fetch varbinary column' do
    @m.query 'create temporary table t (i varbinary(10))'
    @m.query "insert into t values (null),('abc')"
    @s.prepare 'select i from t'
    @s.execute
    @s.entries.should == [[nil], ["abc"]]
  end

  it '#fetch tinyblob column' do
    @m.query 'create temporary table t (i tinyblob)'
    @m.query "insert into t values (null),('abc')"
    @s.prepare 'select i from t'
    @s.execute
    @s.entries.should == [[nil], ["abc"]]
  end

  it '#fetch tinytext column' do
    @m.query 'create temporary table t (i tinytext)'
    @m.query "insert into t values (null),('abc')"
    @s.prepare 'select i from t'
    @s.execute
    @s.entries.should == [[nil], ["abc"]]
  end

  it '#fetch blob column' do
    @m.query 'create temporary table t (i blob)'
    @m.query "insert into t values (null),('abc')"
    @s.prepare 'select i from t'
    @s.execute
    @s.entries.should == [[nil], ["abc"]]
  end

  it '#fetch text column' do
    @m.query 'create temporary table t (i text)'
    @m.query "insert into t values (null),('abc')"
    @s.prepare 'select i from t'
    @s.execute
    @s.entries.should == [[nil], ["abc"]]
  end

  it '#fetch mediumblob column' do
    @m.query 'create temporary table t (i mediumblob)'
    @m.query "insert into t values (null),('abc')"
    @s.prepare 'select i from t'
    @s.execute
    @s.entries.should == [[nil], ["abc"]]
  end

  it '#fetch mediumtext column' do
    @m.query 'create temporary table t (i mediumtext)'
    @m.query "insert into t values (null),('abc')"
    @s.prepare 'select i from t'
    @s.execute
    @s.entries.should == [[nil], ["abc"]]
  end

  it '#fetch longblob column' do
    @m.query 'create temporary table t (i longblob)'
    @m.query "insert into t values (null),('abc')"
    @s.prepare 'select i from t'
    @s.execute
    @s.entries.should == [[nil], ["abc"]]
  end

  it '#fetch longtext column' do
    @m.query 'create temporary table t (i longtext)'
    @m.query "insert into t values (null),('abc')"
    @s.prepare 'select i from t'
    @s.execute
    @s.entries.should == [[nil], ["abc"]]
  end

  it '#fetch enum column' do
    @m.query "create temporary table t (i enum('abc','def'))"
    @m.query "insert into t values (null),(0),(1),(2),('abc'),('def'),('ghi')"
    @s.prepare 'select i from t'
    @s.execute
    @s.entries.should == [[nil], [''], ['abc'], ['def'], ['abc'], ['def'], ['']]
  end

  it '#fetch set column' do
    @m.query "create temporary table t (i set('abc','def'))"
    @m.query "insert into t values (null),(0),(1),(2),(3),('abc'),('def'),('abc,def'),('ghi')"
    @s.prepare 'select i from t'
    @s.execute
    @s.entries.should == [[nil], [''], ['abc'], ['def'], ['abc,def'], ['abc'], ['def'], ['abc,def'], ['']]
  end

  it '#each' do
    @m.query 'create temporary table t (i int, c char(255), d datetime)'
    @m.query "insert into t values (1,'abc','19701224235905'),(2,'def','21120903123456'),(3,'123',null)"
    @s.prepare 'select * from t'
    @s.execute
    expect = [
      [1, 'abc', Mysql::Time.new(1970,12,24,23,59,05)],
      [2, 'def', Mysql::Time.new(2112,9,3,12,34,56)],
      [3, '123', nil],
    ]
    @s.each do |a|
      a.should == expect.shift
    end
  end

  it '#field_count' do
    @s.prepare 'select 1,2,3'
    @s.field_count.should == 3
    @s.prepare 'set @a=1'
    @s.field_count.should == 0
  end

  it '#free_result' do
    @s.free_result
    @s.prepare 'select 1,2,3'
    @s.execute
    @s.free_result
  end

  it '#insert_id' do
    @m.query 'create temporary table t (i int auto_increment, unique(i))'
    @s.prepare 'insert into t values (0)'
    @s.execute
    @s.insert_id.should == 1
    @s.execute
    @s.insert_id.should == 2
  end

  it '#num_rows' do
    @m.query 'create temporary table t (i int)'
    @m.query 'insert into t values (1),(2),(3),(4)'
    @s.prepare 'select * from t'
    @s.execute
    @s.num_rows.should == 4
  end

  it '#param_count' do
    @m.query 'create temporary table t (a int, b int, c int)'
    @s.prepare 'select * from t'
    @s.param_count.should == 0
    @s.prepare 'insert into t values (?,?,?)'
    @s.param_count.should == 3
  end

  it '#prepare' do
    @s.prepare('select 1').should be_kind_of Mysql::Stmt
    proc{@s.prepare 'invalid syntax'}.should raise_error Mysql::ParseError
  end

  it '#result_metadata' do
    @s.prepare 'select 1 foo, 2 bar'
    f = @s.result_metadata.fetch_fields
    f[0].name.should == 'foo'
    f[1].name.should == 'bar'
  end

  it '#result_metadata forn no data' do
    @s.prepare 'set @a=1'
    @s.result_metadata.should == nil
  end

  it '#row_seek and #row_tell' do
    @m.query 'create temporary table t (i int)'
    @m.query 'insert into t values (0),(1),(2),(3),(4)'
    @s.prepare 'select * from t'
    @s.execute
    row0 = @s.row_tell
    @s.fetch.should == [0]
    @s.fetch.should == [1]
    row2 = @s.row_seek row0
    @s.fetch.should == [0]
    @s.row_seek row2
    @s.fetch.should == [2]
  end

  it '#sqlstate' do
    @s.prepare 'select 1'
    @s.sqlstate.should == '00000'
    proc{@s.prepare 'hogehoge'}.should raise_error Mysql::ParseError
    @s.sqlstate.should == '42000'
  end
end

describe 'Mysql::Time' do
  before do
    @t = Mysql::Time.new
  end

  it '.new with no arguments returns 0' do
    @t.year.should == 0
    @t.month.should == 0
    @t.day.should == 0
    @t.hour.should == 0
    @t.minute.should == 0
    @t.second.should == 0
    @t.neg.should == false
    @t.second_part.should == 0
  end

  it '#year' do
    (@t.year = 2009).should == 2009
    @t.year.should == 2009
  end

  it '#month' do
    (@t.month = 12).should == 12
    @t.month.should == 12
  end

  it '#day' do
    (@t.day = 8).should == 8
    @t.day.should == 8
  end

  it '#hour' do
    (@t.hour = 23).should == 23
    @t.hour.should == 23
  end

  it '#minute' do
    (@t.minute = 35).should == 35
    @t.minute.should == 35
  end

  it '#second' do
    (@t.second = 21).should == 21
    @t.second.should == 21
  end

  it '#to_s' do
    Mysql::Time.new(2009,12,8,23,35,21).to_s.should == '2009-12-08 23:35:21'
  end

  it '#to_i' do
    Mysql::Time.new(2009,12,8,23,35,21).to_i.should == 20091208233521
  end

  it '#eql' do
    t1 = Mysql::Time.new 2009,12,8,23,35,21
    t2 = Mysql::Time.new 2009,12,8,23,35,21
    t1.should == t2
  end
end
