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
    it 'change auto-commit mode' do
      @m.autocommit(true).should == @m
      @m.autocommit(false).should == @m
    end
  end

  describe '#more_results, #next_result' do
  end

  describe '#query with block' do
  end

  describe '#set_server_optoin' do
  end

  describe '#sqlstate' do
  end

  describe '#query_with_result' do
  end

  describe '#reconnect' do
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
