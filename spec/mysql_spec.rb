# -*- coding: utf-8 -*-
require "tempfile"

require "mysql"

# MYSQL_USER must have ALL privilege for MYSQL_DATABASE.* and RELOAD privilege for *.*
MYSQL_SERVER   = ENV['MYSQL_SERVER']
MYSQL_USER     = ENV['MYSQL_USER']
MYSQL_PASSWORD = ENV['MYSQL_PASSWORD']
MYSQL_DATABASE = ENV['MYSQL_DATABASE'] || "test_for_mysql_ruby"
MYSQL_PORT     = ENV['MYSQL_PORT']
MYSQL_SOCKET   = ENV['MYSQL_SOCKET']

describe 'Mysql::VERSION' do
  it 'returns client version' do
    Mysql::VERSION.should == 20908
  end
end

describe 'Mysql.init' do
  it 'returns Mysql object' do
    Mysql.init.should be_kind_of Mysql
  end
end

describe 'Mysql.real_connect' do
  it 'connect to mysqld' do
    @m = Mysql.real_connect(MYSQL_SERVER, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT, MYSQL_SOCKET)
    @m.should be_kind_of Mysql
  end
  it 'flag argument affects' do
    @m = Mysql.real_connect(MYSQL_SERVER, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT, MYSQL_SOCKET, Mysql::CLIENT_FOUND_ROWS)
    @m.query 'create temporary table t (c int)'
    @m.query 'insert into t values (123)'
    @m.query 'update t set c=123'
    @m.affected_rows.should == 1
  end
  after do
    @m.close
  end
end

describe 'Mysql.connect' do
  it 'connect to mysqld' do
    @m = Mysql.connect(MYSQL_SERVER, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT, MYSQL_SOCKET)
    @m.should be_kind_of Mysql
  end
  after do
    @m.close if @m
  end
end

describe 'Mysql.new' do
  it 'connect to mysqld' do
    @m = Mysql.new(MYSQL_SERVER, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT, MYSQL_SOCKET)
    @m.should be_kind_of Mysql
  end
  after do
    @m.close if @m
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

describe 'Mysql.client_info' do
  it 'returns client version as string' do
    Mysql.client_info.should == '5.0.0'
  end
end

describe 'Mysql.get_client_info' do
  it 'returns client version as string' do
    Mysql.get_client_info.should == '5.0.0'
  end
end

describe 'Mysql.client_version' do
  it 'returns client version as Integer' do
    Mysql.client_version.should == 50000
  end
end

describe 'Mysql.get_client_version' do
  it 'returns client version as Integer' do
    Mysql.client_version.should == 50000
  end
end

describe 'Mysql#real_connect' do
  it 'connect to mysqld' do
    @m = Mysql.init
    @m.real_connect(MYSQL_SERVER, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT, MYSQL_SOCKET).should == @m
  end
  after do
    @m.close if @m
  end
end

describe 'Mysql#connect' do
  it 'connect to mysqld' do
    @m = Mysql.init
    @m.connect(MYSQL_SERVER, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT, MYSQL_SOCKET).should == @m
  end
  after do
    @m.close if @m
  end
end

describe 'Mysql#options' do
  before do
    @m = Mysql.init
  end
  after do
    @m.close
  end
  it 'INIT_COMMAND: execute query when connecting' do
    @m.options(Mysql::INIT_COMMAND, "SET AUTOCOMMIT=0").should == @m
    @m.connect(MYSQL_SERVER, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT, MYSQL_SOCKET).should == @m
    @m.query('select @@AUTOCOMMIT').fetch_row.should == ["0"]
  end
  it 'OPT_CONNECT_TIMEOUT: set timeout for connecting' do
    @m.options(Mysql::OPT_CONNECT_TIMEOUT, 0.1).should == @m
    UNIXSocket.stub!(:new).and_return{sleep 1}
    TCPSocket.stub!(:new).and_return{sleep 1}
    proc{@m.connect(MYSQL_SERVER, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT, MYSQL_SOCKET)}.should raise_error Mysql::ClientError, 'connection timeout'
    proc{@m.connect}.should raise_error Mysql::ClientError, 'connection timeout'
  end
  it 'OPT_LOCAL_INFILE: client can execute LOAD DATA LOCAL INFILE query' do
    tmpf = Tempfile.new 'mysql_spec'
    tmpf.puts "123\tabc\n"
    tmpf.close
    @m.options(Mysql::OPT_LOCAL_INFILE, true).should == @m
    @m.connect(MYSQL_SERVER, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT, MYSQL_SOCKET)
    @m.query('create temporary table t (i int, c char(10))')
    @m.query("load data local infile '#{tmpf.path}' into table t")
    @m.query('select * from t').fetch_row.should == ['123','abc']
  end
  it 'OPT_READ_TIMEOUT: set timeout for reading packet' do
    @m.options(Mysql::OPT_READ_TIMEOUT, 10).should == @m
  end
  it 'OPT_WRITE_TIMEOUT: set timeout for writing packet' do
    @m.options(Mysql::OPT_WRITE_TIMEOUT, 10).should == @m
  end
  it 'SET_CHARSET_NAME: set charset for connection' do
    @m.options(Mysql::SET_CHARSET_NAME, 'utf8').should == @m
    @m.connect(MYSQL_SERVER, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT, MYSQL_SOCKET)
    @m.query('select @@character_set_connection').fetch_row.should == ['utf8']
  end
end

describe 'Mysql' do
  before do
    @m = Mysql.new(MYSQL_SERVER, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT, MYSQL_SOCKET)
  end

  after do
    @m.close if @m rescue nil
  end

  describe '#escape_string' do
    if defined? ::Encoding
      it 'escape special character for charset' do
        @m.charset = 'cp932'
        @m.escape_string("abc'def\"ghi\0jkl%mno_表".encode('cp932')).should == "abc\\'def\\\"ghi\\0jkl%mno_表".encode('cp932')
      end
    else
      it 'raise error if charset is multibyte' do
        @m.charset = 'cp932'
        proc{@m.escape_string("abc'def\"ghi\0jkl%mno_\x95\\")}.should raise_error(Mysql::ClientError, 'Mysql#escape_string is called for unsafe multibyte charset')
      end
      it 'not warn if charset is singlebyte' do
        @m.charset = 'latin1'
        @m.escape_string("abc'def\"ghi\0jkl%mno_\x95\\").should == "abc\\'def\\\"ghi\\0jkl%mno_\x95\\\\"
      end
    end
  end

  describe '#quote' do
    it 'is alias of #escape_string' do
      @m.method(:quote).should == @m.method(:escape_string)
    end
  end

  describe '#client_info' do
    it 'returns client version as string' do
      @m.client_info.should == '5.0.0'
    end
  end

  describe '#get_client_info' do
    it 'returns client version as string' do
      @m.get_client_info.should == '5.0.0'
    end
  end

  describe '#affected_rows' do
    it 'returns number of affected rows' do
      @m.query 'create temporary table t (id int)'
      @m.query 'insert into t values (1),(2)'
      @m.affected_rows.should == 2
    end
  end

  describe '#character_set_name' do
    it 'returns charset name' do
      m = Mysql.init
      m.options Mysql::SET_CHARSET_NAME, 'cp932'
      m.connect MYSQL_SERVER, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT, MYSQL_SOCKET
      m.character_set_name.should == 'cp932'
    end
  end

  describe '#close' do
    it 'returns self' do
      @m.close.should == @m
    end
  end

  describe '#close!' do
    it 'returns self' do
      @m.close!.should == @m
    end
  end

#  describe '#create_db' do
#  end

#  describe '#drop_db' do
#  end

  describe '#errno' do
    it 'default value is 0' do
      @m.errno.should == 0
    end
    it 'returns error number of latest error' do
      @m.query('hogehoge') rescue nil
      @m.errno.should == 1064
    end
  end

  describe '#error' do
    it 'returns error message of latest error' do
      @m.query('hogehoge') rescue nil
      @m.error.should == "You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near 'hogehoge' at line 1"
    end
  end

  describe '#field_count' do
    it 'returns number of fields for latest query' do
      @m.query 'select 1,2,3'
      @m.field_count.should == 3
    end
  end

  describe '#client_version' do
    it 'returns client version as Integer' do
      @m.client_version.should be_kind_of Integer
    end
  end

  describe '#get_client_version' do
    it 'returns client version as Integer' do
      @m.get_client_version.should be_kind_of Integer
    end
  end

  describe '#get_host_info' do
    it 'returns connection type as String' do
      if MYSQL_SERVER == nil or MYSQL_SERVER == 'localhost'
        @m.get_host_info.should == 'Localhost via UNIX socket'
      else
        @m.get_host_info.should == "#{MYSQL_SERVER} via TCP/IP"
      end
    end
  end

  describe '#host_info' do
    it 'returns connection type as String' do
      if MYSQL_SERVER == nil or MYSQL_SERVER == 'localhost'
        @m.host_info.should == 'Localhost via UNIX socket'
      else
        @m.host_info.should == "#{MYSQL_SERVER} via TCP/IP"
      end
    end
  end

  describe '#get_proto_info' do
    it 'returns version of connection as Integer' do
      @m.get_proto_info.should == 10
    end
  end

  describe '#proto_info' do
    it 'returns version of connection as Integer' do
      @m.proto_info.should == 10
    end
  end

  describe '#get_server_info' do
    it 'returns server version as String' do
      @m.get_server_info.should =~ /\A\d+\.\d+\.\d+/
    end
  end

  describe '#server_info' do
    it 'returns server version as String' do
      @m.server_info.should =~ /\A\d+\.\d+\.\d+/
    end
  end

  describe '#info' do
    it 'returns information of latest query' do
      @m.query 'create temporary table t (id int)'
      @m.query 'insert into t values (1),(2),(3)'
      @m.info.should == 'Records: 3  Duplicates: 0  Warnings: 0'
    end
  end

  describe '#insert_id' do
    it 'returns latest auto_increment value' do
      @m.query 'create temporary table t (id int auto_increment, unique (id))'
      @m.query 'insert into t values (0)'
      @m.insert_id.should == 1
      @m.query 'alter table t auto_increment=1234'
      @m.query 'insert into t values (0)'
      @m.insert_id.should == 1234
    end
  end

  describe '#kill' do
    it 'returns self' do
      @m.kill(@m.thread_id).should == @m
    end
    it 'kill specified connection' do
      m = Mysql.new(MYSQL_SERVER, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT, MYSQL_SOCKET)
      m.list_processes.map(&:first).should be_include @m.thread_id.to_s
      m.close
    end
  end

  describe '#list_dbs' do
    it 'returns database list' do
      ret = @m.list_dbs
      ret.should be_kind_of Array
      ret.should be_include MYSQL_DATABASE
    end
    it 'with pattern returns databases that matches pattern' do
      @m.list_dbs('info%').should be_include 'information_schema'
    end
  end

  describe '#list_fields' do
    before do
      @m.query 'create temporary table t (i int, c char(10), d date)'
    end
    it 'returns result set that contains information of fields' do
      ret = @m.list_fields('t')
      ret.should be_kind_of Mysql::Result
      ret.num_rows.should == 0
      ret.fetch_fields.map{|f|f.name}.should == ['i','c','d']
    end
    it 'with pattern returns result set that contains information of fields that matches pattern' do
      ret = @m.list_fields('t', 'i')
      ret.should be_kind_of Mysql::Result
      ret.num_rows.should == 0
      ret.fetch_fields.map{|f|f.name}.should == ['i']
    end
  end

  describe '#list_processes' do
    it 'returns result set that contains information of all connections' do
      ret = @m.list_processes
      ret.should be_kind_of Mysql::Result
      ret.find{|r|r[0].to_i == @m.thread_id}[4].should == "Processlist"
    end
  end

  describe '#list_tables' do
    before do
      @m.query 'create table test_mysql_list_tables (id int)'
    end
    after do
      @m.query 'drop table test_mysql_list_tables'
    end
    it 'returns table list' do
      ret = @m.list_tables
      ret.should be_kind_of Array
      ret.should be_include 'test_mysql_list_tables'
    end
    it 'with pattern returns lists that matches pattern' do
      ret = @m.list_tables '%mysql\_list\_t%'
      ret.should be_include 'test_mysql_list_tables'
    end
  end

  describe '#ping' do
    it 'returns self' do
      @m.ping.should == @m
    end
  end

  describe '#query' do
    it 'returns Mysql::Result if query returns results' do
      @m.query('select 123').should be_kind_of Mysql::Result
    end
    it 'returns nil if query returns no results' do
      @m.query('set @hoge:=123').should == nil
    end
    it 'returns self if query_with_result is false' do
      @m.query_with_result = false
      @m.query('select 123').should == @m
      @m.store_result
      @m.query('set @hoge:=123').should == @m
    end
  end

  describe '#real_query' do
    it 'is same as #query' do
      @m.real_query('select 123').should be_kind_of Mysql::Result
    end
  end

  describe '#refresh' do
    it 'returns self' do
      @m.refresh(Mysql::REFRESH_HOSTS).should == @m
    end
  end

  describe '#reload' do
    it 'returns self' do
      @m.reload.should == @m
    end
  end

  describe '#select_db' do
    it 'changes default database' do
      @m.select_db 'information_schema'
      @m.query('select database()').fetch_row.first.should == 'information_schema'
    end
  end

#  describe '#shutdown' do
#  end

  describe '#stat' do
    it 'returns server status' do
      @m.stat.should =~ /\AUptime: \d+  Threads: \d+  Questions: \d+  Slow queries: \d+  Opens: \d+  Flush tables: \d+  Open tables: \d+  Queries per second avg: \d+\.\d+\z/
    end
  end

  describe '#store_result' do
    it 'returns Mysql::Result' do
      @m.query_with_result = false
      @m.query 'select 1,2,3'
      ret = @m.store_result
      ret.should be_kind_of Mysql::Result
      ret.fetch_row.should == ['1','2','3']
    end
    it 'raises error when no query' do
      proc{@m.store_result}.should raise_error Mysql::Error
    end
    it 'raises error when query does not return results' do
      @m.query 'set @hoge:=123'
      proc{@m.store_result}.should raise_error Mysql::Error
    end
  end

  describe '#thread_id' do
    it 'returns thread id as Integer' do
      @m.thread_id.should be_kind_of Integer
    end
  end

  describe '#use_result' do
    it 'returns Mysql::Result' do
      @m.query_with_result = false
      @m.query 'select 1,2,3'
      ret = @m.use_result
      ret.should be_kind_of Mysql::Result
      ret.fetch_row.should == ['1','2','3']
    end
    it 'raises error when no query' do
      proc{@m.use_result}.should raise_error Mysql::Error
    end
    it 'raises error when query does not return results' do
      @m.query 'set @hoge:=123'
      proc{@m.use_result}.should raise_error Mysql::Error
    end
  end

  describe '#get_server_version' do
    it 'returns server version as Integer' do
      @m.get_server_version.should be_kind_of Integer
    end
  end

  describe '#server_version' do
    it 'returns server version as Integer' do
      @m.server_version.should be_kind_of Integer
    end
  end

  describe '#warning_count' do
    it 'default values is zero' do
      @m.warning_count.should == 0
    end
    it 'returns number of warnings' do
      @m.query 'create temporary table t (i tinyint)'
      @m.query 'insert into t values (1234567)'
      @m.warning_count.should == 1
    end
  end

  describe '#commit' do
    it 'returns self' do
      @m.commit.should == @m
    end
  end

  describe '#rollback' do
    it 'returns self' do
      @m.rollback.should == @m
    end
  end

  describe '#autocommit' do
    it 'returns self' do
      @m.autocommit(true).should == @m
    end

    it 'change auto-commit mode' do
      @m.autocommit(true)
      @m.query('select @@autocommit').fetch_row.should == ['1']
      @m.autocommit(false)
      @m.query('select @@autocommit').fetch_row.should == ['0']
    end
  end

  describe '#set_server_option' do
    it 'returns self' do
      @m.set_server_option(Mysql::OPTION_MULTI_STATEMENTS_ON).should == @m
    end
  end

  describe '#sqlstate' do
    it 'default values is "00000"' do
      @m.sqlstate.should == "00000"
    end
    it 'returns sqlstate code' do
      proc{@m.query("hoge")}.should raise_error
      @m.sqlstate.should == "42000"
    end
  end

  describe '#query_with_result' do
    it 'default value is true' do
      @m.query_with_result.should == true
    end
    it 'can set value' do
      (@m.query_with_result=true).should == true
      @m.query_with_result.should == true
      (@m.query_with_result=false).should == false
      @m.query_with_result.should == false
    end
  end

  describe '#query_with_result is false' do
    it 'Mysql#query returns self and Mysql#store_result returns result set' do
      @m.query_with_result = false
      @m.query('select 1,2,3').should == @m
      res = @m.store_result
      res.fetch_row.should == ['1','2','3']
    end
  end

  describe '#query with block' do
    it 'returns self' do
      @m.query('select 1'){}.should == @m
    end
    it 'evaluate block with Mysql::Result' do
      @m.query('select 1'){|res| res.should be_kind_of Mysql::Result}.should == @m
    end
    it 'evaluate block multiple times if multiple query is specified' do
      @m.set_server_option Mysql::OPTION_MULTI_STATEMENTS_ON
      cnt = 0
      expect = [["1"], ["2"]]
      @m.query('select 1; select 2'){|res|
        res.fetch_row.should == expect.shift
        cnt += 1
      }.should == @m
      cnt.should == 2
    end
    it 'evaluate block only when query has result' do
      @m.set_server_option Mysql::OPTION_MULTI_STATEMENTS_ON
      cnt = 0
      expect = [["1"], ["2"]]
      @m.query('select 1; set @hoge:=1; select 2'){|res|
        res.fetch_row.should == expect.shift
        cnt += 1
      }.should == @m
      cnt.should == 2
    end
  end
end

describe 'multiple statement query:' do
  before :all do
    @m = Mysql.new(MYSQL_SERVER, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT, MYSQL_SOCKET)
    @m.set_server_option(Mysql::OPTION_MULTI_STATEMENTS_ON)
    @res = @m.query 'select 1,2; select 3,4,5'
  end
  it 'Mysql#query returns results for first query' do
    @res.entries.should == [['1','2']]
  end
  it 'Mysql#more_results is true' do
    @m.more_results.should == true
  end
  it 'Mysql#more_results? is true' do
    @m.more_results?.should == true
  end
  it 'Mysql#next_result is true' do
    @m.next_result.should == true
  end
  it 'Mysql#store_result returns results for next query' do
    res = @m.store_result
    res.entries.should == [['3','4','5']]
  end
  it 'Mysql#more_results is false' do
    @m.more_results.should == false
  end
  it 'Mysql#more_results? is false' do
    @m.more_results?.should == false
  end
  it 'Mysql#next_result is false' do
    @m.next_result.should == false
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

  it '#data_seek set position of current record' do
    @res.fetch_row.should == ['1', 'abc']
    @res.fetch_row.should == ['2', 'defg']
    @res.fetch_row.should == ['3', 'hi']
    @res.data_seek 1
    @res.fetch_row.should == ['2', 'defg']
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
    proc{@res.fetch_field_direct(-1)}.should raise_error Mysql::ClientError, 'invalid argument: -1'
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

  it '#num_fields returns number of fields' do
    @res.num_fields.should == 2
  end

  it '#num_rows returns number of records' do
    @res.num_rows.should == 4
  end

  it '#each iterate block with a record' do
    expect = [["1","abc"], ["2","defg"], ["3","hi"], ["4",nil]]
    @res.each do |a|
      a.should == expect.shift
    end
  end

  it '#each_hash iterate block with a hash' do
    expect = [{"id"=>"1","str"=>"abc"}, {"id"=>"2","str"=>"defg"}, {"id"=>"3","str"=>"hi"}, {"id"=>"4","str"=>nil}]
    @res.each_hash do |a|
      a.should == expect.shift
    end
  end

  it '#each_hash(true): hash key has table name' do
    expect = [{"t.id"=>"1","t.str"=>"abc"}, {"t.id"=>"2","t.str"=>"defg"}, {"t.id"=>"3","t.str"=>"hi"}, {"t.id"=>"4","t.str"=>nil}]
    @res.each_hash(true) do |a|
      a.should == expect.shift
    end
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

  it '#free returns nil' do
    @res.free.should == nil
  end

  it '#num_fields returns number of fields' do
    @res.num_fields.should == 2
  end

  it '#num_rows returns number of records' do
    @res.num_rows.should == 4
  end
end

describe 'Mysql::Field' do
  before do
    @m = Mysql.new(MYSQL_SERVER, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT, MYSQL_SOCKET)
    @m.query 'create temporary table t (id int default 0, str char(10), primary key (id))'
    @m.query "insert into t values (1,'abc'),(2,'defg'),(3,'hi'),(4,null)"
    @res = @m.query 'select * from t'
  end

  after do
    @m.close if @m
  end

  it '#name is name of field' do
    @res.fetch_field.name.should == 'id'
  end

  it '#table is name of table for field' do
    @res.fetch_field.table.should == 't'
  end

  it '#def for result set is null' do
    @res.fetch_field.def.should == nil
  end

  it '#def for field information is default value' do
    @m.list_fields('t').fetch_field.def.should == '0'
  end

  it '#type is type of field as Integer' do
    @res.fetch_field.type.should == Mysql::Field::TYPE_LONG
    @res.fetch_field.type.should == Mysql::Field::TYPE_STRING
  end

  it '#length is length of field' do
    @res.fetch_field.length.should == 11
    @res.fetch_field.length.should == 10
  end

  it '#max_length is maximum length of field value' do
    @res.fetch_field.max_length.should == 1
    @res.fetch_field.max_length.should == 4
  end

  it '#flags is flag of field as Integer' do
    @res.fetch_field.flags.should == Mysql::Field::NUM_FLAG|Mysql::Field::PRI_KEY_FLAG|Mysql::Field::PART_KEY_FLAG|Mysql::Field::NOT_NULL_FLAG
    @res.fetch_field.flags.should == 0
  end

  it '#decimals is number of decimal digits' do
    @m.query('select 1.23').fetch_field.decimals.should == 2
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
    @s.close if @s rescue nil
    @m.close if @m
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

    it 'with mismatch argument count raise error' do
      proc{@s.bind_result(nil)}.should raise_error(Mysql::ClientError, 'bind_result: result value count(4) != number of argument(1)')
    end
  end

  it '#close returns nil' do
    @s.close.should == nil
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

  it '#each iterate block with a record' do
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

  it '#execute returns self' do
    @s.prepare 'select 1'
    @s.execute.should == @s
  end

  it '#execute pass arguments to query' do
    @m.query 'create temporary table t (i int)'
    @s.prepare 'insert into t values (?)'
    @s.execute 123
    @s.execute '456'
    @m.query('select * from t').entries.should == [['123'], ['456']]
  end

  it '#execute with various arguments' do
    @m.query 'create temporary table t (i int, c char(255), t timestamp)'
    @s.prepare 'insert into t values (?,?,?)'
    @s.execute 123, 'hoge', Time.local(2009,12,8,19,56,21)
    @m.query('select * from t').fetch_row.should == ['123', 'hoge', '2009-12-08 19:56:21']
  end

  it '#execute with arguments that is invalid count raise error' do
    @s.prepare 'select ?'
    proc{@s.execute 123, 456}.should raise_error(Mysql::ClientError, 'parameter count mismatch')
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
    if defined? Encoding
      @s.entries.should == [
        ["\x00".force_encoding('ASCII-8BIT')],
        ["\xff".force_encoding('ASCII-8BIT')],
        ["\x7f".force_encoding('ASCII-8BIT')],
        ["\xff".force_encoding('ASCII-8BIT')],
        ["\xff".force_encoding('ASCII-8BIT')],
        ["\xff".force_encoding('ASCII-8BIT')],
        ["\xff".force_encoding('ASCII-8BIT')],
      ]
    else
      @s.entries.should == [["\x00"], ["\xff"], ["\x7f"], ["\xff"], ["\xff"], ["\xff"], ["\xff"]]
    end
  end

  it '#fetch bit column (64bit)' do
    @m.query 'create temporary table t (i bit(64))'
    @m.query 'insert into t values (0),(-1),(4294967296),(18446744073709551615),(18446744073709551616)'
    @s.prepare 'select i from t'
    @s.execute
    if defined? Encoding
      @s.entries.should == [
        ["\x00\x00\x00\x00\x00\x00\x00\x00".force_encoding('ASCII-8BIT')],
        ["\xff\xff\xff\xff\xff\xff\xff\xff".force_encoding('ASCII-8BIT')],
        ["\x00\x00\x00\x01\x00\x00\x00\x00".force_encoding('ASCII-8BIT')],
        ["\xff\xff\xff\xff\xff\xff\xff\xff".force_encoding('ASCII-8BIT')],
        ["\xff\xff\xff\xff\xff\xff\xff\xff".force_encoding('ASCII-8BIT')],
      ]
    else
      @s.entries.should == [
        ["\x00\x00\x00\x00\x00\x00\x00\x00"],
        ["\xff\xff\xff\xff\xff\xff\xff\xff"],
        ["\x00\x00\x00\x01\x00\x00\x00\x00"],
        ["\xff\xff\xff\xff\xff\xff\xff\xff"],
        ["\xff\xff\xff\xff\xff\xff\xff\xff"],
      ]
    end
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
    cols = @s.fetch
    cols.should == [Mysql::Time.new]
    cols.first.to_s.should == '0000-00-00'
    cols = @s.fetch
    cols.should == [Mysql::Time.new(1000,1,1)]
    cols.first.to_s.should == '1000-01-01'
    cols = @s.fetch
    cols.should == [Mysql::Time.new(9999,12,31)]
    cols.first.to_s.should == '9999-12-31'
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

  it '#prepare returns self' do
    @s.prepare('select 1').should == @s
  end

  it '#prepare with invalid query raises error' do
    proc{@s.prepare 'invalid query'}.should raise_error Mysql::ParseError
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

  it '#inspect' do
    Mysql::Time.new(2009,12,8,23,35,21).inspect.should == '#<Mysql::Time:2009-12-08 23:35:21>'
  end

  it '#to_s' do
    Mysql::Time.new(2009,12,8,23,35,21).to_s.should == '2009-12-08 23:35:21'
  end

  it '#to_i' do
    Mysql::Time.new(2009,12,8,23,35,21).to_i.should == 20091208233521
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

  it '#neg' do
    @t.neg.should == false
  end

  it '#second_part' do
    @t.second_part.should == 0
  end

  it '#==' do
    t1 = Mysql::Time.new 2009,12,8,23,35,21
    t2 = Mysql::Time.new 2009,12,8,23,35,21
    t1.should == t2
  end
end

describe 'Mysql::Error' do
  before do
    m = Mysql.connect(MYSQL_SERVER, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT, MYSQL_SOCKET)
    begin
      m.query('hogehoge')
    rescue => @e
    end
  end

  it '#error is error message' do
    @e.error.should == "You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near 'hogehoge' at line 1"
  end

  it '#errno is error number' do
    @e.errno.should == 1064
  end

  it '#sqlstate is sqlstate value as String' do
    @e.sqlstate.should == '42000'
  end
end

if defined? Encoding
  describe 'Connection charset is UTF-8:' do
    before do
      @m = Mysql.connect(MYSQL_SERVER, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT, MYSQL_SOCKET)
      @m.charset = "utf8"
      @m.query "create temporary table t (utf8 char(10) charset utf8, cp932 char(10) charset cp932, eucjp char(10) charset eucjpms, bin varbinary(10))"
      @utf8 = "いろは"
      @cp932 = @utf8.encode "CP932"
      @eucjp = @utf8.encode "EUC-JP-MS"
      @bin = "\x00\x01\x7F\x80\xFE\xFF".force_encoding("ASCII-8BIT")
      @default_internal = Encoding.default_internal
    end

    after do
      Encoding.default_internal = @default_internal
    end

    describe 'default_internal is CP932' do
      before do
        Encoding.default_internal = 'CP932'
      end
      it 'is converted to CP932' do
        @m.query('select "あいう"').fetch.should == ["\x82\xA0\x82\xA2\x82\xA4".force_encoding("CP932")]
      end
    end

    describe 'query with CP932 encoding' do
      it 'is converted to UTF-8' do
        @m.query('select HEX("あいう")'.encode("CP932")).fetch.should == ["E38182E38184E38186"]
      end
    end

    describe 'prepared statement with CP932 encoding' do
      it 'is converted to UTF-8' do
        @m.prepare('select HEX("あいう")'.encode("CP932")).execute.fetch.should == ["E38182E38184E38186"]
      end
    end

    describe 'The encoding of data are correspond to charset of column:' do
      before do
        @m.prepare("insert into t (utf8,cp932,eucjp,bin) values (?,?,?,?)").execute @utf8, @cp932, @eucjp, @bin
      end
      it 'data is stored as is' do
        @m.query('select hex(utf8),hex(cp932),hex(eucjp),hex(bin) from t').fetch.should == ['E38184E3828DE381AF', '82A282EB82CD', 'A4A4A4EDA4CF', '00017F80FEFF']
      end
      it 'By simple query, charset of retrieved data is connection charset' do
        @m.query('select utf8,cp932,eucjp,bin from t').fetch.should == [@utf8, @utf8, @utf8, @bin.dup.force_encoding("UTF-8")]
      end
      it 'By prepared statement, charset of retrieved data is connection charset except for binary' do
        @m.prepare('select utf8,cp932,eucjp,bin from t').execute.fetch.should == [@utf8, @utf8, @utf8, @bin]
      end
    end

    describe 'The encoding of data are different from charset of column:' do
      before do
        @m.prepare("insert into t (utf8,cp932,eucjp,bin) values (?,?,?,?)").execute @utf8, @utf8, @utf8, @utf8
      end
      it 'stored data is converted' do
        @m.query("select hex(utf8),hex(cp932),hex(eucjp),hex(bin) from t").fetch.should == ["E38184E3828DE381AF", "82A282EB82CD", "A4A4A4EDA4CF", "E38184E3828DE381AF"]
      end
      it 'By simple query, charset of retrieved data is connection charset' do
        @m.query("select utf8,cp932,eucjp,bin from t").fetch.should == [@utf8, @utf8, @utf8, @utf8]
      end
      it 'By prepared statement, charset of retrieved data is connection charset except for binary' do
        @m.prepare("select utf8,cp932,eucjp,bin from t").execute.fetch.should == [@utf8, @utf8, @utf8, @utf8.dup.force_encoding("ASCII-8BIT")]
      end
    end

    describe 'The data include invalid byte code:' do
      it 'raises Encoding::InvalidByteSequenceError' do
        cp932 = "\x01\xFF\x80".force_encoding("CP932")
        proc{@m.prepare("insert into t (cp932) values (?)").execute cp932}.should raise_error(Encoding::InvalidByteSequenceError)
      end
    end
  end
end
