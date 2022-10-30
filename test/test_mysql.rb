# -*- coding: utf-8 -*-
require 'test/unit'
require 'test/unit/rr'
begin
  require 'test/unit/notify'
rescue LoadError
  # ignore
end

require 'mysql'

# MYSQL_USER must have ALL privilege for MYSQL_DATABASE.* and RELOAD privilege for *.*
MYSQL_SERVER   = ENV['MYSQL_SERVER']
MYSQL_USER     = ENV['MYSQL_USER']
MYSQL_PASSWORD = ENV['MYSQL_PASSWORD']
MYSQL_DATABASE = ENV['MYSQL_DATABASE'] || "test_for_mysql_ruby"
MYSQL_PORT     = ENV['MYSQL_PORT']
MYSQL_SOCKET   = ENV['MYSQL_SOCKET']

class TestMysql < Test::Unit::TestCase
  sub_test_case 'Mysql::VERSION' do
    test 'returns client version' do
      assert{ Mysql::VERSION == '3.1.0' }
    end
  end

  sub_test_case 'Mysql.new' do
    test 'returns Mysql object' do
      assert{ Mysql.new.kind_of? Mysql }
    end
  end

  sub_test_case 'arguments' do
    test 'with fixed arguments' do
      @m = Mysql.new('127.0.0.1', 'hoge', 'abc&def', 'test', 3306, '/tmp/socket', 12345)
      assert{ @m.host == '127.0.0.1' }
      assert{ @m.username == 'hoge' }
      assert{ @m.password == 'abc&def' }
      assert{ @m.database == 'test' }
      assert{ @m.port == 3306 }
      assert{ @m.socket == '/tmp/socket' }
      assert{ @m.flags == 12345 }
    end

    test 'with keyword arguments' do
      @m = Mysql.new(host: '127.0.0.1', username: 'hoge', password: 'abc&def', database: 'test', port: 3306, socket: '/tmp/socket', flags: 12345)
      assert{ @m.host == '127.0.0.1' }
      assert{ @m.username == 'hoge' }
      assert{ @m.password == 'abc&def' }
      assert{ @m.database == 'test' }
      assert{ @m.port == 3306 }
      assert{ @m.socket == '/tmp/socket' }
      assert{ @m.flags == 12345 }
    end

    test 'with URI' do
      uri = URI.parse("mysql://hoge:abc%26def@127.0.0.1:3306/test?socket=/tmp/socket&flags=12345")
      @m = Mysql.new(uri)
      assert{ @m.host == '127.0.0.1' }
      assert{ @m.username == 'hoge' }
      assert{ @m.password == 'abc&def' }
      assert{ @m.database == 'test' }
      assert{ @m.port == 3306 }
      assert{ @m.socket == '/tmp/socket' }
      assert{ @m.flags == 12345 }
    end

    test 'with URI string' do
      @m = Mysql.new("mysql://hoge:abc%26def@127.0.0.1:3306/test?socket=/tmp/socket&flags=12345")
      assert{ @m.host == '127.0.0.1' }
      assert{ @m.username == 'hoge' }
      assert{ @m.password == 'abc&def' }
      assert{ @m.database == 'test' }
      assert{ @m.port == 3306 }
      assert{ @m.socket == '/tmp/socket' }
      assert{ @m.flags == 12345 }
    end

    test 'with URI string: host is filename' do
      @m = Mysql.new("mysql://hoge:abc%26def@%2Ftmp%2Fsocket:3306/test?flags=12345")
      assert{ @m.host == '' }
      assert{ @m.username == 'hoge' }
      assert{ @m.password == 'abc&def' }
      assert{ @m.database == 'test' }
      assert{ @m.port == 3306 }
      assert{ @m.socket == '/tmp/socket' }
      assert{ @m.flags == 12345 }
    end

    teardown do
      @m.close if @m
    end
  end

  sub_test_case 'Mysql.connect' do
    test 'connect to mysqld' do
      @m = Mysql.connect(MYSQL_SERVER, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT, MYSQL_SOCKET)
      assert{ @m.kind_of? Mysql }
    end

    test 'flag argument affects' do
      @m = Mysql.connect(MYSQL_SERVER, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT, MYSQL_SOCKET, Mysql::CLIENT_FOUND_ROWS)
      @m.query 'create temporary table t (c int)'
      @m.query 'insert into t values (123)'
      @m.query 'update t set c=123'
      assert{ @m.affected_rows == 1 }
    end

    teardown do
      @m.close if @m
    end
  end

  sub_test_case 'Mysql.escape_string' do
    test 'escape special character' do
      assert{ Mysql.escape_string("abc'def\"ghi\0jkl%mno") == "abc\\'def\\\"ghi\\0jkl%mno" }
    end
  end

  sub_test_case 'Mysql.quote' do
    test 'escape special character' do
      assert{ Mysql.quote("abc'def\"ghi\0jkl%mno") == "abc\\'def\\\"ghi\\0jkl%mno" }
    end
  end

  sub_test_case 'Mysql#connect' do
    test 'connect to mysqld' do
      @m = Mysql.new(MYSQL_SERVER, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT, MYSQL_SOCKET)
      assert{ @m.connect == @m }
    end

    test 'connect to mysqld by URI' do
      @m = Mysql.new("mysql://#{MYSQL_USER}:#{MYSQL_PASSWORD}@#{MYSQL_SERVER}:#{MYSQL_PORT}/#{MYSQL_DATABASE}?socket=#{MYSQL_SOCKET}")
      assert{ @m.connect == @m }
    end

    test 'overrides arguments of new method' do
      @m = Mysql.new('example.com', 12345)
      @m.connect(MYSQL_SERVER, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT, MYSQL_SOCKET)
    end

    teardown do
      @m.close if @m
    end
  end

  sub_test_case 'options' do
    setup do
      @m = Mysql.new
    end
    teardown do
      @m.close
    end
    test 'init_command: execute query when connecting' do
      @m.init_command = "SET AUTOCOMMIT=0"
      assert{ @m.connect(MYSQL_SERVER, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT, MYSQL_SOCKET) == @m }
      assert{ @m.query('select @@AUTOCOMMIT').fetch_row == ["0"] }
    end
    test 'connect_timeout: set timeout for connecting' do
      @m.connect_timeout = 0.1
      stub(Socket).tcp{ raise Errno::ETIMEDOUT }
      stub(Socket).unix{ raise Errno::ETIMEDOUT }
      assert_raise Mysql::ClientError, 'connection timeout' do
        @m.connect(MYSQL_SERVER, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT, MYSQL_SOCKET)
      end
      assert_raise Mysql::ClientError, 'connection timeout' do
        @m.connect
      end
    end
    test 'local_infile: client can execute LOAD DATA LOCAL INFILE query' do
      require 'tempfile'
      tmpf = Tempfile.new 'mysql_spec'
      tmpf.puts "123\tabc\n"
      tmpf.close
      @m.local_infile = true
      @m.connect(MYSQL_SERVER, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT, MYSQL_SOCKET)
      if @m.query('select @@local_infile').fetch[0] == '0'
        omit 'skip because local_infile variable is false'
      end
      @m.query('create temporary table t (i int, c char(10))')
      @m.query("load data local infile '#{tmpf.path}' into table t")
      assert{ @m.info == 'Records: 1  Deleted: 0  Skipped: 0  Warnings: 0' }
      assert{ @m.query('select * from t').fetch_row == ['123','abc'] }
    end
    test 'load_data_local_dir: client can execute LOAD DATA LOCAL INFILE query with specified directory' do
      require 'tempfile'
      tmpf = Tempfile.new 'mysql_spec'
      tmpf.puts "123\tabc\n"
      tmpf.close
      @m.load_data_local_dir = File.dirname(tmpf.path)
      @m.connect(MYSQL_SERVER, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT, MYSQL_SOCKET)
      if @m.query('select @@local_infile').fetch[0] == '0'
        omit 'skip because local_infile variable is false'
      end
      @m.query('create temporary table t (i int, c char(10))')
      @m.query("load data local infile '#{tmpf.path}' into table t")
      assert{ @m.query('select * from t').fetch_row == ['123','abc'] }
    end
    test 'load_data_local_dir: client cannot execute LOAD DATA LOCAL INFILE query without specified directory' do
      require 'tempfile'
      tmpf = Tempfile.new 'mysql_spec'
      tmpf.puts "123\tabc\n"
      tmpf.close
      @m.load_data_local_dir = '/hoge'
      @m.connect(MYSQL_SERVER, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT, MYSQL_SOCKET)
      if @m.query('select @@local_infile').fetch[0] == '0'
        omit 'skip because local_infile variable is false'
      end
      @m.query('create temporary table t (i int, c char(10))')
      assert_raise Mysql::ClientError::LoadDataLocalInfileRejected, 'LOAD DATA LOCAL INFILE file request rejected due to restrictions on access.' do
        @m.query("load data local infile '#{tmpf.path}' into table t")
      end
    end
    test 'without local_infile and load_data_local_dir: client cannot execute LOAD DATA LOCAL INFILE query' do
      require 'tempfile'
      tmpf = Tempfile.new 'mysql_spec'
      tmpf.puts "123\tabc\n"
      tmpf.close
      @m.connect(MYSQL_SERVER, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT, MYSQL_SOCKET)
      if @m.query('select @@local_infile').fetch[0] == '0'
        omit 'skip because local_infile variable is false'
      end
      @m.query('create temporary table t (i int, c char(10))')
      assert_raise Mysql::ClientError::LoadDataLocalInfileRejected, 'LOAD DATA LOCAL INFILE file request rejected due to restrictions on access.' do
        @m.query("load data local infile '#{tmpf.path}' into table t")
      end
    end
    test 'read_timeout: set timeout for reading packet' do
      @m.read_timeout = 1
      @m.connect(MYSQL_SERVER, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT, MYSQL_SOCKET)
      @m.query("select 123").entries
    end
    test 'write_timeout: set timeout for writing packet' do
      @m.write_timeout = 1
      @m.connect(MYSQL_SERVER, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT, MYSQL_SOCKET)
      @m.query("select 123").entries
    end
    test 'charset: set charset for connection' do
      @m.charset = 'utf8mb3'
      @m.connect(MYSQL_SERVER, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT, MYSQL_SOCKET)
      assert do
        @m.query('select @@character_set_connection').fetch_row == ['utf8mb3'] ||
          @m.query('select @@character_set_connection').fetch_row == ['utf8']
      end
    end
  end

  sub_test_case 'Mysql' do
    setup do
      @m = Mysql.connect(MYSQL_SERVER, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT, MYSQL_SOCKET)
    end

    teardown do
      @m.close if @m rescue nil
    end

    sub_test_case '#escape_string' do
      test 'escape special character for charset' do
        @m.charset = 'cp932'
        assert{ @m.escape_string("abc'def\"ghi\0jkl%mno_表".encode('cp932')) == "abc\\'def\\\"ghi\\0jkl%mno_表".encode('cp932') }
      end
    end

    sub_test_case '#quote' do
      test 'is alias of #escape_string' do
        assert{ @m.method(:quote) == @m.method(:escape_string) }
      end
    end

    sub_test_case '#affected_rows' do
      test 'returns number of affected rows' do
        @m.query 'create temporary table t (id int)'
        @m.query 'insert into t values (1),(2)'
        assert{ @m.affected_rows == 2 }
      end
    end

    sub_test_case '#character_set_name' do
      test 'returns charset name' do
        m = Mysql.new
        m.charset = 'cp932'
        m.connect MYSQL_SERVER, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT, MYSQL_SOCKET
        assert{ m.character_set_name == 'cp932' }
      end
    end

    sub_test_case '#close' do
      test 'returns self' do
        assert{ @m.close == @m }
      end
    end

    sub_test_case '#close!' do
      test 'returns self' do
        assert{ @m.close! == @m }
      end
    end

    #  sub_test_case '#create_db' do
    #  end

    #  sub_test_case '#drop_db' do
    #  end

    sub_test_case '#errno' do
      test 'default value is 0' do
        assert{ @m.errno == 0 }
      end
      test 'returns error number of latest error' do
        @m.query('hogehoge') rescue nil
        assert{ @m.errno == 1064 }
      end
    end

    sub_test_case '#error' do
      test 'returns error message of latest error' do
        @m.query('hogehoge') rescue nil
        assert{ @m.error == "You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near 'hogehoge' at line 1" }
      end
    end

    sub_test_case '#field_count' do
      test 'returns number of fields for latest query' do
        @m.query 'select 1,2,3'
        assert{ @m.field_count == 3 }
      end
    end

    sub_test_case '#host_info' do
      test 'returns connection type as String' do
        if MYSQL_SERVER == nil or MYSQL_SERVER == 'localhost'
          assert{ @m.host_info == 'Localhost via UNIX socket' }
        else
          assert{ @m.host_info == "#{MYSQL_SERVER} via TCP/IP" }
        end
      end
    end

    sub_test_case '#server_info' do
      test 'returns server version as String' do
        assert{ @m.server_info =~ /\A\d+\.\d+\.\d+/ }
      end
    end

    sub_test_case '#info' do
      test 'returns information of latest query' do
        @m.query 'create temporary table t (id int)'
        @m.query 'insert into t values (1),(2),(3)'
        assert{ @m.info == 'Records: 3  Duplicates: 0  Warnings: 0' }
      end
    end

    sub_test_case '#insert_id' do
      test 'returns latest auto_increment value' do
        @m.query 'create temporary table t (id int auto_increment, unique (id))'
        @m.query 'insert into t values (0)'
        assert{ @m.insert_id == 1 }
        @m.query 'alter table t auto_increment=1234'
        @m.query 'insert into t values (0)'
        assert{ @m.insert_id == 1234 }
      end
    end

    sub_test_case '#kill' do
      setup do
        @m2 = Mysql.connect(MYSQL_SERVER, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT, MYSQL_SOCKET)
      end
      teardown do
        @m2.close rescue nil
      end
      test 'returns self' do
        assert{ @m.kill(@m2.thread_id) == @m }
      end
    end

    sub_test_case '#ping' do
      test 'returns self' do
        assert{ @m.ping == @m }
      end
    end

    sub_test_case '#query' do
      setup do
        @m.set_server_option(Mysql::OPTION_MULTI_STATEMENTS_ON)
      end
      test 'returns Mysql::Result if query returns results' do
        assert{ @m.query('select 123').kind_of? Mysql::Result }
      end
      test 'returns nil if query returns no results' do
        assert{ @m.query('set @hoge=123') == nil }
      end
      test 'returns self if block is specified' do
        assert{ @m.query('select 123'){} == @m }
      end
      test 'returns self if return_result is false' do
        assert{ @m.query('select 123', return_result: false) == @m }
        assert{ @m.store_result.entries == [['123']] }
      end
      test 'if return_result is false and query returns no result' do
        assert{ @m.query('set @hoge=123', return_result: false) == @m }
        assert{ @m.store_result == nil }
      end
      test 'if yield_null_result is true' do
        expects = [[['1']], nil, [['2']]]
        results = []
        @m.query('select 1; set @hoge=123; select 2', yield_null_result: true){|r| results.push r&.entries }
        assert{ results == expects }
      end
      test 'if yield_null_result is false' do
        expects = [[['1']], [['2']]]
        results = []
        @m.query('select 1; set @hoge=123; select 2', yield_null_result: false){|r| results.push r&.entries }
        assert{ results == expects }
      end
    end

    sub_test_case '#refresh' do
      test 'returns self' do
        assert{ @m.refresh(Mysql::REFRESH_HOSTS) == @m }
      end
    end

    sub_test_case '#reload' do
      test 'returns self' do
        assert{ @m.reload == @m }
      end
    end

    sub_test_case '#select_db' do
      test 'changes default database' do
        @m.select_db 'information_schema'
        assert{ @m.query('select database()').fetch_row.first == 'information_schema' }
      end
    end

    #  sub_test_case '#shutdown' do
    #  end

    sub_test_case '#stat' do
      test 'returns server status' do
        assert{ @m.stat =~ /\AUptime: \d+  Threads: \d+  Questions: \d+  Slow queries: \d+  Opens: \d+  Flush tables: \d+  Open tables: \d+  Queries per second avg: \d+\.\d+\z/ }
      end
    end

    sub_test_case '#thread_id' do
      test 'returns thread id as Integer' do
        assert{ @m.thread_id.kind_of? Integer }
      end
    end

    sub_test_case '#server_version' do
      test 'returns server version as Integer' do
        assert{ @m.server_version.kind_of? Integer }
      end
    end

    sub_test_case '#warning_count' do
      setup do
        @m.query("set sql_mode=''")
        @m.query("set sql_mode=''")  # clear warnings on previous `set' statement.
      end
      test 'default values is zero' do
        assert{ @m.warning_count == 0 }
      end
      test 'returns number of warnings' do
        @m.query 'create temporary table t (i tinyint)'
        @m.query 'insert into t values (1234567)'
        assert{ @m.warning_count == 1 }
      end
    end

    sub_test_case '#commit' do
      test 'returns self' do
        assert{ @m.commit == @m }
      end
    end

    sub_test_case '#rollback' do
      test 'returns self' do
        assert{ @m.rollback == @m }
      end
    end

    sub_test_case '#autocommit' do
      test 'returns self' do
        assert{ @m.autocommit(true) == @m }
      end

      test 'change auto-commit mode' do
        @m.autocommit(true)
        assert{ @m.query('select @@autocommit').fetch_row == ['1'] }
        @m.autocommit(false)
        assert{ @m.query('select @@autocommit').fetch_row == ['0'] }
      end
    end

    sub_test_case '#set_server_option' do
      test 'returns self' do
        assert_raise(Mysql::ServerError::ParseError){ @m.query('select 1; select 2'){} }
        assert{ @m.set_server_option(Mysql::OPTION_MULTI_STATEMENTS_ON) == @m }
        assert_nothing_raised{ @m.query('select 1; select 2'){} }
      end
    end

    sub_test_case '#sqlstate' do
      test 'default values is "00000"' do
        assert{ @m.sqlstate == "00000" }
      end
      test 'returns sqlstate code' do
        assert_raise do
          @m.query("hoge")
        end
        assert{ @m.sqlstate == "42000" }
      end
    end

    sub_test_case '#query with block' do
      test 'returns self' do
        assert{ @m.query('select 1'){} == @m }
      end
      test 'evaluate block with Mysql::Result' do
        assert{ @m.query('select 1'){|res| res.kind_of? Mysql::Result} == @m }
      end
      test 'evaluate block multiple times if multiple query is specified' do
        @m.set_server_option Mysql::OPTION_MULTI_STATEMENTS_ON
        cnt = 0
        expect = [["1"], ["2"]]
        assert{ @m.query('select 1; select 2'){|res|
            assert{ res.fetch_row == expect.shift }
            cnt += 1
          } == @m }
        assert{ cnt == 2 }
      end
      test 'evaluate block only when query has result' do
        @m.set_server_option Mysql::OPTION_MULTI_STATEMENTS_ON
        cnt = 0
        expect = [[["1"]], nil, [["2"]]]
        assert do
          @m.query('select 1; set @hoge:=1; select 2'){|res|
            assert{ res&.entries == expect.shift }
            cnt += 1
          } == @m
        end
        assert{ cnt == 3 }
      end
    end
  end

  test 'multiple statement query' do
    m = Mysql.connect(MYSQL_SERVER, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT, MYSQL_SOCKET)
    m.set_server_option(Mysql::OPTION_MULTI_STATEMENTS_ON)
    res = m.query 'select 1,2; select 3,4,5'
    assert{ res.entries == [['1','2']] }
    assert{ m.more_results? == true }
    assert{ m.next_result.entries == [['3','4','5']] }
    assert{ m.more_results? == false }
    assert{ m.next_result == nil }
    m.close!
  end

  test 'multiple statement error' do
    m = Mysql.connect(MYSQL_SERVER, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT, MYSQL_SOCKET)
    m.set_server_option(Mysql::OPTION_MULTI_STATEMENTS_ON)
    res = m.query 'select 1; select hoge; select 2'
    assert{ res.entries == [['1']] }
    assert{ m.more_results? == true }
    assert_raise(Mysql::ServerError::BadFieldError){ m.next_result }
    assert{ m.more_results? == false }
    m.close!
  end

  test 'procedure returns multiple results' do
    m = Mysql.connect(MYSQL_SERVER, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT, MYSQL_SOCKET)
    m.query 'drop procedure if exists test_proc'
    m.query 'create procedure test_proc() begin select 1 as a; select 2 as b; end'
    res = m.query 'call test_proc()'
    assert{ res.entries == [['1']] }
    assert{ m.more_results? == true }
    assert{ m.next_result.entries == [['2']] }
    assert{ m.more_results? == true }
    assert{ m.next_result == nil }
    assert{ m.more_results? == false }
  end

  test 'multiple statements includes no results statement' do
    m = Mysql.connect(MYSQL_SERVER, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT, MYSQL_SOCKET)
    m.set_server_option(Mysql::OPTION_MULTI_STATEMENTS_ON)
    m.query('create temporary table t (i int)')
    res = m.query 'select 1; insert into t values (1),(2),(3); select 2'
    assert{ res.entries == [['1']] }
    assert{ m.more_results? == true }
    assert{ m.next_result == nil }
    assert{ m.info == 'Records: 3  Duplicates: 0  Warnings: 0' }
    assert{ m.more_results? == true }
    assert{ m.next_result.entries == [['2']] }
    assert{ m.more_results? == false }
  end

  sub_test_case 'Mysql::Result' do
    setup do
      @m = Mysql.connect(MYSQL_SERVER, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT, MYSQL_SOCKET)
      @m.charset = 'latin1'
      @m.query 'create temporary table t (id int default 0, str char(10), primary key (id))'
      @m.query "insert into t values (1,'abc'),(2,'defg'),(3,'hi'),(4,null)"
      @res = @m.query 'select * from t'
    end

    teardown do
      @m.close if @m
    end

    test '#data_seek set position of current record' do
      assert{ @res.fetch_row == ['1', 'abc'] }
      assert{ @res.fetch_row == ['2', 'defg'] }
      assert{ @res.fetch_row == ['3', 'hi'] }
      @res.data_seek 1
      assert{ @res.fetch_row == ['2', 'defg'] }
    end

    test '#fields returns array of field' do
      f = @res.fields[0]
      assert{ f.name == 'id' }
      assert{ f.table == 't' }
      assert{ f.def == nil }
      assert{ f.type == Mysql::Field::TYPE_LONG }
      assert{ f.length == 11 }
      assert{ f.max_length == 1 }
      assert{ f.flags == Mysql::Field::NUM_FLAG|Mysql::Field::PRI_KEY_FLAG|Mysql::Field::PART_KEY_FLAG|Mysql::Field::NOT_NULL_FLAG }
      assert{ f.decimals == 0 }

      f = @res.fields[1]
      assert{ f.name == 'str' }
      assert{ f.table == 't' }
      assert{ f.def == nil }
      assert{ f.type == Mysql::Field::TYPE_STRING }
      assert{ f.length == 10 }
      assert{ f.max_length == 4 }
      assert{ f.flags == 0 }
      assert{ f.decimals == 0 }

      assert{ @res.fields[2] == nil }
    end

    test '#fetch_fields returns array of fields' do
      ret = @res.fetch_fields
      assert{ ret.size == 2 }
      assert{ ret[0].name == 'id' }
      assert{ ret[1].name == 'str' }
    end

    test '#fetch_row returns one record as array for current record' do
      assert{ @res.fetch_row == ['1', 'abc'] }
      assert{ @res.fetch_row == ['2', 'defg'] }
      assert{ @res.fetch_row == ['3', 'hi'] }
      assert{ @res.fetch_row == ['4', nil] }
      assert{ @res.fetch_row == nil }
    end

    test '#fetch_hash returns one record as hash for current record' do
      assert{ @res.fetch_hash == {'id'=>'1', 'str'=>'abc'} }
      assert{ @res.fetch_hash == {'id'=>'2', 'str'=>'defg'} }
      assert{ @res.fetch_hash == {'id'=>'3', 'str'=>'hi'} }
      assert{ @res.fetch_hash == {'id'=>'4', 'str'=>nil} }
      assert{ @res.fetch_hash == nil }
    end

    test '#fetch_hash(true) returns with table name' do
      assert{ @res.fetch_hash(true) == {'t.id'=>'1', 't.str'=>'abc'} }
      assert{ @res.fetch_hash(true) == {'t.id'=>'2', 't.str'=>'defg'} }
      assert{ @res.fetch_hash(true) == {'t.id'=>'3', 't.str'=>'hi'} }
      assert{ @res.fetch_hash(true) == {'t.id'=>'4', 't.str'=>nil} }
      assert{ @res.fetch_hash(true) == nil }
    end

    test '#num_rows returns number of records' do
      assert{ @res.num_rows == 4 }
    end

    test '#each iterate block with a record' do
      expect = [["1","abc"], ["2","defg"], ["3","hi"], ["4",nil]]
      @res.each do |a|
        assert{ a == expect.shift }
      end
    end

    test '#each_hash iterate block with a hash' do
      expect = [{"id"=>"1","str"=>"abc"}, {"id"=>"2","str"=>"defg"}, {"id"=>"3","str"=>"hi"}, {"id"=>"4","str"=>nil}]
      @res.each_hash do |a|
        assert{ a == expect.shift }
      end
    end

    test '#each_hash(true): hash key has table name' do
      expect = [{"t.id"=>"1","t.str"=>"abc"}, {"t.id"=>"2","t.str"=>"defg"}, {"t.id"=>"3","t.str"=>"hi"}, {"t.id"=>"4","t.str"=>nil}]
      @res.each_hash(true) do |a|
        assert{ a == expect.shift }
      end
    end

    test '#each always returns records from the beginning' do
      assert{ @res.each.entries == [["1", "abc"], ["2", "defg"], ["3", "hi"], ["4", nil]] }
      assert{ @res.each.entries == [["1", "abc"], ["2", "defg"], ["3", "hi"], ["4", nil]] }
    end

    test '#row_tell returns position of current record, #row_seek set position of current record' do
      assert{ @res.fetch_row == ['1', 'abc'] }
      pos = @res.row_tell
      assert{ @res.fetch_row == ['2', 'defg'] }
      assert{ @res.fetch_row == ['3', 'hi'] }
      @res.row_seek pos
      assert{ @res.fetch_row == ['2', 'defg'] }
    end

    test '#free returns nil' do
      assert{ @res.free == nil }
    end

    test '#server_status returns server status as Intger' do
      assert{ @res.server_status.is_a? Integer }
    end
  end

  sub_test_case 'Mysql::Field' do
    setup do
      @m = Mysql.connect(MYSQL_SERVER, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT, MYSQL_SOCKET)
      @m.charset = 'latin1'
      @m.query 'create temporary table t (id int default 0, str char(10), primary key (id))'
      @m.query "insert into t values (1,'abc'),(2,'defg'),(3,'hi'),(4,null)"
      @res = @m.query 'select * from t'
    end

    teardown do
      @m.close if @m
    end

    test '#name is name of field' do
      assert{ @res.fields[0].name == 'id' }
    end

    test '#table is name of table for field' do
      assert{ @res.fields[0].table == 't' }
    end

    test '#def for result set is null' do
      assert{ @res.fields[0].def == nil }
    end

    test '#type is type of field as Integer' do
      assert{ @res.fields[0].type == Mysql::Field::TYPE_LONG }
      assert{ @res.fields[1].type == Mysql::Field::TYPE_STRING }
    end

    test '#length is length of field' do
      assert{ @res.fields[0].length == 11 }
      assert{ @res.fields[1].length == 10 }
    end

    test '#max_length is maximum length of field value' do
      assert{ @res.fields[0].max_length == 1 }
      assert{ @res.fields[1].max_length == 4 }
    end

    test '#flags is flag of field as Integer' do
      assert{ @res.fields[0].flags == Mysql::Field::NUM_FLAG|Mysql::Field::PRI_KEY_FLAG|Mysql::Field::PART_KEY_FLAG|Mysql::Field::NOT_NULL_FLAG }
      assert{ @res.fields[1].flags == 0 }
    end

    test '#decimals is number of decimal digits' do
      assert{ @m.query('select 1.23').fields[0].decimals == 2 }
    end

    test '#to_hash return field as hash' do
      assert{ @res.fields[0].to_hash == {
          'name'       => 'id',
          'table'      => 't',
          'def'        => nil,
          'type'       => Mysql::Field::TYPE_LONG,
          'length'     => 11,
          'max_length' => 1,
          'flags'      => Mysql::Field::NUM_FLAG|Mysql::Field::PRI_KEY_FLAG|Mysql::Field::PART_KEY_FLAG|Mysql::Field::NOT_NULL_FLAG,
          'decimals'   => 0,
        }
      }
      assert{ @res.fields[1].to_hash == {
          'name'       => 'str',
          'table'      => 't',
          'def'        => nil,
          'type'       => Mysql::Field::TYPE_STRING,
          'length'     => 10,
          'max_length' => 4,
          'flags'      => 0,
          'decimals'   => 0,
        }
      }
    end

    test '#inspect returns "#<Mysql::Field:name>"' do
      assert{ @res.fields[0].inspect == '#<Mysql::Field:id>' }
      assert{ @res.fields[1].inspect == '#<Mysql::Field:str>' }
    end

    test '#is_num? returns true if the field is numeric' do
      assert{ @res.fields[0].is_num? == true }
      assert{ @res.fields[1].is_num? == false }
    end

    test '#is_not_null? returns true if the field is not null' do
      assert{ @res.fields[0].is_not_null? == true }
      assert{ @res.fields[1].is_not_null? == false }
    end

    test '#is_pri_key? returns true if the field is primary key' do
      assert{ @res.fields[0].is_pri_key? == true }
      assert{ @res.fields[1].is_pri_key? == false }
    end
  end

  sub_test_case 'create Mysql::Stmt object:' do
    setup do
      @m = Mysql.connect(MYSQL_SERVER, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT, MYSQL_SOCKET)
    end

    teardown do
      @m.close if @m
    end

    test 'Mysql#stmt returns Mysql::Stmt object' do
      assert{ @m.stmt.kind_of? Mysql::Stmt }
    end

    test 'Mysq;#prepare returns Mysql::Stmt object' do
      assert{ @m.prepare("select 1").kind_of? Mysql::Stmt }
    end
  end

  sub_test_case 'Mysql::Stmt' do
    setup do
      @m = Mysql.connect(MYSQL_SERVER, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT, MYSQL_SOCKET)
      @m.query("set sql_mode=''")
      @s = @m.stmt
    end

    teardown do
      @s.close if @s rescue nil
      @m.close if @m rescue nil
    end

    test '#affected_rows returns number of affected records' do
      @m.query 'create temporary table t (i int, c char(10))'
      @s.prepare 'insert into t values (?,?)'
      @s.execute 1, 'hoge'
      assert{ @s.affected_rows == 1 }
      @s.execute 2, 'hoge'
      @s.execute 3, 'hoge'
      @s.prepare 'update t set c=?'
      @s.execute 'fuga'
      assert{ @s.affected_rows == 3 }
    end

    test '#close returns nil' do
      assert{ @s.close == nil }
    end

    test '#data_seek set position of current record' do
      @m.query 'create temporary table t (i int)'
      @m.query 'insert into t values (0),(1),(2),(3),(4),(5),(6)'
      @s.prepare 'select i from t'
      res = @s.execute
      assert{ res.fetch == [0] }
      assert{ res.fetch == [1] }
      assert{ res.fetch == [2] }
      res.data_seek 5
      assert{ res.fetch == [5] }
      res.data_seek 1
      assert{ res.fetch == [1] }
    end

    test '#each iterate block with a record' do
      @m.query 'create temporary table t (i int, c char(255), d datetime)'
      @m.query "insert into t values (1,'abc','19701224235905'),(2,'def','21120903123456'),(3,'123',null)"
      @s.prepare 'select * from t'
      res = @s.execute
      expect = [
        [1, 'abc', Time.new(1970,12,24,23,59,05)],
        [2, 'def', Time.new(2112,9,3,12,34,56)],
        [3, '123', nil],
      ]
      res.each do |a|
        assert{ a == expect.shift }
      end
    end

    test '#execute returns result set' do
      @s.prepare 'select 1'
      assert{ @s.execute.entries == [[1]] }
    end

    test '#execute returns nil if query returns no results' do
      @s.prepare 'set @a=1'
      assert{ @s.execute == nil }
    end

    test '#execute returns self if return_result is false' do
      @s.prepare 'select 1'
      assert{ @s.execute(return_result: false) == @s }
    end

    test '#execute pass arguments to query' do
      @m.query 'create temporary table t (i int)'
      @s.prepare 'insert into t values (?)'
      @s.execute 123
      @s.execute '456'
      @s.execute true
      @s.execute false
      assert{ @m.query('select * from t').entries == [['123'], ['456'], ['1'], ['0']] }
    end

    test '#execute with various arguments' do
      @m.query 'create temporary table t (i int, c char(255), t timestamp)'
      @s.prepare 'insert into t values (?,?,?)'
      @s.execute 123, 'hoge', Time.local(2009,12,8,19,56,21)
      assert{ @m.query('select * from t').fetch_row == ['123', 'hoge', '2009-12-08 19:56:21'] }
    end

    test '#execute with arguments that is invalid count raise error' do
      @s.prepare 'select ?'
      assert_raise Mysql::ClientError, 'parameter count mismatch' do
        @s.execute 123, 456
      end
    end

    test '#execute with huge value' do
      [30, 31, 32, 62, 63, 64].each do |i|
        assert{ @m.prepare('select ?').execute(2**i-1).fetch == [2**i-1] }
        assert{ @m.prepare('select ?').execute(-(2**i)).fetch == [-2**i] }
      end
    end

    sub_test_case '#execute with various integer value:' do
      setup do
        @m.query('create temporary table t (i bigint)')
      end
      [
        -9223372036854775808,
        -9223372036854775807,
        -4294967297,
        -4294967296,
        -4294967295,
        -2147483649,
        -2147483648,
        -2147483647,
        -65537,
        -65536,
        -65535,
        -32769,
        -32768,
        -32767,
        -257,
        -256,
        -255,
        -129,
        -128,
        -127,
        0,
        126,
        127,
        128,
        254,
        255,
        256,
        32766,
        32767,
        32768,
        65534,
        65535,
        65536,
        2147483646,
        2147483647,
        2147483648,
        4294967294,
        4294967295,
        4294967296,
        9223372036854775806,
        9223372036854775807,
      ].each do |n|
        test "#{n} is #{n}" do
          @s.prepare 'insert into t values (?)'
          @s.execute n
          assert{ @m.query('select i from t').fetch == ["#{n}"] }
        end
      end
    end

    sub_test_case '#execute with various unsigned integer value:' do
      setup do
        @m.query('create temporary table t (i bigint unsigned)')
      end
      [
        0,
        126,
        127,
        128,
        254,
        255,
        256,
        32766,
        32767,
        32768,
        65534,
        65535,
        65536,
        2147483646,
        2147483647,
        2147483648,
        4294967294,
        4294967295,
        4294967296,
        9223372036854775806,
        9223372036854775807,
        9223372036854775808,
        18446744073709551614,
        18446744073709551615,
      ].each do |n|
        test "#{n} is #{n}" do
          @s.prepare 'insert into t values (?)'
          @s.execute n
          assert{ @m.query('select i from t').fetch == ["#{n}"] }
        end
      end
    end

    test '#fetch returns result-record' do
      @s.prepare 'select 123, "abc", null'
      @s.execute
      assert{ @s.fetch == [123, 'abc', nil] }
    end

    test '#fetch bit column (8bit)' do
      @m.query 'create temporary table t (i bit(8))'
      @m.query 'insert into t values (0),(-1),(127),(-128),(255),(-255),(256)'
      @s.prepare 'select i from t'
      @s.execute
      assert{ @s.entries == [
          ["\x00".force_encoding('ASCII-8BIT')],
          ["\xff".force_encoding('ASCII-8BIT')],
          ["\x7f".force_encoding('ASCII-8BIT')],
          ["\xff".force_encoding('ASCII-8BIT')],
          ["\xff".force_encoding('ASCII-8BIT')],
          ["\xff".force_encoding('ASCII-8BIT')],
          ["\xff".force_encoding('ASCII-8BIT')],
        ]
      }
    end

    test '#fetch bit column (64bit)' do
      @m.query 'create temporary table t (i bit(64))'
      @m.query 'insert into t values (0),(-1),(4294967296),(18446744073709551615),(18446744073709551616)'
      @s.prepare 'select i from t'
      @s.execute
      assert{ @s.entries == [
          ["\x00\x00\x00\x00\x00\x00\x00\x00".force_encoding('ASCII-8BIT')],
          ["\xff\xff\xff\xff\xff\xff\xff\xff".force_encoding('ASCII-8BIT')],
          ["\x00\x00\x00\x01\x00\x00\x00\x00".force_encoding('ASCII-8BIT')],
          ["\xff\xff\xff\xff\xff\xff\xff\xff".force_encoding('ASCII-8BIT')],
          ["\xff\xff\xff\xff\xff\xff\xff\xff".force_encoding('ASCII-8BIT')],
        ]
      }
    end

    test '#fetch tinyint column' do
      @m.query 'create temporary table t (i tinyint)'
      @m.query 'insert into t values (0),(-1),(127),(-128),(255),(-255)'
      @s.prepare 'select i from t'
      @s.execute
      assert{ @s.entries == [[0], [-1], [127], [-128], [127], [-128]] }
    end

    test '#fetch tinyint unsigned column' do
      @m.query 'create temporary table t (i tinyint unsigned)'
      @m.query 'insert into t values (0),(-1),(127),(-128),(255),(-255),(256)'
      @s.prepare 'select i from t'
      @s.execute
      assert{ @s.entries == [[0], [0], [127], [0], [255], [0], [255]] }
    end

    test '#fetch smallint column' do
      @m.query 'create temporary table t (i smallint)'
      @m.query 'insert into t values (0),(-1),(32767),(-32768),(65535),(-65535),(65536)'
      @s.prepare 'select i from t'
      @s.execute
      assert{ @s.entries == [[0], [-1], [32767], [-32768], [32767], [-32768], [32767]] }
    end

    test '#fetch smallint unsigned column' do
      @m.query 'create temporary table t (i smallint unsigned)'
      @m.query 'insert into t values (0),(-1),(32767),(-32768),(65535),(-65535),(65536)'
      @s.prepare 'select i from t'
      @s.execute
      assert{ @s.entries == [[0], [0], [32767], [0], [65535], [0], [65535]] }
    end

    test '#fetch mediumint column' do
      @m.query 'create temporary table t (i mediumint)'
      @m.query 'insert into t values (0),(-1),(8388607),(-8388608),(16777215),(-16777215),(16777216)'
      @s.prepare 'select i from t'
      @s.execute
      assert{ @s.entries == [[0], [-1], [8388607], [-8388608], [8388607], [-8388608], [8388607]] }
    end

    test '#fetch mediumint unsigned column' do
      @m.query 'create temporary table t (i mediumint unsigned)'
      @m.query 'insert into t values (0),(-1),(8388607),(-8388608),(16777215),(-16777215),(16777216)'
      @s.prepare 'select i from t'
      @s.execute
      assert{ @s.entries == [[0], [0], [8388607], [0], [16777215], [0], [16777215]] }
    end

    test '#fetch int column' do
      @m.query 'create temporary table t (i int)'
      @m.query 'insert into t values (0),(-1),(2147483647),(-2147483648),(4294967295),(-4294967295),(4294967296)'
      @s.prepare 'select i from t'
      @s.execute
      assert{ @s.entries == [[0], [-1], [2147483647], [-2147483648], [2147483647], [-2147483648], [2147483647]] }
    end

    test '#fetch int unsigned column' do
      @m.query 'create temporary table t (i int unsigned)'
      @m.query 'insert into t values (0),(-1),(2147483647),(-2147483648),(4294967295),(-4294967295),(4294967296)'
      @s.prepare 'select i from t'
      @s.execute
      assert{ @s.entries == [[0], [0], [2147483647], [0], [4294967295], [0], [4294967295]] }
    end

    test '#fetch bigint column' do
      @m.query 'create temporary table t (i bigint)'
      @m.query 'insert into t values (0),(-1),(9223372036854775807),(-9223372036854775808),(18446744073709551615),(-18446744073709551615),(18446744073709551616)'
      @s.prepare 'select i from t'
      @s.execute
      assert{ @s.entries == [[0], [-1], [9223372036854775807], [-9223372036854775808], [9223372036854775807], [-9223372036854775808], [9223372036854775807]] }
    end

    test '#fetch bigint unsigned column' do
      @m.query 'create temporary table t (i bigint unsigned)'
      @m.query 'insert into t values (0),(-1),(9223372036854775807),(-9223372036854775808),(18446744073709551615),(-18446744073709551615),(18446744073709551616)'
      @s.prepare 'select i from t'
      @s.execute
      assert{ @s.entries == [[0], [0], [9223372036854775807], [0], [18446744073709551615], [0], [18446744073709551615]] }
    end

    test '#fetch float column' do
      @m.query 'create temporary table t (i float)'
      @m.query 'insert into t values (0),(-3.402823466E+38),(-1.175494351E-38),(1.175494351E-38),(3.402823466E+38)'
      @s.prepare 'select i from t'
      @s.execute
      assert{ @s.fetch[0] == 0.0 }
      assert{ (@s.fetch[0] - -3.402823466E+38).abs < 0.000000001E+38 }
      assert{ (@s.fetch[0] - -1.175494351E-38).abs < 0.000000001E-38 }
      assert{ (@s.fetch[0] -  1.175494351E-38).abs < 0.000000001E-38 }
      assert{ (@s.fetch[0] -  3.402823466E+38).abs < 0.000000001E+38 }
    end

    test '#fetch float unsigned column' do
      @m.query 'create temporary table t (i float unsigned)'
      @m.query 'insert into t values (0),(-3.402823466E+38),(-1.175494351E-38),(1.175494351E-38),(3.402823466E+38)'
      @s.prepare 'select i from t'
      @s.execute
      assert{ @s.fetch[0] == 0.0 }
      assert{ @s.fetch[0] == 0.0 }
      assert{ @s.fetch[0] == 0.0 }
      assert{ (@s.fetch[0] -  1.175494351E-38).abs < 0.000000001E-38 }
      assert{ (@s.fetch[0] -  3.402823466E+38).abs < 0.000000001E+38 }
    end

    test '#fetch double column' do
      @m.query 'create temporary table t (i double)'
      @m.query 'insert into t values (0),(-1.7976931348623157E+308),(-2.2250738585072014E-308),(2.2250738585072014E-308),(1.7976931348623157E+308)'
      @s.prepare 'select i from t'
      @s.execute
      assert{ @s.fetch[0] == 0.0 }
      assert{ (@s.fetch[0] - -Float::MAX).abs < Float::EPSILON }
      assert{ (@s.fetch[0] - -Float::MIN).abs < Float::EPSILON }
      assert{ (@s.fetch[0] -  Float::MIN).abs < Float::EPSILON }
      assert{ (@s.fetch[0] -  Float::MAX).abs < Float::EPSILON }
    end

    test '#fetch double unsigned column' do
      @m.query 'create temporary table t (i double unsigned)'
      @m.query 'insert into t values (0),(-1.7976931348623157E+308),(-2.2250738585072014E-308),(2.2250738585072014E-308),(1.7976931348623157E+308)'
      @s.prepare 'select i from t'
      @s.execute
      assert{ @s.fetch[0] == 0.0 }
      assert{ @s.fetch[0] == 0.0 }
      assert{ @s.fetch[0] == 0.0 }
      assert{ (@s.fetch[0] - Float::MIN).abs < Float::EPSILON }
      assert{ (@s.fetch[0] - Float::MAX).abs < Float::EPSILON }
    end

    test '#fetch decimal column' do
      @m.query 'create temporary table t (i decimal(12,2))'
      @m.query 'insert into t values (0),(9999999999),(-9999999999),(10000000000),(-10000000000)'
      @s.prepare 'select i from t'
      @s.execute
      assert{ @s.entries == [[0], [9999999999], [-9999999999], [BigDecimal('9999999999.99')], [BigDecimal('-9999999999.99')]] }
    end

    test '#fetch decimal unsigned column' do
      @m.query 'create temporary table t (i decimal(12,2) unsigned)'
      @m.query 'insert into t values (0),(9999999998),(9999999999),(-9999999998),(-9999999999),(10000000000),(-10000000000)'
      @s.prepare 'select i from t'
      @s.execute
      assert{ @s.entries == [[0], [9999999998], [9999999999], [0], [0], [BigDecimal('9999999999.99')], [0]] }
    end

    test '#fetch date column' do
      @m.query 'create temporary table t (i date)'
      @m.query "insert into t values ('0000-00-00'),('1000-01-01'),('9999-12-31')"
      @s.prepare 'select i from t'
      @s.execute
      cols = @s.fetch
      assert{ cols == [nil] }
      cols = @s.fetch
      assert{ cols == [Date.new(1000,1,1)] }
      cols = @s.fetch
      assert{ cols == [Date.new(9999,12,31)] }
    end

    test '#fetch datetime column' do
      @m.query 'create temporary table t (i datetime(6))'
      @m.query "insert into t values ('0000-00-00 00:00:00'),('1000-01-01 00:00:00'),('2022-10-30 12:34:56.789'),('9999-12-31 23:59:59')"
      @s.prepare 'select i from t'
      @s.execute
      assert{ @s.fetch == [nil] }
      assert{ @s.fetch == [Time.new(1000,1,1)] }
      assert{ @s.fetch == [Time.new(2022,10,30,12,34,56789/1000r)] }
      assert{ @s.fetch == [Time.new(9999,12,31,23,59,59)] }
    end

    test '#fetch timestamp column' do
      @m.query 'create temporary table t (i timestamp(6))'
      @m.query("insert into t values ('1970-01-02 00:00:00'),('2022-10-30 12:34:56.789'),('2037-12-30 23:59:59')")
      @s.prepare 'select i from t'
      @s.execute
      assert{ @s.fetch == [Time.new(1970,1,2)] }
      assert{ @s.fetch == [Time.new(2022,10,30,12,34,56789/1000r)] }
      assert{ @s.fetch == [Time.new(2037,12,30,23,59,59)] }
    end

    test '#fetch time column' do
      @m.query 'create temporary table t (i time)'
      @m.query "insert into t values ('-838:59:59'),(0),('838:59:59')"
      @s.prepare 'select i from t'
      @s.execute
      assert{ @s.fetch == [-(838*3600+59*60+59)] }
      assert{ @s.fetch == [0] }
      assert{ @s.fetch == [838*3600+59*60+59] }
    end

    test '#fetch year column' do
      @m.query 'create temporary table t (i year)'
      @m.query 'insert into t values (0),(70),(69),(1901),(2155)'
      @s.prepare 'select i from t'
      @s.execute
      assert{ @s.entries == [[0], [1970], [2069], [1901], [2155]] }
    end

    test '#fetch char column' do
      @m.query 'create temporary table t (i char(10))'
      @m.query "insert into t values (null),('abc')"
      @s.prepare 'select i from t'
      @s.execute
      assert{ @s.entries == [[nil], ['abc']] }
    end

    test '#fetch varchar column' do
      @m.query 'create temporary table t (i varchar(10))'
      @m.query "insert into t values (null),('abc')"
      @s.prepare 'select i from t'
      @s.execute
      assert{ @s.entries == [[nil], ['abc']] }
    end

    test '#fetch binary column' do
      @m.query 'create temporary table t (i binary(10))'
      @m.query "insert into t values (null),('abc')"
      @s.prepare 'select i from t'
      @s.execute
      assert{ @s.entries == [[nil], ["abc\0\0\0\0\0\0\0"]] }
    end

    test '#fetch varbinary column' do
      @m.query 'create temporary table t (i varbinary(10))'
      @m.query "insert into t values (null),('abc')"
      @s.prepare 'select i from t'
      @s.execute
      assert{ @s.entries == [[nil], ["abc"]] }
    end

    test '#fetch tinyblob column' do
      @m.query 'create temporary table t (i tinyblob)'
      @m.query "insert into t values (null),('#{"a"*255}')"
      @s.prepare 'select i from t'
      @s.execute
      assert{ @s.entries == [[nil], ["a"*255]] }
    end

    test '#fetch tinytext column' do
      @m.query 'create temporary table t (i tinytext)'
      @m.query "insert into t values (null),('#{"a"*255}')"
      @s.prepare 'select i from t'
      @s.execute
      assert{ @s.entries == [[nil], ["a"*255]] }
    end

    test '#fetch blob column' do
      @m.query 'create temporary table t (i blob)'
      @m.query "insert into t values (null),('#{"a"*65535}')"
      @s.prepare 'select i from t'
      @s.execute
      assert{ @s.entries == [[nil], ["a"*65535]] }
    end

    test '#fetch text column' do
      @m.query 'create temporary table t (i text)'
      @m.query "insert into t values (null),('#{"a"*65535}')"
      @s.prepare 'select i from t'
      @s.execute
      assert{ @s.entries == [[nil], ["a"*65535]] }
    end

    test '#fetch mediumblob column' do
      @m.query 'create temporary table t (i mediumblob)'
      @m.query "insert into t values (null),('#{"a"*16777215}')"
      @s.prepare 'select i from t'
      @s.execute
      assert{ @s.entries == [[nil], ['a'*16777215]] }
    end

    test '#fetch mediumtext column' do
      @m.query 'create temporary table t (i mediumtext)'
      @m.query "insert into t values (null),('#{"a"*16777215}')"
      @s.prepare 'select i from t'
      @s.execute
      assert{ @s.entries == [[nil], ['a'*16777215]] }
    end

    test '#fetch longblob column' do
      @m.query 'create temporary table t (i longblob)'
      @m.query "insert into t values (null),('#{"a"*16777216}')"
      @s.prepare 'select i from t'
      @s.execute
      assert{ @s.entries == [[nil], ["a"*16777216]] }
    end

    test '#fetch longtext column' do
      @m.query 'create temporary table t (i longtext)'
      @m.query "insert into t values (null),('#{"a"*16777216}')"
      @s.prepare 'select i from t'
      @s.execute
      assert{ @s.entries == [[nil], ["a"*16777216]] }
    end

    test '#fetch enum column' do
      @m.query "create temporary table t (i enum('abc','def'))"
      @m.query "insert into t values (null),(0),(1),(2),('abc'),('def'),('ghi')"
      @s.prepare 'select i from t'
      @s.execute
      assert{ @s.entries == [[nil], [''], ['abc'], ['def'], ['abc'], ['def'], ['']] }
    end

    test '#fetch set column' do
      @m.query "create temporary table t (i set('abc','def'))"
      @m.query "insert into t values (null),(0),(1),(2),(3),('abc'),('def'),('abc,def'),('ghi')"
      @s.prepare 'select i from t'
      @s.execute
      assert{ @s.entries == [[nil], [''], ['abc'], ['def'], ['abc,def'], ['abc'], ['def'], ['abc,def'], ['']] }
    end

    test '#fetch json column' do
      if @m.server_version >= 50700
        @m.query "create temporary table t (i json)"
        @m.query "insert into t values ('123'),('{\"a\":1,\"b\":2,\"c\":3}'),('[1,2,3]')"
        @s.prepare 'select i from t'
        @s.execute
        assert{ @s.entries == [['123'], ['{"a": 1, "b": 2, "c": 3}'], ['[1, 2, 3]']] }
      end
    end

    test '#field_count' do
      @s.prepare 'select 1,2,3'
      assert{ @s.field_count == 3 }
      @s.prepare 'set @a=1'
      assert{ @s.field_count == 0 }
    end

    test '#free_result' do
      @s.free_result
      @s.prepare 'select 1,2,3'
      @s.execute
      @s.free_result
    end

    test '#info' do
      @s.free_result
      @m.query 'create temporary table t (i int)'
      @s.prepare 'insert into t values (1),(2),(3)'
      @s.execute
      assert{ @s.info == 'Records: 3  Duplicates: 0  Warnings: 0' }
    end

    test '#insert_id' do
      @m.query 'create temporary table t (i int auto_increment, unique(i))'
      @s.prepare 'insert into t values (0)'
      @s.execute
      assert{ @s.insert_id == 1 }
      @s.execute
      assert{ @s.insert_id == 2 }
    end

    test '#more_reults? and #next_result' do
      @m.query 'drop procedure if exists test_proc'
      @m.query 'create procedure test_proc() begin select 1 as a; select 2 as b; end'
      st = @m.prepare 'call test_proc()'
      res = st.execute
      assert{ res.entries == [[1]] }
      assert{ st.more_results? == true }
      res = st.next_result
      assert{ res.entries == [[2]] }
      assert{ st.more_results? == true }
      res = st.next_result
      assert{ res == nil }
      assert{ st.more_results? == false }
    end

    sub_test_case '#execute with block' do
      setup do
        @m.query 'drop procedure if exists test_proc'
        @m.query 'create procedure test_proc() begin select 1 as a; select 2 as b; end'
        @st = @m.prepare 'call test_proc()'
      end
      test 'returns self' do
        assert{ @st.execute{} == @st }
      end
      test 'evaluate block multiple times' do
        res = []
        @st.execute do |r|
          res.push r&.entries
        end
        assert{ res == [[[1]], [[2]], nil] }
      end
      test 'evaluate block only when query has result' do
        res = []
        @st.execute(yield_null_result: false) do |r|
          res.push r&.entries
        end
        assert{ res == [[[1]], [[2]]] }
      end
    end

    test '#num_rows' do
      @m.query 'create temporary table t (i int)'
      @m.query 'insert into t values (1),(2),(3),(4)'
      @s.prepare 'select * from t'
      @s.execute
      assert{ @s.num_rows == 4 }
    end

    test '#param_count' do
      @m.query 'create temporary table t (a int, b int, c int)'
      @s.prepare 'select * from t'
      assert{ @s.param_count == 0 }
      @s.prepare 'insert into t values (?,?,?)'
      assert{ @s.param_count == 3 }
    end

    test '#prepare' do
      assert{ @s.prepare('select 1').kind_of? Mysql::Stmt }
      assert_raise Mysql::ParseError do
        @s.prepare 'invalid syntax'
      end
    end

    test '#prepare returns self' do
      assert{ @s.prepare('select 1') == @s }
    end

    test '#prepare with invalid query raises error' do
      assert_raise Mysql::ParseError do
        @s.prepare 'invalid query'
      end
    end

    test '#fields' do
      @s.prepare 'select 1 foo, 2 bar'
      f = @s.fields
      assert{ f[0].name == 'foo' }
      assert{ f[1].name == 'bar' }

      @s.prepare 'set @a=1'
      assert{ @s.fields == [] }
    end

    test '#result_metadata' do
      @s.prepare 'select 1 foo, 2 bar'
      f = @s.result_metadata.fetch_fields
      assert{ f[0].name == 'foo' }
      assert{ f[1].name == 'bar' }
    end

    test '#result_metadata forn no data' do
      @s.prepare 'set @a=1'
      assert{ @s.result_metadata == nil }
    end

    test '#row_seek and #row_tell' do
      @m.query 'create temporary table t (i int)'
      @m.query 'insert into t values (0),(1),(2),(3),(4)'
      @s.prepare 'select * from t'
      @s.execute
      row0 = @s.row_tell
      assert{ @s.fetch == [0] }
      assert{ @s.fetch == [1] }
      row2 = @s.row_seek row0
      assert{ @s.fetch == [0] }
      @s.row_seek row2
      assert{ @s.fetch == [2] }
    end

    test '#sqlstate' do
      @s.prepare 'select 1'
      assert{ @s.sqlstate == '00000' }
      assert_raise Mysql::ParseError do
        @s.prepare 'hogehoge'
      end
      assert{ @s.sqlstate == '42000' }
    end
  end

  sub_test_case 'Mysql::Error' do
    setup do
      m = Mysql.connect(MYSQL_SERVER, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT, MYSQL_SOCKET)
      begin
        m.query('hogehoge')
      rescue => @e
      end
    end

    test '#error is error message' do
      assert{ @e.error == "You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near 'hogehoge' at line 1" }
    end

    test '#errno is error number' do
      assert{ @e.errno == 1064 }
    end

    test '#sqlstate is sqlstate value as String' do
      assert{ @e.sqlstate == '42000' }
    end
  end

  sub_test_case 'Connection charset is UTF-8:' do
    setup do
      @m = Mysql.connect(MYSQL_SERVER, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT, MYSQL_SOCKET)
      @m.charset = "utf8"
      @m.query "create temporary table t (utf8 char(10) charset utf8, cp932 char(10) charset cp932, eucjp char(10) charset eucjpms, bin varbinary(10))"
      @utf8 = "いろは"
      @cp932 = @utf8.encode "CP932"
      @eucjp = @utf8.encode "EUC-JP-MS"
      @bin = "\x00\x01\x7F\x80\xFE\xFF".force_encoding("ASCII-8BIT")
      @default_internal = Encoding.default_internal
    end

    teardown do
      v =  $VERBOSE
      $VERBOSE = false
      Encoding.default_internal = @default_internal
      $VERBOSE = v
    end

    sub_test_case 'default_internal is CP932' do
      setup do
        @m.prepare("insert into t (utf8,cp932,eucjp,bin) values (?,?,?,?)").execute @utf8, @cp932, @eucjp, @bin
        v =  $VERBOSE
        $VERBOSE = false
        Encoding.default_internal = 'CP932'
        $VERBOSE = v
      end
      test 'is converted to CP932' do
        assert @m.query('select "あいう"').fetch == ["\x82\xA0\x82\xA2\x82\xA4".force_encoding("CP932")]
      end
      test 'data is stored as is' do
        assert @m.query('select hex(utf8),hex(cp932),hex(eucjp),hex(bin) from t').fetch == ['E38184E3828DE381AF', '82A282EB82CD', 'A4A4A4EDA4CF', '00017F80FEFF']
      end
      test 'By simple query, charset of retrieved data is connection charset' do
        assert @m.query('select utf8,cp932,eucjp,bin from t').fetch == [@cp932, @cp932, @cp932, @bin]
      end
      test 'By prepared statement, charset of retrieved data is connection charset except for binary' do
        assert @m.prepare('select utf8,cp932,eucjp,bin from t').execute.fetch == [@cp932, @cp932, @cp932, @bin]
      end
    end

    sub_test_case 'query with CP932 encoding' do
      test 'is converted to UTF-8' do
        assert @m.query('select HEX("あいう")'.encode("CP932")).fetch == ["E38182E38184E38186"]
      end
    end

    sub_test_case 'prepared statement with CP932 encoding' do
      test 'is converted to UTF-8' do
        assert @m.prepare('select HEX("あいう")'.encode("CP932")).execute.fetch == ["E38182E38184E38186"]
      end
    end

    sub_test_case 'The encoding of data are correspond to charset of column:' do
      setup do
        @m.prepare("insert into t (utf8,cp932,eucjp,bin) values (?,?,?,?)").execute @utf8, @cp932, @eucjp, @bin
      end
      test 'data is stored as is' do
        assert{ @m.query('select hex(utf8),hex(cp932),hex(eucjp),hex(bin) from t').fetch == ['E38184E3828DE381AF', '82A282EB82CD', 'A4A4A4EDA4CF', '00017F80FEFF'] }
      end
      test 'By simple query, charset of retrieved data is connection charset' do
        assert{ @m.query('select utf8,cp932,eucjp,bin from t').fetch == [@utf8, @utf8, @utf8, @bin] }
      end
      test 'By prepared statement, charset of retrieved data is connection charset except for binary' do
        assert{ @m.prepare('select utf8,cp932,eucjp,bin from t').execute.fetch == [@utf8, @utf8, @utf8, @bin] }
      end
    end

    sub_test_case 'The encoding of data are different from charset of column:' do
      setup do
        @m.prepare("insert into t (utf8,cp932,eucjp,bin) values (?,?,?,?)").execute @utf8, @utf8, @utf8, @utf8
      end
      test 'stored data is converted' do
        assert{ @m.query("select hex(utf8),hex(cp932),hex(eucjp),hex(bin) from t").fetch == ["E38184E3828DE381AF", "82A282EB82CD", "A4A4A4EDA4CF", "E38184E3828DE381AF"] }
      end
      test 'By simple query, charset of retrieved data is connection charset' do
        assert{ @m.query("select utf8,cp932,eucjp,bin from t").fetch == [@utf8, @utf8, @utf8, @utf8.dup.force_encoding('ASCII-8BIT')] }
      end
      test 'By prepared statement, charset of retrieved data is connection charset except for binary' do
        assert{ @m.prepare("select utf8,cp932,eucjp,bin from t").execute.fetch == [@utf8, @utf8, @utf8, @utf8.dup.force_encoding("ASCII-8BIT")] }
      end
    end

    sub_test_case 'The data include invalid byte code:' do
      test 'raises Encoding::InvalidByteSequenceError' do
        cp932 = "\x01\xFF\x80".force_encoding("CP932")
        assert_raise Encoding::InvalidByteSequenceError do
          @m.prepare("insert into t (cp932) values (?)").execute cp932
        end
      end
    end
  end

  test 'connect_attrs' do
    m = Mysql.connect(MYSQL_SERVER, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT, MYSQL_SOCKET, connect_attrs: {hoge: 'fuga'})
    if m.server_version >= 50600
      h = m.query("select * from performance_schema.session_connect_attrs where processlist_id=connection_id()").fetch_hash
      assert{ h['ATTR_NAME'] == 'hoge' && h['ATTR_VALUE'] == 'fuga' }
    end
  end

  test 'disconnect from server' do
    m = Mysql.connect(MYSQL_SERVER, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT, MYSQL_SOCKET)
    m.query('kill connection_id()') rescue nil
    e = assert_raise(Mysql::ClientError::ServerLost){ m.query('select 1') }
    assert{ e.message == 'Lost connection to server during query' }
  end

  test 'disconnect from client' do
    m = Mysql.connect(MYSQL_SERVER, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT, MYSQL_SOCKET)
    m.close
    e = assert_raise(Mysql::ClientError){ m.query('select 1') }
    assert{ e.message == 'MySQL client is not connected' }
  end
end
