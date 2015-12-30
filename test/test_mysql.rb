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
      assert{ Mysql::VERSION == 20913 }
    end
  end

  sub_test_case 'Mysql.init' do
    test 'returns Mysql object' do
      assert{ Mysql.init.kind_of? Mysql }
    end
  end

  sub_test_case 'Mysql.real_connect' do
    test 'connect to mysqld' do
      @m = Mysql.real_connect(MYSQL_SERVER, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT, MYSQL_SOCKET)
      assert{ @m.kind_of? Mysql }
    end

    test 'flag argument affects' do
      @m = Mysql.real_connect(MYSQL_SERVER, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT, MYSQL_SOCKET, Mysql::CLIENT_FOUND_ROWS)
      @m.query 'create temporary table t (c int)'
      @m.query 'insert into t values (123)'
      @m.query 'update t set c=123'
      assert{ @m.affected_rows == 1 }
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

    teardown do
      @m.close if @m
    end
  end

  sub_test_case 'Mysql.new' do
    test 'connect to mysqld' do
      @m = Mysql.new(MYSQL_SERVER, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT, MYSQL_SOCKET)
      assert{ @m.kind_of? Mysql }
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

  sub_test_case 'Mysql.client_info' do
    test 'returns client version as string' do
      assert{ Mysql.client_info == '5.0.0' }
    end
  end

  sub_test_case 'Mysql.get_client_info' do
    test 'returns client version as string' do
      assert{ Mysql.get_client_info == '5.0.0' }
    end
  end

  sub_test_case 'Mysql.client_version' do
    test 'returns client version as Integer' do
      assert{ Mysql.client_version == 50000 }
    end
  end

  sub_test_case 'Mysql.get_client_version' do
    test 'returns client version as Integer' do
      assert{ Mysql.client_version == 50000 }
    end
  end

  sub_test_case 'Mysql#real_connect' do
    test 'connect to mysqld' do
      @m = Mysql.init
      assert{ @m.real_connect(MYSQL_SERVER, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT, MYSQL_SOCKET) == @m }
    end
    teardown do
      @m.close if @m
    end
  end

  sub_test_case 'Mysql#connect' do
    test 'connect to mysqld' do
      @m = Mysql.init
      assert{ @m.connect(MYSQL_SERVER, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT, MYSQL_SOCKET) == @m }
    end
    teardown do
      @m.close if @m
    end
  end

  sub_test_case 'Mysql#options' do
    setup do
      @m = Mysql.init
    end
    teardown do
      @m.close
    end
    test 'INIT_COMMAND: execute query when connecting' do
      assert{ @m.options(Mysql::INIT_COMMAND, "SET AUTOCOMMIT=0") == @m }
      assert{ @m.connect(MYSQL_SERVER, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT, MYSQL_SOCKET) == @m }
      assert{ @m.query('select @@AUTOCOMMIT').fetch_row == ["0"] }
    end
    test 'OPT_CONNECT_TIMEOUT: set timeout for connecting' do
      assert{ @m.options(Mysql::OPT_CONNECT_TIMEOUT, 0.1) == @m }
      stub(UNIXSocket).new{ sleep 1}
      stub(TCPSocket).new{ sleep 1}
      assert_raise Mysql::ClientError, 'connection timeout' do
        @m.connect(MYSQL_SERVER, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT, MYSQL_SOCKET)
      end
      assert_raise Mysql::ClientError, 'connection timeout' do
        @m.connect
      end
    end
    test 'OPT_LOCAL_INFILE: client can execute LOAD DATA LOCAL INFILE query' do
      require 'tempfile'
      tmpf = Tempfile.new 'mysql_spec'
      tmpf.puts "123\tabc\n"
      tmpf.close
      assert{ @m.options(Mysql::OPT_LOCAL_INFILE, true) == @m }
      @m.connect(MYSQL_SERVER, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT, MYSQL_SOCKET)
      @m.query('create temporary table t (i int, c char(10))')
      @m.query("load data local infile '#{tmpf.path}' into table t")
      assert{ @m.query('select * from t').fetch_row == ['123','abc'] }
    end
    test 'OPT_READ_TIMEOUT: set timeout for reading packet' do
      assert{ @m.options(Mysql::OPT_READ_TIMEOUT, 10) == @m }
    end
    test 'OPT_WRITE_TIMEOUT: set timeout for writing packet' do
      assert{ @m.options(Mysql::OPT_WRITE_TIMEOUT, 10) == @m }
    end
    test 'SET_CHARSET_NAME: set charset for connection' do
      assert{ @m.options(Mysql::SET_CHARSET_NAME, 'utf8') == @m }
      @m.connect(MYSQL_SERVER, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT, MYSQL_SOCKET)
      assert{ @m.query('select @@character_set_connection').fetch_row == ['utf8'] }
    end
  end

  sub_test_case 'Mysql' do
    setup do
      @m = Mysql.new(MYSQL_SERVER, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT, MYSQL_SOCKET)
    end

    teardown do
      @m.close if @m rescue nil
    end

    sub_test_case '#escape_string' do
      if defined? ::Encoding
        test 'escape special character for charset' do
          @m.charset = 'cp932'
          assert{ @m.escape_string("abc'def\"ghi\0jkl%mno_表".encode('cp932')) == "abc\\'def\\\"ghi\\0jkl%mno_表".encode('cp932') }
        end
      else
        test 'raise error if charset is multibyte' do
          @m.charset = 'cp932'
          assert_raise Mysql::ClientError, 'Mysql#escape_string is called for unsafe multibyte charset' do
            @m.escape_string("abc'def\"ghi\0jkl%mno_\x95\\")
          end
        end
        test 'not warn if charset is singlebyte' do
          @m.charset = 'latin1'
          assert{ @m.escape_string("abc'def\"ghi\0jkl%mno_\x95\\") == "abc\\'def\\\"ghi\\0jkl%mno_\x95\\\\" }
        end
      end
    end

    sub_test_case '#quote' do
      test 'is alias of #escape_string' do
        assert{ @m.method(:quote) == @m.method(:escape_string) }
      end
    end

    sub_test_case '#client_info' do
      test 'returns client version as string' do
        assert{ @m.client_info == '5.0.0' }
      end
    end

    sub_test_case '#get_client_info' do
      test 'returns client version as string' do
        assert{ @m.get_client_info == '5.0.0' }
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
        m = Mysql.init
        m.options Mysql::SET_CHARSET_NAME, 'cp932'
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

    sub_test_case '#client_version' do
      test 'returns client version as Integer' do
        assert{ @m.client_version.kind_of? Integer }
      end
    end

    sub_test_case '#get_client_version' do
      test 'returns client version as Integer' do
        assert{ @m.get_client_version.kind_of? Integer }
      end
    end

    sub_test_case '#get_host_info' do
      test 'returns connection type as String' do
        if MYSQL_SERVER == nil or MYSQL_SERVER == 'localhost'
          assert{ @m.get_host_info == 'Localhost via UNIX socket' }
        else
          assert{ @m.get_host_info == "#{MYSQL_SERVER} via TCP/IP" }
        end
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

    sub_test_case '#get_proto_info' do
      test 'returns version of connection as Integer' do
        assert{ @m.get_proto_info == 10 }
      end
    end

    sub_test_case '#proto_info' do
      test 'returns version of connection as Integer' do
        assert{ @m.proto_info == 10 }
      end
    end

    sub_test_case '#get_server_info' do
      test 'returns server version as String' do
        assert{ @m.get_server_info =~ /\A\d+\.\d+\.\d+/ }
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
        @m2 = Mysql.new(MYSQL_SERVER, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT, MYSQL_SOCKET)
      end
      teardown do
        @m2.close rescue nil
      end
      test 'returns self' do
        assert{ @m.kill(@m2.thread_id) == @m }
      end
    end

    sub_test_case '#list_dbs' do
      test 'returns database list' do
        ret = @m.list_dbs
        assert{ ret.kind_of? Array }
        assert{ ret.include? MYSQL_DATABASE }
      end
      test 'with pattern returns databases that matches pattern' do
        assert{ @m.list_dbs('info%').include? 'information_schema' }
      end
    end

    sub_test_case '#list_fields' do
      setup do
        @m.query 'create temporary table t (i int, c char(10), d date)'
      end
      test 'returns result set that contains information of fields' do
        ret = @m.list_fields('t')
        assert{ ret.kind_of? Mysql::Result }
        assert{ ret.num_rows == 0 }
        assert{ ret.fetch_fields.map{|f|f.name} == ['i','c','d'] }
      end
      test 'with pattern returns result set that contains information of fields that matches pattern' do
        ret = @m.list_fields('t', 'i')
        assert{ ret.kind_of? Mysql::Result }
        assert{ ret.num_rows == 0 }
        ret.fetch_fields.map{|f|f.name} == ['i']
      end
    end

    sub_test_case '#list_processes' do
      test 'returns result set that contains information of all connections' do
        ret = @m.list_processes
        assert{ ret.kind_of? Mysql::Result }
        assert{ ret.find{|r|r[0].to_i == @m.thread_id}[4] == "Processlist" }
      end
    end

    sub_test_case '#list_tables' do
      setup do
        @m.query 'create table test_mysql_list_tables (id int)'
      end
      teardown do
        @m.query 'drop table if exists test_mysql_list_tables'
      end
      test 'returns table list' do
        ret = @m.list_tables
        assert{ ret.kind_of? Array }
        assert{ ret.include? 'test_mysql_list_tables' }
      end
      test 'with pattern returns lists that matches pattern' do
        ret = @m.list_tables '%mysql\_list\_t%'
        assert{ ret.include? 'test_mysql_list_tables' }
      end
    end

    sub_test_case '#ping' do
      test 'returns self' do
        assert{ @m.ping == @m }
      end
    end

    sub_test_case '#query' do
      test 'returns Mysql::Result if query returns results' do
        assert{ @m.query('select 123').kind_of? Mysql::Result }
      end
      test 'returns nil if query returns no results' do
        assert{ @m.query('set @hoge:=123') == nil }
      end
      test 'returns self if query_with_result is false' do
        @m.query_with_result = false
        assert{ @m.query('select 123') == @m }
        @m.store_result
        assert{ @m.query('set @hoge:=123') == @m }
      end
    end

    sub_test_case '#real_query' do
      test 'is same as #query' do
        assert{ @m.real_query('select 123').kind_of? Mysql::Result }
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

    sub_test_case '#store_result' do
      test 'returns Mysql::Result' do
        @m.query_with_result = false
        @m.query 'select 1,2,3'
        ret = @m.store_result
        assert{ ret.kind_of? Mysql::Result }
        assert{ ret.fetch_row == ['1','2','3'] }
      end
      test 'raises error when no query' do
        assert_raise Mysql::ClientError, 'invalid usage' do
          @m.store_result
        end
      end
      test 'raises error when query does not return results' do
        @m.query 'set @hoge:=123'
        assert_raise Mysql::ClientError, 'invalid usage' do
          @m.store_result
        end
      end
    end

    sub_test_case '#thread_id' do
      test 'returns thread id as Integer' do
        assert{ @m.thread_id.kind_of? Integer }
      end
    end

    sub_test_case '#use_result' do
      test 'returns Mysql::Result' do
        @m.query_with_result = false
        @m.query 'select 1,2,3'
        ret = @m.use_result
        assert{ ret.kind_of? Mysql::Result }
        assert{ ret.fetch_row == ['1','2','3'] }
      end
      test 'raises error when no query' do
        assert_raise Mysql::ClientError, 'invalid usage' do
          @m.use_result
        end
      end
      test 'raises error when query does not return results' do
        @m.query 'set @hoge:=123'
        assert_raise Mysql::ClientError, 'invalid usage' do
          @m.use_result
        end
      end
    end

    sub_test_case '#get_server_version' do
      test 'returns server version as Integer' do
        assert{ @m.get_server_version.kind_of? Integer }
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
        assert{ @m.set_server_option(Mysql::OPTION_MULTI_STATEMENTS_ON) == @m }
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

    sub_test_case '#query_with_result' do
      test 'default value is true' do
        assert{ @m.query_with_result == true }
      end
      test 'can set value' do
        assert{ (@m.query_with_result=true) == true }
        assert{ @m.query_with_result == true }
        assert{ (@m.query_with_result=false) == false }
        assert{ @m.query_with_result == false }
      end
    end

    sub_test_case '#query_with_result is false' do
      test 'Mysql#query returns self and Mysql#store_result returns result set' do
        @m.query_with_result = false
        assert{ @m.query('select 1,2,3') == @m }
        res = @m.store_result
        assert{ res.fetch_row == ['1','2','3'] }
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
        expect = [["1"], ["2"]]
        assert{ @m.query('select 1; set @hoge:=1; select 2'){|res|
            assert{ res.fetch_row == expect.shift }
            cnt += 1
          } == @m }
        assert{ cnt == 2 }
      end
    end
  end

  sub_test_case 'multiple statement query:' do
    setup do
      @m = Mysql.new(MYSQL_SERVER, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT, MYSQL_SOCKET)
      @m.set_server_option(Mysql::OPTION_MULTI_STATEMENTS_ON)
      @res = @m.query 'select 1,2; select 3,4,5'
    end
    test 'Mysql#query returns results for first query' do
      assert{ @res.entries == [['1','2']] }
    end
    test 'Mysql#more_results is true' do
      assert{ @m.more_results == true }
    end
    test 'Mysql#more_results? is true' do
      assert{ @m.more_results? == true }
    end
    test 'Mysql#next_result is true' do
      assert{ @m.next_result == true }
    end
    sub_test_case 'for next query:'  do
      setup do
        @m.next_result
        @res = @m.store_result
      end
      test 'Mysql#store_result returns results' do
        assert{ @res.entries == [['3','4','5']] }
      end
      test 'Mysql#more_results is false' do
        assert{ @m.more_results == false }
      end
      test 'Mysql#more_results? is false' do
        assert{ @m.more_results? == false }
      end
      test 'Mysql#next_result is false' do
        assert{ @m.next_result == false }
      end
    end
  end

  sub_test_case 'Mysql::Result' do
    setup do
      @m = Mysql.new(MYSQL_SERVER, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT, MYSQL_SOCKET)
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

    test '#fetch_field return current field' do
      f = @res.fetch_field
      assert{ f.name == 'id' }
      assert{ f.table == 't' }
      assert{ f.def == nil }
      assert{ f.type == Mysql::Field::TYPE_LONG }
      assert{ f.length == 11 }
      assert{ f.max_length == 1 }
      assert{ f.flags == Mysql::Field::NUM_FLAG|Mysql::Field::PRI_KEY_FLAG|Mysql::Field::PART_KEY_FLAG|Mysql::Field::NOT_NULL_FLAG }
      assert{ f.decimals == 0 }

      f = @res.fetch_field
      assert{ f.name == 'str' }
      assert{ f.table == 't' }
      assert{ f.def == nil }
      assert{ f.type == Mysql::Field::TYPE_STRING }
      assert{ f.length == 10 }
      assert{ f.max_length == 4 }
      assert{ f.flags == 0 }
      assert{ f.decimals == 0 }

      assert{ @res.fetch_field == nil }
    end

    test '#fetch_fields returns array of fields' do
      ret = @res.fetch_fields
      assert{ ret.size == 2 }
      assert{ ret[0].name == 'id' }
      assert{ ret[1].name == 'str' }
    end

    test '#fetch_field_direct returns field' do
      f = @res.fetch_field_direct 0
      assert{ f.name == 'id' }
      f = @res.fetch_field_direct 1
      assert{ f.name == 'str' }
      assert_raise Mysql::ClientError, 'invalid argument: -1' do
        @res.fetch_field_direct(-1)
      end
      assert_raise Mysql::ClientError, 'invalid argument: 2' do
        @res.fetch_field_direct 2
      end
    end

    test '#fetch_lengths returns array of length of field data' do
      assert{ @res.fetch_lengths == nil }
      @res.fetch_row
      assert{ @res.fetch_lengths == [1, 3] }
      @res.fetch_row
      assert{ @res.fetch_lengths == [1, 4] }
      @res.fetch_row
      assert{ @res.fetch_lengths == [1, 2] }
      @res.fetch_row
      assert{ @res.fetch_lengths == [1, 0] }
      @res.fetch_row
      assert{ @res.fetch_lengths == nil }
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

    test '#num_fields returns number of fields' do
      assert{ @res.num_fields == 2 }
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

    test '#row_tell returns position of current record, #row_seek set position of current record' do
      assert{ @res.fetch_row == ['1', 'abc'] }
      pos = @res.row_tell
      assert{ @res.fetch_row == ['2', 'defg'] }
      assert{ @res.fetch_row == ['3', 'hi'] }
      @res.row_seek pos
      assert{ @res.fetch_row == ['2', 'defg'] }
    end

    test '#field_tell returns position of current field, #field_seek set position of current field' do
      assert{ @res.field_tell == 0 }
      @res.fetch_field
      assert{ @res.field_tell == 1 }
      @res.fetch_field
      assert{ @res.field_tell == 2 }
      @res.field_seek 1
      assert{ @res.field_tell == 1 }
    end

    test '#free returns nil' do
      assert{ @res.free == nil }
    end
  end

  sub_test_case 'Mysql::Field' do
    setup do
      @m = Mysql.new(MYSQL_SERVER, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT, MYSQL_SOCKET)
      @m.charset = 'latin1'
      @m.query 'create temporary table t (id int default 0, str char(10), primary key (id))'
      @m.query "insert into t values (1,'abc'),(2,'defg'),(3,'hi'),(4,null)"
      @res = @m.query 'select * from t'
    end

    teardown do
      @m.close if @m
    end

    test '#name is name of field' do
      assert{ @res.fetch_field.name == 'id' }
    end

    test '#table is name of table for field' do
      assert{ @res.fetch_field.table == 't' }
    end

    test '#def for result set is null' do
      assert{ @res.fetch_field.def == nil }
    end

    test '#def for field information is default value' do
      assert{ @m.list_fields('t').fetch_field.def == '0' }
    end

    test '#type is type of field as Integer' do
      assert{ @res.fetch_field.type == Mysql::Field::TYPE_LONG }
      assert{ @res.fetch_field.type == Mysql::Field::TYPE_STRING }
    end

    test '#length is length of field' do
      assert{ @res.fetch_field.length == 11 }
      assert{ @res.fetch_field.length == 10 }
    end

    test '#max_length is maximum length of field value' do
      assert{ @res.fetch_field.max_length == 1 }
      assert{ @res.fetch_field.max_length == 4 }
    end

    test '#flags is flag of field as Integer' do
      assert{ @res.fetch_field.flags == Mysql::Field::NUM_FLAG|Mysql::Field::PRI_KEY_FLAG|Mysql::Field::PART_KEY_FLAG|Mysql::Field::NOT_NULL_FLAG }
      assert{ @res.fetch_field.flags == 0 }
    end

    test '#decimals is number of decimal digits' do
      assert{ @m.query('select 1.23').fetch_field.decimals == 2 }
    end

    test '#hash return field as hash' do
      assert{ @res.fetch_field.hash == {
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
      assert{ @res.fetch_field.hash == {
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
      assert{ @res.fetch_field.inspect == '#<Mysql::Field:id>' }
      assert{ @res.fetch_field.inspect == '#<Mysql::Field:str>' }
    end

    test '#is_num? returns true if the field is numeric' do
      assert{ @res.fetch_field.is_num? == true }
      assert{ @res.fetch_field.is_num? == false }
    end

    test '#is_not_null? returns true if the field is not null' do
      assert{ @res.fetch_field.is_not_null? == true }
      assert{ @res.fetch_field.is_not_null? == false }
    end

    test '#is_pri_key? returns true if the field is primary key' do
      assert{ @res.fetch_field.is_pri_key? == true }
      assert{ @res.fetch_field.is_pri_key? == false }
    end
  end

  sub_test_case 'create Mysql::Stmt object:' do
    setup do
      @m = Mysql.new(MYSQL_SERVER, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT, MYSQL_SOCKET)
    end

    teardown do
      @m.close if @m
    end

    test 'Mysql#stmt_init returns Mysql::Stmt object' do
      assert{ @m.stmt_init.kind_of? Mysql::Stmt }
    end

    test 'Mysq;#prepare returns Mysql::Stmt object' do
      assert{ @m.prepare("select 1").kind_of? Mysql::Stmt }
    end
  end

  sub_test_case 'Mysql::Stmt' do
    setup do
      @m = Mysql.new(MYSQL_SERVER, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT, MYSQL_SOCKET)
      @m.query("set sql_mode=''")
      @s = @m.stmt_init
    end

    teardown do
      @s.close if @s rescue nil
      @m.close if @m
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

    sub_test_case '#bind_result' do
      setup do
        @m.query 'create temporary table t (i int, c char(10), d double, t datetime)'
        @m.query 'insert into t values (123,"9abcdefg",1.2345,20091208100446)'
        @s.prepare 'select * from t'
      end

      test '(nil) make result format to be standard value' do
        @s.bind_result nil, nil, nil, nil
        @s.execute
        assert{ @s.fetch == [123, '9abcdefg', 1.2345, Mysql::Time.new(2009,12,8,10,4,46)] }
      end

      test '(Numeric) make result format to be Integer value' do
        @s.bind_result Numeric, Numeric, Numeric, Numeric
        @s.execute
        assert{ @s.fetch == [123, 9, 1, 20091208100446] }
      end

      test '(Integer) make result format to be Integer value' do
        @s.bind_result Integer, Integer, Integer, Integer
        @s.execute
        assert{ @s.fetch == [123, 9, 1, 20091208100446] }
      end

      test '(Fixnum) make result format to be Integer value' do
        @s.bind_result Fixnum, Fixnum, Fixnum, Fixnum
        @s.execute
        assert{ @s.fetch == [123, 9, 1, 20091208100446] }
      end

      test '(String) make result format to be String value' do
        @s.bind_result String, String, String, String
        @s.execute
        assert{ @s.fetch == ["123", "9abcdefg", "1.2345", "2009-12-08 10:04:46"] }
      end

      test '(Float) make result format to be Float value' do
        @s.bind_result Float, Float, Float, Float
        @s.execute
        assert{ @s.fetch == [123.0, 9.0, 1.2345 , 20091208100446.0] }
      end

      test '(Mysql::Time) make result format to be Mysql::Time value' do
        @s.bind_result Mysql::Time, Mysql::Time, Mysql::Time, Mysql::Time
        @s.execute
        assert{ @s.fetch == [Mysql::Time.new(2000,1,23), Mysql::Time.new, Mysql::Time.new, Mysql::Time.new(2009,12,8,10,4,46)] }
      end

      test '(invalid) raises error' do
        assert_raise TypeError do
          @s.bind_result(Time, nil, nil, nil)
        end
      end

      test 'with mismatch argument count raise error' do
        assert_raise Mysql::ClientError, 'bind_result: result value count(4) != number of argument(1)' do
          @s.bind_result(nil)
        end
      end
    end

    test '#close returns nil' do
      assert{ @s.close == nil }
    end

    test '#data_seek set position of current record' do
      @m.query 'create temporary table t (i int)'
      @m.query 'insert into t values (0),(1),(2),(3),(4),(5),(6)'
      @s.prepare 'select i from t'
      @s.execute
      assert{ @s.fetch == [0] }
      assert{ @s.fetch == [1] }
      assert{ @s.fetch == [2] }
      @s.data_seek 5
      assert{ @s.fetch == [5] }
      @s.data_seek 1
      assert{ @s.fetch == [1] }
    end

    test '#each iterate block with a record' do
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
        assert{ a == expect.shift }
      end
    end

    test '#execute returns self' do
      @s.prepare 'select 1'
      assert{ @s.execute == @s }
    end

    test '#execute pass arguments to query' do
      @m.query 'create temporary table t (i int)'
      @s.prepare 'insert into t values (?)'
      @s.execute 123
      @s.execute '456'
      assert{ @m.query('select * from t').entries == [['123'], ['456']] }
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
      [30, 31, 32, 62, 63].each do |i|
        assert{ @m.prepare('select cast(? as signed)').execute(2**i-1).fetch == [2**i-1] }
        assert{ @m.prepare('select cast(? as signed)').execute(-(2**i)).fetch == [-2**i] }
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
      if defined? Encoding
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
      else
        assert{ @s.entries == [["\x00"], ["\xff"], ["\x7f"], ["\xff"], ["\xff"], ["\xff"], ["\xff"]] }
      end
    end

    test '#fetch bit column (64bit)' do
      @m.query 'create temporary table t (i bit(64))'
      @m.query 'insert into t values (0),(-1),(4294967296),(18446744073709551615),(18446744073709551616)'
      @s.prepare 'select i from t'
      @s.execute
      if defined? Encoding
        assert{ @s.entries == [
            ["\x00\x00\x00\x00\x00\x00\x00\x00".force_encoding('ASCII-8BIT')],
            ["\xff\xff\xff\xff\xff\xff\xff\xff".force_encoding('ASCII-8BIT')],
            ["\x00\x00\x00\x01\x00\x00\x00\x00".force_encoding('ASCII-8BIT')],
            ["\xff\xff\xff\xff\xff\xff\xff\xff".force_encoding('ASCII-8BIT')],
            ["\xff\xff\xff\xff\xff\xff\xff\xff".force_encoding('ASCII-8BIT')],
          ]
        }
      else
        assert{ @s.entries == [
            ["\x00\x00\x00\x00\x00\x00\x00\x00"],
            ["\xff\xff\xff\xff\xff\xff\xff\xff"],
            ["\x00\x00\x00\x01\x00\x00\x00\x00"],
            ["\xff\xff\xff\xff\xff\xff\xff\xff"],
            ["\xff\xff\xff\xff\xff\xff\xff\xff"],
          ]
        }
      end
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
      @m.query 'create temporary table t (i decimal)'
      @m.query 'insert into t values (0),(9999999999),(-9999999999),(10000000000),(-10000000000)'
      @s.prepare 'select i from t'
      @s.execute
      assert{ @s.entries == [["0"], ["9999999999"], ["-9999999999"], ["9999999999"], ["-9999999999"]] }
    end

    test '#fetch decimal unsigned column' do
      @m.query 'create temporary table t (i decimal unsigned)'
      @m.query 'insert into t values (0),(9999999998),(9999999999),(-9999999998),(-9999999999),(10000000000),(-10000000000)'
      @s.prepare 'select i from t'
      @s.execute
      assert{ @s.entries == [["0"], ["9999999998"], ["9999999999"], ["0"], ["0"], ["9999999999"], ["0"]] }
    end

    test '#fetch date column' do
      @m.query 'create temporary table t (i date)'
      @m.query "insert into t values ('0000-00-00'),('1000-01-01'),('9999-12-31')"
      @s.prepare 'select i from t'
      @s.execute
      cols = @s.fetch
      assert{ cols == [Mysql::Time.new] }
      assert{ cols.first.to_s == '0000-00-00' }
      cols = @s.fetch
      assert{ cols == [Mysql::Time.new(1000,1,1)] }
      assert{ cols.first.to_s == '1000-01-01' }
      cols = @s.fetch
      assert{ cols == [Mysql::Time.new(9999,12,31)] }
      assert{ cols.first.to_s == '9999-12-31' }
    end

    test '#fetch datetime column' do
      @m.query 'create temporary table t (i datetime)'
      @m.query "insert into t values ('0000-00-00 00:00:00'),('1000-01-01 00:00:00'),('9999-12-31 23:59:59')"
      @s.prepare 'select i from t'
      @s.execute
      assert{ @s.fetch == [Mysql::Time.new] }
      assert{ @s.fetch == [Mysql::Time.new(1000,1,1)] }
      assert{ @s.fetch == [Mysql::Time.new(9999,12,31,23,59,59)] }
    end

    test '#fetch timestamp column' do
      @m.query 'create temporary table t (i timestamp)'
      @m.query("insert into t values ('1970-01-02 00:00:00'),('2037-12-30 23:59:59')")
      @s.prepare 'select i from t'
      @s.execute
      assert{ @s.fetch == [Mysql::Time.new(1970,1,2)] }
      assert{ @s.fetch == [Mysql::Time.new(2037,12,30,23,59,59)] }
    end

    test '#fetch time column' do
      @m.query 'create temporary table t (i time)'
      @m.query "insert into t values ('-838:59:59'),(0),('838:59:59')"
      @s.prepare 'select i from t'
      @s.execute
      assert{ @s.fetch == [Mysql::Time.new(0,0,0,838,59,59,true)] }
      assert{ @s.fetch == [Mysql::Time.new(0,0,0,0,0,0,false)] }
      assert{ @s.fetch == [Mysql::Time.new(0,0,0,838,59,59,false)] }
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

    test '#insert_id' do
      @m.query 'create temporary table t (i int auto_increment, unique(i))'
      @s.prepare 'insert into t values (0)'
      @s.execute
      assert{ @s.insert_id == 1 }
      @s.execute
      assert{ @s.insert_id == 2 }
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

  sub_test_case 'Mysql::Time' do
    setup do
      @t = Mysql::Time.new
    end

    test '.new with no arguments returns 0' do
      assert{ @t.year == 0 }
      assert{ @t.month == 0 }
      assert{ @t.day == 0 }
      assert{ @t.hour == 0 }
      assert{ @t.minute == 0 }
      assert{ @t.second == 0 }
      assert{ @t.neg == false }
      assert{ @t.second_part == 0 }
    end

    test '#inspect' do
      assert{ Mysql::Time.new(2009,12,8,23,35,21).inspect == '#<Mysql::Time:2009-12-08 23:35:21>' }
    end

    test '#to_s' do
      assert{ Mysql::Time.new(2009,12,8,23,35,21).to_s == '2009-12-08 23:35:21' }
    end

    test '#to_i' do
      assert{ Mysql::Time.new(2009,12,8,23,35,21).to_i == 20091208233521 }
    end

    test '#year' do
      assert{ (@t.year = 2009) == 2009 }
      assert{ @t.year == 2009 }
    end

    test '#month' do
      assert{ (@t.month = 12) == 12 }
      assert{ @t.month == 12 }
    end

    test '#day' do
      assert{ (@t.day = 8) == 8 }
      assert{ @t.day == 8 }
    end

    test '#hour' do
      assert{ (@t.hour = 23) == 23 }
      assert{ @t.hour == 23 }
    end

    test '#minute' do
      assert{ (@t.minute = 35) == 35 }
      assert{ @t.minute == 35 }
    end

    test '#second' do
      assert{ (@t.second = 21) == 21 }
      assert{ @t.second == 21 }
    end

    test '#neg' do
      assert{ @t.neg == false }
    end

    test '#second_part' do
      assert{ @t.second_part == 0 }
    end

    test '#==' do
      t1 = Mysql::Time.new 2009,12,8,23,35,21
      t2 = Mysql::Time.new 2009,12,8,23,35,21
      assert{ t1 == t2 }
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

  if defined? Encoding
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
        Encoding.default_internal = @default_internal
      end

      sub_test_case 'default_internal is CP932' do
        setup do
          @m.prepare("insert into t (utf8,cp932,eucjp,bin) values (?,?,?,?)").execute @utf8, @cp932, @eucjp, @bin
          Encoding.default_internal = 'CP932'
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
  end
end
