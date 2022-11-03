require 'spec_helper'

describe Mysql do
  it 'Mysql::VERSION returns client version' do
    assert{ Mysql::VERSION == '4.0.0' }
  end

  it 'Mysql.new returns Mysql object' do
    assert{ Mysql.new.kind_of? Mysql }
  end

  describe 'arguments' do
    after{ @m&.close }

    it 'with fixed arguments' do
      @m = Mysql.new('127.0.0.1', 'hoge', 'abc&def', 'test', 3306, '/tmp/socket', 12345)
      assert{ @m.host == '127.0.0.1' }
      assert{ @m.username == 'hoge' }
      assert{ @m.password == 'abc&def' }
      assert{ @m.database == 'test' }
      assert{ @m.port == 3306 }
      assert{ @m.socket == '/tmp/socket' }
      assert{ @m.flags == 12345 }
    end

    it 'with keyword arguments' do
      @m = Mysql.new(host: '127.0.0.1', username: 'hoge', password: 'abc&def', database: 'test', port: 3306, socket: '/tmp/socket', flags: 12345)
      assert{ @m.host == '127.0.0.1' }
      assert{ @m.username == 'hoge' }
      assert{ @m.password == 'abc&def' }
      assert{ @m.database == 'test' }
      assert{ @m.port == 3306 }
      assert{ @m.socket == '/tmp/socket' }
      assert{ @m.flags == 12345 }
    end

    it 'with URI' do
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

    it 'with URI string' do
      @m = Mysql.new("mysql://hoge:abc%26def@127.0.0.1:3306/test?socket=/tmp/socket&flags=12345")
      assert{ @m.host == '127.0.0.1' }
      assert{ @m.username == 'hoge' }
      assert{ @m.password == 'abc&def' }
      assert{ @m.database == 'test' }
      assert{ @m.port == 3306 }
      assert{ @m.socket == '/tmp/socket' }
      assert{ @m.flags == 12345 }
    end

    it 'with URI string: host is filename' do
      @m = Mysql.new("mysql://hoge:abc%26def@%2Ftmp%2Fsocket:3306/test?flags=12345")
      assert{ @m.host == '' }
      assert{ @m.username == 'hoge' }
      assert{ @m.password == 'abc&def' }
      assert{ @m.database == 'test' }
      assert{ @m.port == 3306 }
      assert{ @m.socket == '/tmp/socket' }
      assert{ @m.flags == 12345 }
    end
  end

  describe 'Mysql.connect' do
    after{ @m&.close }

    it 'connect to mysqld' do
      @m = Mysql.connect(MYSQL_SERVER, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT, MYSQL_SOCKET)
      assert{ @m.kind_of? Mysql }
    end

    it 'flag argument affects' do
      @m = Mysql.connect(MYSQL_SERVER, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT, MYSQL_SOCKET, Mysql::CLIENT_FOUND_ROWS)
      @m.query 'create temporary table t (c int)'
      @m.query 'insert into t values (123)'
      @m.query 'update t set c=123'
      assert{ @m.affected_rows == 1 }
    end
  end

  describe 'Mysql.escape_string' do
    it 'escape special character' do
      assert{ Mysql.escape_string("abc'def\"ghi\0jkl%mno") == "abc\\'def\\\"ghi\\0jkl%mno" }
    end
  end

  describe 'Mysql.quote' do
    it 'escape special character' do
      assert{ Mysql.quote("abc'def\"ghi\0jkl%mno") == "abc\\'def\\\"ghi\\0jkl%mno" }
    end
  end

  describe 'Mysql#connect' do
    after{ @m&.close }

    it 'connect to mysqld' do
      @m = Mysql.new(MYSQL_SERVER, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT, MYSQL_SOCKET)
      assert{ @m.connect == @m }
    end

    it 'connect to mysqld by URI' do
      @m = Mysql.new("mysql://#{MYSQL_USER}:#{MYSQL_PASSWORD}@#{MYSQL_SERVER}:#{MYSQL_PORT}/#{MYSQL_DATABASE}?socket=#{MYSQL_SOCKET}")
      assert{ @m.connect == @m }
    end

    it 'overrides arguments of new method' do
      @m = Mysql.new('example.com', 12345)
      @m.connect(MYSQL_SERVER, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT, MYSQL_SOCKET)
    end
  end

  describe 'options' do
    before do
      @m = Mysql.new
    end
    after do
      @m.close
    end
    it 'init_command: execute query when connecting' do
      @m.init_command = "SET AUTOCOMMIT=0"
      assert{ @m.connect(MYSQL_SERVER, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT, MYSQL_SOCKET) == @m }
      assert{ @m.query('select @@AUTOCOMMIT').fetch_row == [0] }
    end
    it 'connect_timeout: set timeout for connecting' do
      @m.connect_timeout = 0.1
      allow(Socket).to receive(:tcp) { raise Errno::ETIMEDOUT }
      allow(Socket).to receive(:unix) { raise Errno::ETIMEDOUT }
      expect {
        @m.connect(MYSQL_SERVER, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT, MYSQL_SOCKET)
      }.to raise_error Mysql::ClientError, 'connection timeout'
      expect {
        @m.connect
      }.to raise_error Mysql::ClientError, 'connection timeout'
    end
    it 'local_infile: client can execute LOAD DATA LOCAL INFILE query' do
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
      assert{ @m.query('select * from t').fetch_row == [123, 'abc'] }
    end
    it 'load_data_local_dir: client can execute LOAD DATA LOCAL INFILE query with specified directory' do
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
      assert{ @m.query('select * from t').fetch_row == [123, 'abc'] }
    end
    it 'load_data_local_dir: client cannot execute LOAD DATA LOCAL INFILE query without specified directory' do
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
      expect {
        @m.query("load data local infile '#{tmpf.path}' into table t")
      }.to raise_error Mysql::ClientError::LoadDataLocalInfileRejected, 'LOAD DATA LOCAL INFILE file request rejected due to restrictions on access.'
    end
    it 'without local_infile and load_data_local_dir: client cannot execute LOAD DATA LOCAL INFILE query' do
      require 'tempfile'
      tmpf = Tempfile.new 'mysql_spec'
      tmpf.puts "123\tabc\n"
      tmpf.close
      @m.connect(MYSQL_SERVER, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT, MYSQL_SOCKET)
      if @m.query('select @@local_infile').fetch[0] == '0'
        omit 'skip because local_infile variable is false'
      end
      @m.query('create temporary table t (i int, c char(10))')
      expect {
        @m.query("load data local infile '#{tmpf.path}' into table t")
      }.to raise_error Mysql::ClientError::LoadDataLocalInfileRejected, 'LOAD DATA LOCAL INFILE file request rejected due to restrictions on access.'
    end
    it 'read_timeout: set timeout for reading packet' do
      @m.read_timeout = 1
      @m.connect(MYSQL_SERVER, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT, MYSQL_SOCKET)
      @m.query("select 123").entries
    end
    it 'write_timeout: set timeout for writing packet' do
      @m.write_timeout = 1
      @m.connect(MYSQL_SERVER, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT, MYSQL_SOCKET)
      @m.query("select 123").entries
    end
    it 'charset: set charset for connection' do
      @m.charset = 'utf8mb3'
      @m.connect(MYSQL_SERVER, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT, MYSQL_SOCKET)
      assert do
        @m.query('select @@character_set_connection').fetch_row == ['utf8mb3'] ||
          @m.query('select @@character_set_connection').fetch_row == ['utf8']
      end
    end
  end

  describe 'Mysql' do
    before do
      @m = Mysql.connect(MYSQL_SERVER, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT, MYSQL_SOCKET)
    end

    after do
      @m&.close
    end

    describe '#escape_string' do
      it 'escape special character for charset' do
        @m.charset = 'cp932'
        assert{ @m.escape_string("abc'def\"ghi\0jkl%mno_表".encode('cp932')) == "abc\\'def\\\"ghi\\0jkl%mno_表".encode('cp932') }
      end
    end

    describe '#quote' do
      it 'is alias of #escape_string' do
        assert{ @m.method(:quote) == @m.method(:escape_string) }
      end
    end

    describe '#affected_rows' do
      it 'returns number of affected rows' do
        @m.query 'create temporary table t (id int)'
        @m.query 'insert into t values (1),(2)'
        assert{ @m.affected_rows == 2 }
      end
    end

    describe '#character_set_name' do
      it 'returns charset name' do
        m = Mysql.new
        m.charset = 'cp932'
        m.connect MYSQL_SERVER, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT, MYSQL_SOCKET
        assert{ m.character_set_name == 'cp932' }
      end
    end

    describe '#close' do
      it 'returns self' do
        assert{ @m.close == @m }
      end
    end

    describe '#close!' do
      it 'returns self' do
        assert{ @m.close! == @m }
      end
    end

    #  describe '#create_db' do
    #  end

    #  describe '#drop_db' do
    #  end

    describe '#errno' do
      it 'default value is 0' do
        assert{ @m.errno == 0 }
      end
      it 'returns error number of latest error' do
        @m.query('hogehoge') rescue nil
        assert{ @m.errno == 1064 }
      end
    end

    describe '#error' do
      it 'returns error message of latest error' do
        @m.query('hogehoge') rescue nil
        assert{ @m.error == "You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near 'hogehoge' at line 1" }
      end
    end

    describe '#field_count' do
      it 'returns number of fields for latest query' do
        @m.query 'select 1,2,3'
        assert{ @m.field_count == 3 }
      end
    end

    describe '#host_info' do
      it 'returns connection type as String' do
        if MYSQL_SERVER == nil or MYSQL_SERVER == 'localhost'
          assert{ @m.host_info == 'Localhost via UNIX socket' }
        else
          assert{ @m.host_info == "#{MYSQL_SERVER} via TCP/IP" }
        end
      end
    end

    describe '#server_info' do
      it 'returns server version as String' do
        assert{ @m.server_info =~ /\A\d+\.\d+\.\d+/ }
      end
    end

    describe '#info' do
      it 'returns information of latest query' do
        @m.query 'create temporary table t (id int)'
        @m.query 'insert into t values (1),(2),(3)'
        assert{ @m.info == 'Records: 3  Duplicates: 0  Warnings: 0' }
      end
    end

    describe '#insert_id' do
      it 'returns latest auto_increment value' do
        @m.query 'create temporary table t (id int auto_increment, unique (id))'
        @m.query 'insert into t values (0)'
        assert{ @m.insert_id == 1 }
        @m.query 'alter table t auto_increment=1234'
        @m.query 'insert into t values (0)'
        assert{ @m.insert_id == 1234 }
      end
    end

    describe '#kill' do
      before do
        @m2 = Mysql.connect(MYSQL_SERVER, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT, MYSQL_SOCKET)
      end
      after do
        @m2.close rescue nil
      end
      it 'returns self' do
        assert{ @m.kill(@m2.thread_id) == @m }
      end
    end

    describe '#ping' do
      it 'returns self' do
        assert{ @m.ping == @m }
      end
    end

    describe '#query' do
      before do
        @m.set_server_option(Mysql::OPTION_MULTI_STATEMENTS_ON)
      end
      it 'returns Mysql::Result if query returns results' do
        assert{ @m.query('select 123').kind_of? Mysql::Result }
      end
      it 'returns nil if query returns no results' do
        assert{ @m.query('set @hoge=123') == nil }
      end
      it 'returns self if block is specified' do
        assert{ @m.query('select 123'){} == @m }
      end
      it 'returns self if return_result is false' do
        assert{ @m.query('select 123', return_result: false) == @m }
        assert{ @m.store_result.entries == [[123]] }
      end
      it 'if return_result is false and query returns no result' do
        assert{ @m.query('set @hoge=123', return_result: false) == @m }
        assert{ @m.store_result == nil }
      end
      it 'if yield_null_result is true' do
        expects = [[[1]], nil, [[2]]]
        results = []
        @m.query('select 1; set @hoge=123; select 2', yield_null_result: true){|r| results.push r&.entries }
        assert{ results == expects }
      end
      it 'if yield_null_result is false' do
        expects = [[[1]], [[2]]]
        results = []
        @m.query('select 1; set @hoge=123; select 2', yield_null_result: false){|r| results.push r&.entries }
        assert{ results == expects }
      end
    end

    describe '#refresh' do
      it 'returns self' do
        assert{ @m.refresh(Mysql::REFRESH_HOSTS) == @m }
      end
    end

    describe '#reload' do
      it 'returns self' do
        assert{ @m.reload == @m }
      end
    end

    describe '#select_db' do
      it 'changes default database' do
        @m.select_db 'information_schema'
        assert{ @m.query('select database()').fetch_row.first == 'information_schema' }
      end
    end

    #  describe '#shutdown' do
    #  end

    describe '#stat' do
      it 'returns server status' do
        assert{ @m.stat =~ /\AUptime: \d+  Threads: \d+  Questions: \d+  Slow queries: \d+  Opens: \d+  Flush tables: \d+  Open tables: \d+  Queries per second avg: \d+\.\d+\z/ }
      end
    end

    describe '#thread_id' do
      it 'returns thread id as Integer' do
        assert{ @m.thread_id.kind_of? Integer }
      end
    end

    describe '#server_version' do
      it 'returns server version as Integer' do
        assert{ @m.server_version.kind_of? Integer }
      end
    end

    describe '#warning_count' do
      before do
        @m.query("set sql_mode=''")
        @m.query("set sql_mode=''")  # clear warnings on previous `set' statement.
      end
      it 'default values is zero' do
        assert{ @m.warning_count == 0 }
      end
      it 'returns number of warnings' do
        @m.query 'create temporary table t (i tinyint)'
        @m.query 'insert into t values (1234567)'
        assert{ @m.warning_count == 1 }
      end
    end

    describe '#commit' do
      it 'returns self' do
        assert{ @m.commit == @m }
      end
    end

    describe '#rollback' do
      it 'returns self' do
        assert{ @m.rollback == @m }
      end
    end

    describe '#autocommit' do
      it 'returns self' do
        assert{ @m.autocommit(true) == @m }
      end

      it 'change auto-commit mode' do
        @m.autocommit(true)
        assert{ @m.query('select @@autocommit').fetch_row == [1] }
        @m.autocommit(false)
        assert{ @m.query('select @@autocommit').fetch_row == [0] }
      end
    end

    describe '#set_server_option' do
      it 'returns self' do
        expect { @m.query('select 1; select 2'){} }.to raise_error Mysql::ServerError::ParseError
        assert{ @m.set_server_option(Mysql::OPTION_MULTI_STATEMENTS_ON) == @m }
        expect { @m.query('select 1; select 2'){} }.to_not raise_error
      end
    end

    describe '#sqlstate' do
      it 'default values is "00000"' do
        assert{ @m.sqlstate == "00000" }
      end
      it 'returns sqlstate code' do
        expect { @m.query("hoge") }.to raise_error Mysql::ServerError::ParseError
        assert{ @m.sqlstate == "42000" }
      end
    end

    describe '#query with block' do
      it 'returns self' do
        assert{ @m.query('select 1'){} == @m }
      end
      it 'evaluate block with Mysql::Result' do
        assert{ @m.query('select 1'){|res| res.kind_of? Mysql::Result} == @m }
      end
      it 'evaluate block multiple times if multiple query is specified' do
        @m.set_server_option Mysql::OPTION_MULTI_STATEMENTS_ON
        cnt = 0
        expect = [[1], [2]]
        assert{
          @m.query('select 1; select 2'){|res|
            assert{ res.fetch_row == expect.shift }
            cnt += 1
          } == @m
        }
        assert{ cnt == 2 }
      end
      it 'evaluate block only when query has result' do
        @m.set_server_option Mysql::OPTION_MULTI_STATEMENTS_ON
        cnt = 0
        expect = [[[1]], nil, [[2]]]
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

  it 'multiple statement query' do
    m = Mysql.connect(MYSQL_SERVER, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT, MYSQL_SOCKET)
    m.set_server_option(Mysql::OPTION_MULTI_STATEMENTS_ON)
    res = m.query 'select 1,2; select 3,4,5'
    assert{ res.entries == [[1, 2]] }
    assert{ m.more_results? == true }
    assert{ m.next_result.entries == [[3, 4, 5]] }
    assert{ m.more_results? == false }
    assert{ m.next_result == nil }
    m.close!
  end

  it 'multiple statement error' do
    m = Mysql.connect(MYSQL_SERVER, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT, MYSQL_SOCKET)
    m.set_server_option(Mysql::OPTION_MULTI_STATEMENTS_ON)
    res = m.query 'select 1; select hoge; select 2'
    assert{ res.entries == [[1]] }
    assert{ m.more_results? == true }
    expect { m.next_result }.to raise_error Mysql::ServerError::BadFieldError
    assert{ m.more_results? == false }
    m.close!
  end

  it 'procedure returns multiple results' do
    m = Mysql.connect(MYSQL_SERVER, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT, MYSQL_SOCKET)
    m.query 'drop procedure if exists test_proc'
    m.query 'create procedure test_proc() begin select 1 as a; select 2 as b; end'
    res = m.query 'call test_proc()'
    assert{ res.entries == [[1]] }
    assert{ m.more_results? == true }
    assert{ m.next_result.entries == [[2]] }
    assert{ m.more_results? == true }
    assert{ m.next_result == nil }
    assert{ m.more_results? == false }
  end

  it 'multiple statements includes no results statement' do
    m = Mysql.connect(MYSQL_SERVER, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT, MYSQL_SOCKET)
    m.set_server_option(Mysql::OPTION_MULTI_STATEMENTS_ON)
    m.query('create temporary table t (i int)')
    res = m.query 'select 1; insert into t values (1),(2),(3); select 2'
    assert{ res.entries == [[1]] }
    assert{ m.more_results? == true }
    assert{ m.next_result == nil }
    assert{ m.info == 'Records: 3  Duplicates: 0  Warnings: 0' }
    assert{ m.more_results? == true }
    assert{ m.next_result.entries == [[2]] }
    assert{ m.more_results? == false }
  end

  describe 'Mysql::Result' do
    before do
      @m = Mysql.connect(MYSQL_SERVER, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT, MYSQL_SOCKET)
      @m.charset = 'latin1'
      @m.query 'create temporary table t (id int default 0, str char(10), primary key (id))'
      @m.query "insert into t values (1,'abc'),(2,'defg'),(3,'hi'),(4,null)"
      @res = @m.query 'select * from t'
    end

    after do
      @m&.close
    end

    it '#data_seek set position of current record' do
      assert{ @res.fetch_row == [1, 'abc'] }
      assert{ @res.fetch_row == [2, 'defg'] }
      assert{ @res.fetch_row == [3, 'hi'] }
      @res.data_seek 1
      assert{ @res.fetch_row == [2, 'defg'] }
    end

    it '#fields returns array of field' do
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

    it '#fetch_fields returns array of fields' do
      ret = @res.fetch_fields
      assert{ ret.size == 2 }
      assert{ ret[0].name == 'id' }
      assert{ ret[1].name == 'str' }
    end

    it '#fetch_row returns one record as array for current record' do
      assert{ @res.fetch_row == [1, 'abc'] }
      assert{ @res.fetch_row == [2, 'defg'] }
      assert{ @res.fetch_row == [3, 'hi'] }
      assert{ @res.fetch_row == [4, nil] }
      assert{ @res.fetch_row == nil }
    end

    it '#fetch_hash returns one record as hash for current record' do
      assert{ @res.fetch_hash == {'id'=>1, 'str'=>'abc'} }
      assert{ @res.fetch_hash == {'id'=>2, 'str'=>'defg'} }
      assert{ @res.fetch_hash == {'id'=>3, 'str'=>'hi'} }
      assert{ @res.fetch_hash == {'id'=>4, 'str'=>nil} }
      assert{ @res.fetch_hash == nil }
    end

    it '#fetch_hash(true) returns with table name' do
      assert{ @res.fetch_hash(true) == {'t.id'=>1, 't.str'=>'abc'} }
      assert{ @res.fetch_hash(true) == {'t.id'=>2, 't.str'=>'defg'} }
      assert{ @res.fetch_hash(true) == {'t.id'=>3, 't.str'=>'hi'} }
      assert{ @res.fetch_hash(true) == {'t.id'=>4, 't.str'=>nil} }
      assert{ @res.fetch_hash(true) == nil }
    end

    it '#num_rows returns number of records' do
      assert{ @res.num_rows == 4 }
    end

    it '#each iterate block with a record' do
      expect = [[1, "abc"], [2, "defg"], [3, "hi"], [4, nil]]
      @res.each do |a|
        assert{ a == expect.shift }
      end
    end

    it '#each_hash iterate block with a hash' do
      expect = [{"id"=>1, "str"=>"abc"}, {"id"=>2, "str"=>"defg"}, {"id"=>3, "str"=>"hi"}, {"id"=>4, "str"=>nil}]
      @res.each_hash do |a|
        assert{ a == expect.shift }
      end
    end

    it '#each_hash(true): hash key has table name' do
      expect = [{"t.id"=>1, "t.str"=>"abc"}, {"t.id"=>2, "t.str"=>"defg"}, {"t.id"=>3, "t.str"=>"hi"}, {"t.id"=>4, "t.str"=>nil}]
      @res.each_hash(true) do |a|
        assert{ a == expect.shift }
      end
    end

    it '#each always returns records from the beginning' do
      assert{ @res.each.entries == [[1, "abc"], [2, "defg"], [3, "hi"], [4, nil]] }
      assert{ @res.each.entries == [[1, "abc"], [2, "defg"], [3, "hi"], [4, nil]] }
    end

    it '#row_tell returns position of current record, #row_seek set position of current record' do
      assert{ @res.fetch_row == [1, 'abc'] }
      pos = @res.row_tell
      assert{ @res.fetch_row == [2, 'defg'] }
      assert{ @res.fetch_row == [3, 'hi'] }
      @res.row_seek pos
      assert{ @res.fetch_row == [2, 'defg'] }
    end

    it '#free returns nil' do
      assert{ @res.free == nil }
    end

    it '#server_status returns server status as Intger' do
      assert{ @res.server_status.is_a? Integer }
    end
  end

  describe 'Mysql::Result: variable data' do
    before do
      @m = Mysql.connect(MYSQL_SERVER, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT, MYSQL_SOCKET)
      @m.query("set sql_mode=''")
    end

    after do
      @m&.close
    end

    it '#fetch returns result-record' do
      res = @m.query 'select 123, "abc", null'
      assert{ res.fetch == [123, 'abc', nil] }
    end

    it '#fetch bit column (8bit)' do
      @m.query 'create temporary table t (i bit(8))'
      @m.query 'insert into t values (0),(-1),(127),(-128),(255),(-255),(256)'
      res = @m.query 'select i from t'
      assert{
        res.entries == [
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

    it '#fetch bit column (64bit)' do
      @m.query 'create temporary table t (i bit(64))'
      @m.query 'insert into t values (0),(-1),(4294967296),(18446744073709551615),(18446744073709551616)'
      res = @m.query 'select i from t'
      assert{
        res.entries == [
          ["\x00\x00\x00\x00\x00\x00\x00\x00".force_encoding('ASCII-8BIT')],
          ["\xff\xff\xff\xff\xff\xff\xff\xff".force_encoding('ASCII-8BIT')],
          ["\x00\x00\x00\x01\x00\x00\x00\x00".force_encoding('ASCII-8BIT')],
          ["\xff\xff\xff\xff\xff\xff\xff\xff".force_encoding('ASCII-8BIT')],
          ["\xff\xff\xff\xff\xff\xff\xff\xff".force_encoding('ASCII-8BIT')],
        ]
      }
    end

    it '#fetch tinyint column' do
      @m.query 'create temporary table t (i tinyint)'
      @m.query 'insert into t values (0),(-1),(127),(-128),(255),(-255)'
      res = @m.query 'select i from t'
      assert{ res.entries == [[0], [-1], [127], [-128], [127], [-128]] }
    end

    it '#fetch tinyint unsigned column' do
      @m.query 'create temporary table t (i tinyint unsigned)'
      @m.query 'insert into t values (0),(-1),(127),(-128),(255),(-255),(256)'
      res = @m.query 'select i from t'
      assert{ res.entries == [[0], [0], [127], [0], [255], [0], [255]] }
    end

    it '#fetch smallint column' do
      @m.query 'create temporary table t (i smallint)'
      @m.query 'insert into t values (0),(-1),(32767),(-32768),(65535),(-65535),(65536)'
      res = @m.query 'select i from t'
      assert{ res.entries == [[0], [-1], [32767], [-32768], [32767], [-32768], [32767]] }
    end

    it '#fetch smallint unsigned column' do
      @m.query 'create temporary table t (i smallint unsigned)'
      @m.query 'insert into t values (0),(-1),(32767),(-32768),(65535),(-65535),(65536)'
      res = @m.query 'select i from t'
      assert{ res.entries == [[0], [0], [32767], [0], [65535], [0], [65535]] }
    end

    it '#fetch mediumint column' do
      @m.query 'create temporary table t (i mediumint)'
      @m.query 'insert into t values (0),(-1),(8388607),(-8388608),(16777215),(-16777215),(16777216)'
      res = @m.query 'select i from t'
      assert{ res.entries == [[0], [-1], [8388607], [-8388608], [8388607], [-8388608], [8388607]] }
    end

    it '#fetch mediumint unsigned column' do
      @m.query 'create temporary table t (i mediumint unsigned)'
      @m.query 'insert into t values (0),(-1),(8388607),(-8388608),(16777215),(-16777215),(16777216)'
      res = @m.query 'select i from t'
      assert{ res.entries == [[0], [0], [8388607], [0], [16777215], [0], [16777215]] }
    end

    it '#fetch int column' do
      @m.query 'create temporary table t (i int)'
      @m.query 'insert into t values (0),(-1),(2147483647),(-2147483648),(4294967295),(-4294967295),(4294967296)'
      res = @m.query 'select i from t'
      assert{ res.entries == [[0], [-1], [2147483647], [-2147483648], [2147483647], [-2147483648], [2147483647]] }
    end

    it '#fetch int unsigned column' do
      @m.query 'create temporary table t (i int unsigned)'
      @m.query 'insert into t values (0),(-1),(2147483647),(-2147483648),(4294967295),(-4294967295),(4294967296)'
      res = @m.query 'select i from t'
      assert{ res.entries == [[0], [0], [2147483647], [0], [4294967295], [0], [4294967295]] }
    end

    it '#fetch bigint column' do
      @m.query 'create temporary table t (i bigint)'
      @m.query 'insert into t values (0),(-1),(9223372036854775807),(-9223372036854775808),(18446744073709551615),(-18446744073709551615),(18446744073709551616)'
      res = @m.query 'select i from t'
      assert{ res.entries == [[0], [-1], [9223372036854775807], [-9223372036854775808], [9223372036854775807], [-9223372036854775808], [9223372036854775807]] }
    end

    it '#fetch bigint unsigned column' do
      @m.query 'create temporary table t (i bigint unsigned)'
      @m.query 'insert into t values (0),(-1),(9223372036854775807),(-9223372036854775808),(18446744073709551615),(-18446744073709551615),(18446744073709551616)'
      res = @m.query 'select i from t'
      assert{ res.entries == [[0], [0], [9223372036854775807], [0], [18446744073709551615], [0], [18446744073709551615]] }
    end

    it '#fetch float column' do
      @m.query 'create temporary table t (i float)'
      @m.query 'insert into t values (0),(-3.402823466E+38),(-1.175494351E-38),(1.175494351E-38),(3.402823466E+38)'
      res = @m.query 'select i from t'
      assert{ res.fetch[0] == 0 }
      assert{ (res.fetch[0] - -3.40282E+38).abs < 0.000000001E+38 }
      assert{ (res.fetch[0] - -1.17549E-38).abs < 0.000000001E-38 }
      assert{ (res.fetch[0] -  1.17549E-38).abs < 0.000000001E-38 }
      assert{ (res.fetch[0] -  3.40282E+38).abs < 0.000000001E+38 }
    end

    it '#fetch float unsigned column' do
      @m.query 'create temporary table t (i float unsigned)'
      @m.query 'insert into t values (0),(-3.402823466E+38),(-1.175494351E-38),(1.175494351E-38),(3.402823466E+38)'
      res = @m.query 'select i from t'
      assert{ res.fetch[0] == 0 }
      assert{ res.fetch[0] == 0 }
      assert{ res.fetch[0] == 0 }
      assert{ (res.fetch[0] -  1.17549E-38).abs < 0.000000001E-38 }
      assert{ (res.fetch[0] -  3.40282E+38).abs < 0.000000001E+38 }
    end

    it '#fetch double column' do
      @m.query 'create temporary table t (i double)'
      @m.query 'insert into t values (0),(-1.7976931348623157E+308),(-2.2250738585072014E-308),(2.2250738585072014E-308),(1.7976931348623157E+308)'
      res = @m.query 'select i from t'
      assert{ res.fetch[0] == 0 }
      assert{ (res.fetch[0] - -Float::MAX).abs < Float::EPSILON }
      assert{ (res.fetch[0] - -Float::MIN).abs < Float::EPSILON }
      assert{ (res.fetch[0] -  Float::MIN).abs < Float::EPSILON }
      assert{ (res.fetch[0] -  Float::MAX).abs < Float::EPSILON }
    end

    it '#fetch double unsigned column' do
      @m.query 'create temporary table t (i double unsigned)'
      @m.query 'insert into t values (0),(-1.7976931348623157E+308),(-2.2250738585072014E-308),(2.2250738585072014E-308),(1.7976931348623157E+308)'
      res = @m.query 'select i from t'
      assert{ res.fetch[0] == 0 }
      assert{ res.fetch[0] == 0 }
      assert{ res.fetch[0] == 0 }
      assert{ (res.fetch[0] - Float::MIN).abs < Float::EPSILON }
      assert{ (res.fetch[0] - Float::MAX).abs < Float::EPSILON }
    end

    it '#fetch decimal column' do
      @m.query 'create temporary table t (i decimal(12,2))'
      @m.query 'insert into t values (0),(9999999999),(-9999999999),(10000000000),(-10000000000)'
      res = @m.query 'select i from t'
      assert{ res.entries == [[0], [9999999999], [-9999999999], [BigDecimal('9999999999.99')], [BigDecimal('-9999999999.99')]] }
    end

    it '#fetch decimal unsigned column' do
      @m.query 'create temporary table t (i decimal(12,2) unsigned)'
      @m.query 'insert into t values (0),(9999999998),(9999999999),(-9999999998),(-9999999999),(10000000000),(-10000000000)'
      res = @m.query 'select i from t'
      assert{ res.entries == [[0], [9999999998], [9999999999], [0], [0], [BigDecimal('9999999999.99')], [0]] }
    end

    it '#fetch date column' do
      @m.query 'create temporary table t (i date)'
      @m.query "insert into t values ('0000-00-00'),('1000-01-01'),('9999-12-31')"
      res = @m.query 'select i from t'
      cols = res.fetch
      assert{ cols == [nil] }
      cols = res.fetch
      assert{ cols == [Date.new(1000, 1, 1)] }
      cols = res.fetch
      assert{ cols == [Date.new(9999, 12, 31)] }
    end

    it '#fetch datetime column' do
      @m.query 'create temporary table t (i datetime(6))'
      @m.query "insert into t values ('0000-00-00 00:00:00'),('1000-01-01 00:00:00'),('2022-10-30 12:34:56.789'),('9999-12-31 23:59:59')"
      res = @m.query 'select i from t'
      assert{ res.fetch == [nil] }
      assert{ res.fetch == [Time.new(1000, 1, 1)] }
      assert{ res.fetch == [Time.new(2022, 10, 30, 12, 34, 56789/1000r)] }
      assert{ res.fetch == [Time.new(9999, 12, 31, 23, 59, 59)] }
    end

    it '#fetch timestamp column' do
      @m.query 'create temporary table t (i timestamp(6))'
      @m.query("insert into t values ('1970-01-02 00:00:00'),('2022-10-30 12:34:56.789'),('2037-12-30 23:59:59')")
      res = @m.query 'select i from t'
      assert{ res.fetch == [Time.new(1970, 1, 2)] }
      assert{ res.fetch == [Time.new(2022, 10, 30, 12, 34, 56789/1000r)] }
      assert{ res.fetch == [Time.new(2037, 12, 30, 23, 59, 59)] }
    end

    it '#fetch time column' do
      @m.query 'create temporary table t (i time)'
      @m.query "insert into t values ('-838:59:59'),(0),('838:59:59')"
      res = @m.query 'select i from t'
      assert{ res.fetch == [-(838*3600+59*60+59)] }
      assert{ res.fetch == [0] }
      assert{ res.fetch == [838*3600+59*60+59] }
    end

    it '#fetch year column' do
      @m.query 'create temporary table t (i year)'
      @m.query 'insert into t values (0),(70),(69),(1901),(2155)'
      res = @m.query 'select i from t'
      assert{ res.entries == [[0], [1970], [2069], [1901], [2155]] }
    end

    it '#fetch char column' do
      @m.query 'create temporary table t (i char(10))'
      @m.query "insert into t values (null),('abc')"
      res = @m.query 'select i from t'
      assert{ res.entries == [[nil], ['abc']] }
    end

    it '#fetch varchar column' do
      @m.query 'create temporary table t (i varchar(10))'
      @m.query "insert into t values (null),('abc')"
      res = @m.query 'select i from t'
      assert{ res.entries == [[nil], ['abc']] }
    end

    it '#fetch binary column' do
      @m.query 'create temporary table t (i binary(10))'
      @m.query "insert into t values (null),('abc')"
      res = @m.query 'select i from t'
      assert{ res.entries == [[nil], ["abc\0\0\0\0\0\0\0"]] }
    end

    it '#fetch varbinary column' do
      @m.query 'create temporary table t (i varbinary(10))'
      @m.query "insert into t values (null),('abc')"
      res = @m.query 'select i from t'
      assert{ res.entries == [[nil], ["abc"]] }
    end

    it '#fetch tinyblob column' do
      @m.query 'create temporary table t (i tinyblob)'
      @m.query "insert into t values (null),('#{"a"*255}')"
      res = @m.query 'select i from t'
      assert{ res.entries == [[nil], ["a"*255]] }
    end

    it '#fetch tinytext column' do
      @m.query 'create temporary table t (i tinytext)'
      @m.query "insert into t values (null),('#{"a"*255}')"
      res = @m.query 'select i from t'
      assert{ res.entries == [[nil], ["a"*255]] }
    end

    it '#fetch blob column' do
      @m.query 'create temporary table t (i blob)'
      @m.query "insert into t values (null),('#{"a"*65535}')"
      res = @m.query 'select i from t'
      assert{ res.entries == [[nil], ["a"*65535]] }
    end

    it '#fetch text column' do
      @m.query 'create temporary table t (i text)'
      @m.query "insert into t values (null),('#{"a"*65535}')"
      res = @m.query 'select i from t'
      assert{ res.entries == [[nil], ["a"*65535]] }
    end

    it '#fetch mediumblob column' do
      @m.query 'create temporary table t (i mediumblob)'
      @m.query "insert into t values (null),('#{"a"*16777215}')"
      res = @m.query 'select i from t'
      assert{ res.entries == [[nil], ['a'*16777215]] }
    end

    it '#fetch mediumtext column' do
      @m.query 'create temporary table t (i mediumtext)'
      @m.query "insert into t values (null),('#{"a"*16777215}')"
      res = @m.query 'select i from t'
      assert{ res.entries == [[nil], ['a'*16777215]] }
    end

    it '#fetch longblob column' do
      @m.query 'create temporary table t (i longblob)'
      @m.query "insert into t values (null),('#{"a"*16777216}')"
      res = @m.query 'select i from t'
      assert{ res.entries == [[nil], ["a"*16777216]] }
    end

    it '#fetch longtext column' do
      @m.query 'create temporary table t (i longtext)'
      @m.query "insert into t values (null),('#{"a"*16777216}')"
      res = @m.query 'select i from t'
      assert{ res.entries == [[nil], ["a"*16777216]] }
    end

    it '#fetch enum column' do
      @m.query "create temporary table t (i enum('abc','def'))"
      @m.query "insert into t values (null),(0),(1),(2),('abc'),('def'),('ghi')"
      res = @m.query 'select i from t'
      assert{ res.entries == [[nil], [''], ['abc'], ['def'], ['abc'], ['def'], ['']] }
    end

    it '#fetch set column' do
      @m.query "create temporary table t (i set('abc','def'))"
      @m.query "insert into t values (null),(0),(1),(2),(3),('abc'),('def'),('abc,def'),('ghi')"
      res = @m.query 'select i from t'
      assert{ res.entries == [[nil], [''], ['abc'], ['def'], ['abc,def'], ['abc'], ['def'], ['abc,def'], ['']] }
    end

    it '#fetch json column' do
      if @m.server_version >= 50700
        @m.query "create temporary table t (i json)"
        @m.query "insert into t values ('123'),('{\"a\":1,\"b\":2,\"c\":3}'),('[1,2,3]')"
        res = @m.query 'select i from t'
        assert{ res.entries == [['123'], ['{"a": 1, "b": 2, "c": 3}'], ['[1, 2, 3]']] }
      end
    end
  end

  describe 'Mysql::Field' do
    before do
      @m = Mysql.connect(MYSQL_SERVER, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT, MYSQL_SOCKET)
      @m.charset = 'latin1'
      @m.query 'create temporary table t (id int default 0, str char(10), primary key (id))'
      @m.query "insert into t values (1,'abc'),(2,'defg'),(3,'hi'),(4,null)"
      @res = @m.query 'select * from t'
    end

    after do
      @m&.close
    end

    it '#name is name of field' do
      assert{ @res.fields[0].name == 'id' }
    end

    it '#table is name of table for field' do
      assert{ @res.fields[0].table == 't' }
    end

    it '#def for result set is null' do
      assert{ @res.fields[0].def == nil }
    end

    it '#type is type of field as Integer' do
      assert{ @res.fields[0].type == Mysql::Field::TYPE_LONG }
      assert{ @res.fields[1].type == Mysql::Field::TYPE_STRING }
    end

    it '#length is length of field' do
      assert{ @res.fields[0].length == 11 }
      assert{ @res.fields[1].length == 10 }
    end

    it '#max_length is maximum length of field value' do
      assert{ @res.fields[0].max_length == 1 }
      assert{ @res.fields[1].max_length == 4 }
    end

    it '#flags is flag of field as Integer' do
      assert{ @res.fields[0].flags == Mysql::Field::NUM_FLAG|Mysql::Field::PRI_KEY_FLAG|Mysql::Field::PART_KEY_FLAG|Mysql::Field::NOT_NULL_FLAG }
      assert{ @res.fields[1].flags == 0 }
    end

    it '#decimals is number of decimal digits' do
      assert{ @m.query('select 1.23').fields[0].decimals == 2 }
    end

    it '#to_hash return field as hash' do
      assert{
        @res.fields[0].to_hash == {
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
      assert{
        @res.fields[1].to_hash == {
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

    it '#inspect returns "#<Mysql::Field:name>"' do
      assert{ @res.fields[0].inspect == '#<Mysql::Field:id>' }
      assert{ @res.fields[1].inspect == '#<Mysql::Field:str>' }
    end

    it '#is_num? returns true if the field is numeric' do
      assert{ @res.fields[0].is_num? == true }
      assert{ @res.fields[1].is_num? == false }
    end

    it '#is_not_null? returns true if the field is not null' do
      assert{ @res.fields[0].is_not_null? == true }
      assert{ @res.fields[1].is_not_null? == false }
    end

    it '#is_pri_key? returns true if the field is primary key' do
      assert{ @res.fields[0].is_pri_key? == true }
      assert{ @res.fields[1].is_pri_key? == false }
    end
  end

  describe 'create Mysql::Stmt object:' do
    before do
      @m = Mysql.connect(MYSQL_SERVER, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT, MYSQL_SOCKET)
    end

    after do
      @m&.close
    end

    it 'Mysql#stmt returns Mysql::Stmt object' do
      assert{ @m.stmt.kind_of? Mysql::Stmt }
    end

    it 'Mysq;#prepare returns Mysql::Stmt object' do
      assert{ @m.prepare("select 1").kind_of? Mysql::Stmt }
    end
  end

  describe 'Mysql::Stmt' do
    before do
      @m = Mysql.connect(MYSQL_SERVER, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT, MYSQL_SOCKET)
      @m.query("set sql_mode=''")
      @s = @m.stmt
    end

    after do
      @s&.close
      @m&.close
    end

    it '#affected_rows returns number of affected records' do
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

    it '#close returns nil' do
      assert{ @s.close == nil }
    end

    it '#data_seek set position of current record' do
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

    it '#each iterate block with a record' do
      @m.query 'create temporary table t (i int, c char(255), d datetime)'
      @m.query "insert into t values (1,'abc','19701224235905'),(2,'def','21120903123456'),(3,'123',null)"
      @s.prepare 'select * from t'
      res = @s.execute
      expect = [
        [1, 'abc', Time.new(1970, 12, 24, 23, 59, 5)],
        [2, 'def', Time.new(2112, 9, 3, 12, 34, 56)],
        [3, '123', nil],
      ]
      res.each do |a|
        assert{ a == expect.shift }
      end
    end

    it '#execute returns result set' do
      @s.prepare 'select 1'
      assert{ @s.execute.entries == [[1]] }
    end

    it '#execute returns nil if query returns no results' do
      @s.prepare 'set @a=1'
      assert{ @s.execute == nil }
    end

    it '#execute returns self if return_result is false' do
      @s.prepare 'select 1'
      assert{ @s.execute(return_result: false) == @s }
    end

    it '#execute pass arguments to query' do
      @m.query 'create temporary table t (i int)'
      @s.prepare 'insert into t values (?)'
      @s.execute 123
      @s.execute '456'
      @s.execute true
      @s.execute false
      assert{ @m.query('select * from t').entries == [[123], [456], [1], [0]] }
    end

    it '#execute with various arguments' do
      @m.query 'create temporary table t (i int, c char(255), t timestamp)'
      @s.prepare 'insert into t values (?,?,?)'
      @s.execute 123, 'hoge', Time.local(2009, 12, 8, 19, 56, 21)
      assert{ @m.query('select * from t').fetch_row == [123, 'hoge', Time.local(2009, 12, 8, 19, 56, 21)] }
    end

    it '#execute with arguments that is invalid count raise error' do
      @s.prepare 'select ?'
      expect { @s.execute 123, 456 }.to raise_error Mysql::ClientError, 'parameter count mismatch'
    end

    it '#execute with huge value' do
      [30, 31, 32, 62, 63, 64].each do |i|
        assert{ @m.prepare('select ?').execute(2**i-1).fetch == [2**i-1] }
        assert{ @m.prepare('select ?').execute(-(2**i)).fetch == [-2**i] }
      end
    end

    describe '#execute with various integer value:' do
      before do
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
        it "#{n} is #{n}" do
          @s.prepare 'insert into t values (?)'
          @s.execute n
          assert{ @m.query('select i from t').fetch == [n] }
        end
      end
    end

    describe '#execute with various unsigned integer value:' do
      before do
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
        it "#{n} is #{n}" do
          @s.prepare 'insert into t values (?)'
          @s.execute n
          assert{ @m.query('select i from t').fetch == [n] }
        end
      end
    end

    it '#fetch returns result-record' do
      @s.prepare 'select 123, "abc", null'
      @s.execute
      assert{ @s.fetch == [123, 'abc', nil] }
    end

    it '#fetch bit column (8bit)' do
      @m.query 'create temporary table t (i bit(8))'
      @m.query 'insert into t values (0),(-1),(127),(-128),(255),(-255),(256)'
      @s.prepare 'select i from t'
      @s.execute
      assert{
        @s.entries == [
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

    it '#fetch bit column (64bit)' do
      @m.query 'create temporary table t (i bit(64))'
      @m.query 'insert into t values (0),(-1),(4294967296),(18446744073709551615),(18446744073709551616)'
      @s.prepare 'select i from t'
      @s.execute
      assert{
        @s.entries == [
          ["\x00\x00\x00\x00\x00\x00\x00\x00".force_encoding('ASCII-8BIT')],
          ["\xff\xff\xff\xff\xff\xff\xff\xff".force_encoding('ASCII-8BIT')],
          ["\x00\x00\x00\x01\x00\x00\x00\x00".force_encoding('ASCII-8BIT')],
          ["\xff\xff\xff\xff\xff\xff\xff\xff".force_encoding('ASCII-8BIT')],
          ["\xff\xff\xff\xff\xff\xff\xff\xff".force_encoding('ASCII-8BIT')],
        ]
      }
    end

    it '#fetch tinyint column' do
      @m.query 'create temporary table t (i tinyint)'
      @m.query 'insert into t values (0),(-1),(127),(-128),(255),(-255)'
      @s.prepare 'select i from t'
      @s.execute
      assert{ @s.entries == [[0], [-1], [127], [-128], [127], [-128]] }
    end

    it '#fetch tinyint unsigned column' do
      @m.query 'create temporary table t (i tinyint unsigned)'
      @m.query 'insert into t values (0),(-1),(127),(-128),(255),(-255),(256)'
      @s.prepare 'select i from t'
      @s.execute
      assert{ @s.entries == [[0], [0], [127], [0], [255], [0], [255]] }
    end

    it '#fetch smallint column' do
      @m.query 'create temporary table t (i smallint)'
      @m.query 'insert into t values (0),(-1),(32767),(-32768),(65535),(-65535),(65536)'
      @s.prepare 'select i from t'
      @s.execute
      assert{ @s.entries == [[0], [-1], [32767], [-32768], [32767], [-32768], [32767]] }
    end

    it '#fetch smallint unsigned column' do
      @m.query 'create temporary table t (i smallint unsigned)'
      @m.query 'insert into t values (0),(-1),(32767),(-32768),(65535),(-65535),(65536)'
      @s.prepare 'select i from t'
      @s.execute
      assert{ @s.entries == [[0], [0], [32767], [0], [65535], [0], [65535]] }
    end

    it '#fetch mediumint column' do
      @m.query 'create temporary table t (i mediumint)'
      @m.query 'insert into t values (0),(-1),(8388607),(-8388608),(16777215),(-16777215),(16777216)'
      @s.prepare 'select i from t'
      @s.execute
      assert{ @s.entries == [[0], [-1], [8388607], [-8388608], [8388607], [-8388608], [8388607]] }
    end

    it '#fetch mediumint unsigned column' do
      @m.query 'create temporary table t (i mediumint unsigned)'
      @m.query 'insert into t values (0),(-1),(8388607),(-8388608),(16777215),(-16777215),(16777216)'
      @s.prepare 'select i from t'
      @s.execute
      assert{ @s.entries == [[0], [0], [8388607], [0], [16777215], [0], [16777215]] }
    end

    it '#fetch int column' do
      @m.query 'create temporary table t (i int)'
      @m.query 'insert into t values (0),(-1),(2147483647),(-2147483648),(4294967295),(-4294967295),(4294967296)'
      @s.prepare 'select i from t'
      @s.execute
      assert{ @s.entries == [[0], [-1], [2147483647], [-2147483648], [2147483647], [-2147483648], [2147483647]] }
    end

    it '#fetch int unsigned column' do
      @m.query 'create temporary table t (i int unsigned)'
      @m.query 'insert into t values (0),(-1),(2147483647),(-2147483648),(4294967295),(-4294967295),(4294967296)'
      @s.prepare 'select i from t'
      @s.execute
      assert{ @s.entries == [[0], [0], [2147483647], [0], [4294967295], [0], [4294967295]] }
    end

    it '#fetch bigint column' do
      @m.query 'create temporary table t (i bigint)'
      @m.query 'insert into t values (0),(-1),(9223372036854775807),(-9223372036854775808),(18446744073709551615),(-18446744073709551615),(18446744073709551616)'
      @s.prepare 'select i from t'
      @s.execute
      assert{ @s.entries == [[0], [-1], [9223372036854775807], [-9223372036854775808], [9223372036854775807], [-9223372036854775808], [9223372036854775807]] }
    end

    it '#fetch bigint unsigned column' do
      @m.query 'create temporary table t (i bigint unsigned)'
      @m.query 'insert into t values (0),(-1),(9223372036854775807),(-9223372036854775808),(18446744073709551615),(-18446744073709551615),(18446744073709551616)'
      @s.prepare 'select i from t'
      @s.execute
      assert{ @s.entries == [[0], [0], [9223372036854775807], [0], [18446744073709551615], [0], [18446744073709551615]] }
    end

    it '#fetch float column' do
      @m.query 'create temporary table t (i float)'
      @m.query 'insert into t values (0),(-3.402823466E+38),(-1.175494351E-38),(1.175494351E-38),(3.402823466E+38)'
      @s.prepare 'select i from t'
      @s.execute
      assert{ @s.fetch[0] == 0 }
      assert{ (@s.fetch[0] - -3.402823466E+38).abs < 0.000000001E+38 }
      assert{ (@s.fetch[0] - -1.175494351E-38).abs < 0.000000001E-38 }
      assert{ (@s.fetch[0] -  1.175494351E-38).abs < 0.000000001E-38 }
      assert{ (@s.fetch[0] -  3.402823466E+38).abs < 0.000000001E+38 }
    end

    it '#fetch float unsigned column' do
      @m.query 'create temporary table t (i float unsigned)'
      @m.query 'insert into t values (0),(-3.402823466E+38),(-1.175494351E-38),(1.175494351E-38),(3.402823466E+38)'
      @s.prepare 'select i from t'
      @s.execute
      assert{ @s.fetch[0] == 0 }
      assert{ @s.fetch[0] == 0 }
      assert{ @s.fetch[0] == 0 }
      assert{ (@s.fetch[0] -  1.175494351E-38).abs < 0.000000001E-38 }
      assert{ (@s.fetch[0] -  3.402823466E+38).abs < 0.000000001E+38 }
    end

    it '#fetch double column' do
      @m.query 'create temporary table t (i double)'
      @m.query 'insert into t values (0),(-1.7976931348623157E+308),(-2.2250738585072014E-308),(2.2250738585072014E-308),(1.7976931348623157E+308)'
      @s.prepare 'select i from t'
      @s.execute
      assert{ @s.fetch[0] == 0 }
      assert{ (@s.fetch[0] - -Float::MAX).abs < Float::EPSILON }
      assert{ (@s.fetch[0] - -Float::MIN).abs < Float::EPSILON }
      assert{ (@s.fetch[0] -  Float::MIN).abs < Float::EPSILON }
      assert{ (@s.fetch[0] -  Float::MAX).abs < Float::EPSILON }
    end

    it '#fetch double unsigned column' do
      @m.query 'create temporary table t (i double unsigned)'
      @m.query 'insert into t values (0),(-1.7976931348623157E+308),(-2.2250738585072014E-308),(2.2250738585072014E-308),(1.7976931348623157E+308)'
      @s.prepare 'select i from t'
      @s.execute
      assert{ @s.fetch[0] == 0 }
      assert{ @s.fetch[0] == 0 }
      assert{ @s.fetch[0] == 0 }
      assert{ (@s.fetch[0] - Float::MIN).abs < Float::EPSILON }
      assert{ (@s.fetch[0] - Float::MAX).abs < Float::EPSILON }
    end

    it '#fetch decimal column' do
      @m.query 'create temporary table t (i decimal(12,2))'
      @m.query 'insert into t values (0),(9999999999),(-9999999999),(10000000000),(-10000000000)'
      @s.prepare 'select i from t'
      @s.execute
      assert{ @s.entries == [[0], [9999999999], [-9999999999], [BigDecimal('9999999999.99')], [BigDecimal('-9999999999.99')]] }
    end

    it '#fetch decimal unsigned column' do
      @m.query 'create temporary table t (i decimal(12,2) unsigned)'
      @m.query 'insert into t values (0),(9999999998),(9999999999),(-9999999998),(-9999999999),(10000000000),(-10000000000)'
      @s.prepare 'select i from t'
      @s.execute
      assert{ @s.entries == [[0], [9999999998], [9999999999], [0], [0], [BigDecimal('9999999999.99')], [0]] }
    end

    it '#fetch date column' do
      @m.query 'create temporary table t (i date)'
      @m.query "insert into t values ('0000-00-00'),('1000-01-01'),('9999-12-31')"
      @s.prepare 'select i from t'
      @s.execute
      cols = @s.fetch
      assert{ cols == [nil] }
      cols = @s.fetch
      assert{ cols == [Date.new(1000, 1, 1)] }
      cols = @s.fetch
      assert{ cols == [Date.new(9999, 12, 31)] }
    end

    it '#fetch datetime column' do
      @m.query 'create temporary table t (i datetime(6))'
      @m.query "insert into t values ('0000-00-00 00:00:00'),('1000-01-01 00:00:00'),('2022-10-30 12:34:56.789'),('9999-12-31 23:59:59')"
      @s.prepare 'select i from t'
      @s.execute
      assert{ @s.fetch == [nil] }
      assert{ @s.fetch == [Time.new(1000, 1, 1)] }
      assert{ @s.fetch == [Time.new(2022, 10, 30, 12, 34, 56789/1000r)] }
      assert{ @s.fetch == [Time.new(9999, 12, 31, 23, 59, 59)] }
    end

    it '#fetch timestamp column' do
      @m.query 'create temporary table t (i timestamp(6))'
      @m.query("insert into t values ('1970-01-02 00:00:00'),('2022-10-30 12:34:56.789'),('2037-12-30 23:59:59')")
      @s.prepare 'select i from t'
      @s.execute
      assert{ @s.fetch == [Time.new(1970, 1, 2)] }
      assert{ @s.fetch == [Time.new(2022, 10, 30, 12, 34, 56789/1000r)] }
      assert{ @s.fetch == [Time.new(2037, 12, 30, 23, 59, 59)] }
    end

    it '#fetch time column' do
      @m.query 'create temporary table t (i time)'
      @m.query "insert into t values ('-838:59:59'),(0),('838:59:59')"
      @s.prepare 'select i from t'
      @s.execute
      assert{ @s.fetch == [-(838*3600+59*60+59)] }
      assert{ @s.fetch == [0] }
      assert{ @s.fetch == [838*3600+59*60+59] }
    end

    it '#fetch year column' do
      @m.query 'create temporary table t (i year)'
      @m.query 'insert into t values (0),(70),(69),(1901),(2155)'
      @s.prepare 'select i from t'
      @s.execute
      assert{ @s.entries == [[0], [1970], [2069], [1901], [2155]] }
    end

    it '#fetch char column' do
      @m.query 'create temporary table t (i char(10))'
      @m.query "insert into t values (null),('abc')"
      @s.prepare 'select i from t'
      @s.execute
      assert{ @s.entries == [[nil], ['abc']] }
    end

    it '#fetch varchar column' do
      @m.query 'create temporary table t (i varchar(10))'
      @m.query "insert into t values (null),('abc')"
      @s.prepare 'select i from t'
      @s.execute
      assert{ @s.entries == [[nil], ['abc']] }
    end

    it '#fetch binary column' do
      @m.query 'create temporary table t (i binary(10))'
      @m.query "insert into t values (null),('abc')"
      @s.prepare 'select i from t'
      @s.execute
      assert{ @s.entries == [[nil], ["abc\0\0\0\0\0\0\0"]] }
    end

    it '#fetch varbinary column' do
      @m.query 'create temporary table t (i varbinary(10))'
      @m.query "insert into t values (null),('abc')"
      @s.prepare 'select i from t'
      @s.execute
      assert{ @s.entries == [[nil], ["abc"]] }
    end

    it '#fetch tinyblob column' do
      @m.query 'create temporary table t (i tinyblob)'
      @m.query "insert into t values (null),('#{"a"*255}')"
      @s.prepare 'select i from t'
      @s.execute
      assert{ @s.entries == [[nil], ["a"*255]] }
    end

    it '#fetch tinytext column' do
      @m.query 'create temporary table t (i tinytext)'
      @m.query "insert into t values (null),('#{"a"*255}')"
      @s.prepare 'select i from t'
      @s.execute
      assert{ @s.entries == [[nil], ["a"*255]] }
    end

    it '#fetch blob column' do
      @m.query 'create temporary table t (i blob)'
      @m.query "insert into t values (null),('#{"a"*65535}')"
      @s.prepare 'select i from t'
      @s.execute
      assert{ @s.entries == [[nil], ["a"*65535]] }
    end

    it '#fetch text column' do
      @m.query 'create temporary table t (i text)'
      @m.query "insert into t values (null),('#{"a"*65535}')"
      @s.prepare 'select i from t'
      @s.execute
      assert{ @s.entries == [[nil], ["a"*65535]] }
    end

    it '#fetch mediumblob column' do
      @m.query 'create temporary table t (i mediumblob)'
      @m.query "insert into t values (null),('#{"a"*16777215}')"
      @s.prepare 'select i from t'
      @s.execute
      assert{ @s.entries == [[nil], ['a'*16777215]] }
    end

    it '#fetch mediumtext column' do
      @m.query 'create temporary table t (i mediumtext)'
      @m.query "insert into t values (null),('#{"a"*16777215}')"
      @s.prepare 'select i from t'
      @s.execute
      assert{ @s.entries == [[nil], ['a'*16777215]] }
    end

    it '#fetch longblob column' do
      @m.query 'create temporary table t (i longblob)'
      @m.query "insert into t values (null),('#{"a"*16777216}')"
      @s.prepare 'select i from t'
      @s.execute
      assert{ @s.entries == [[nil], ["a"*16777216]] }
    end

    it '#fetch longtext column' do
      @m.query 'create temporary table t (i longtext)'
      @m.query "insert into t values (null),('#{"a"*16777216}')"
      @s.prepare 'select i from t'
      @s.execute
      assert{ @s.entries == [[nil], ["a"*16777216]] }
    end

    it '#fetch enum column' do
      @m.query "create temporary table t (i enum('abc','def'))"
      @m.query "insert into t values (null),(0),(1),(2),('abc'),('def'),('ghi')"
      @s.prepare 'select i from t'
      @s.execute
      assert{ @s.entries == [[nil], [''], ['abc'], ['def'], ['abc'], ['def'], ['']] }
    end

    it '#fetch set column' do
      @m.query "create temporary table t (i set('abc','def'))"
      @m.query "insert into t values (null),(0),(1),(2),(3),('abc'),('def'),('abc,def'),('ghi')"
      @s.prepare 'select i from t'
      @s.execute
      assert{ @s.entries == [[nil], [''], ['abc'], ['def'], ['abc,def'], ['abc'], ['def'], ['abc,def'], ['']] }
    end

    it '#fetch json column' do
      if @m.server_version >= 50700
        @m.query "create temporary table t (i json)"
        @m.query "insert into t values ('123'),('{\"a\":1,\"b\":2,\"c\":3}'),('[1,2,3]')"
        @s.prepare 'select i from t'
        @s.execute
        assert{ @s.entries == [['123'], ['{"a": 1, "b": 2, "c": 3}'], ['[1, 2, 3]']] }
      end
    end

    it '#field_count' do
      @s.prepare 'select 1,2,3'
      assert{ @s.field_count == 3 }
      @s.prepare 'set @a=1'
      assert{ @s.field_count == 0 }
    end

    it '#free_result' do
      @s.free_result
      @s.prepare 'select 1,2,3'
      @s.execute
      @s.free_result
    end

    it '#info' do
      @s.free_result
      @m.query 'create temporary table t (i int)'
      @s.prepare 'insert into t values (1),(2),(3)'
      @s.execute
      assert{ @s.info == 'Records: 3  Duplicates: 0  Warnings: 0' }
    end

    it '#insert_id' do
      @m.query 'create temporary table t (i int auto_increment, unique(i))'
      @s.prepare 'insert into t values (0)'
      @s.execute
      assert{ @s.insert_id == 1 }
      @s.execute
      assert{ @s.insert_id == 2 }
    end

    it '#more_reults? and #next_result' do
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

    describe '#execute with block' do
      before do
        @m.query 'drop procedure if exists test_proc'
        @m.query 'create procedure test_proc() begin select 1 as a; select 2 as b; end'
        @st = @m.prepare 'call test_proc()'
      end
      it 'returns self' do
        assert{ @st.execute{} == @st }
      end
      it 'evaluate block multiple times' do
        res = []
        @st.execute do |r|
          res.push r&.entries
        end
        assert{ res == [[[1]], [[2]], nil] }
      end
      it 'evaluate block only when query has result' do
        res = []
        @st.execute(yield_null_result: false) do |r|
          res.push r&.entries
        end
        assert{ res == [[[1]], [[2]]] }
      end
    end

    it '#num_rows' do
      @m.query 'create temporary table t (i int)'
      @m.query 'insert into t values (1),(2),(3),(4)'
      @s.prepare 'select * from t'
      @s.execute
      assert{ @s.num_rows == 4 }
    end

    it '#param_count' do
      @m.query 'create temporary table t (a int, b int, c int)'
      @s.prepare 'select * from t'
      assert{ @s.param_count == 0 }
      @s.prepare 'insert into t values (?,?,?)'
      assert{ @s.param_count == 3 }
    end

    it '#prepare' do
      assert{ @s.prepare('select 1').kind_of? Mysql::Stmt }
      expect { @s.prepare 'invalid syntax' }.to raise_error Mysql::ParseError
    end

    it '#prepare returns self' do
      assert{ @s.prepare('select 1') == @s }
    end

    it '#prepare with invalid query raises error' do
      expect { @s.prepare 'invalid query' }.to raise_error Mysql::ParseError
    end

    it '#fields' do
      @s.prepare 'select 1 foo, 2 bar'
      f = @s.fields
      assert{ f[0].name == 'foo' }
      assert{ f[1].name == 'bar' }

      @s.prepare 'set @a=1'
      assert{ @s.fields == [] }
    end

    it '#result_metadata' do
      @s.prepare 'select 1 foo, 2 bar'
      f = @s.result_metadata.fetch_fields
      assert{ f[0].name == 'foo' }
      assert{ f[1].name == 'bar' }
    end

    it '#result_metadata forn no data' do
      @s.prepare 'set @a=1'
      assert{ @s.result_metadata == nil }
    end

    it '#row_seek and #row_tell' do
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

    it '#sqlstate' do
      @s.prepare 'select 1'
      assert{ @s.sqlstate == '00000' }
      expect { @s.prepare 'hogehoge' }.to raise_error Mysql::ParseError
      assert{ @s.sqlstate == '42000' }
    end
  end

  describe 'Mysql::Error' do
    before do
      m = Mysql.connect(MYSQL_SERVER, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT, MYSQL_SOCKET)
      begin
        m.query('hogehoge')
      rescue => e
        @e = e
      end
      m.close
    end

    it '#error is error message' do
      assert{ @e.error == "You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near 'hogehoge' at line 1" }
    end

    it '#errno is error number' do
      assert{ @e.errno == 1064 }
    end

    it '#sqlstate is sqlstate value as String' do
      assert{ @e.sqlstate == '42000' }
    end
  end

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
      v = $VERBOSE
      $VERBOSE = false
      Encoding.default_internal = @default_internal
      $VERBOSE = v
    end

    describe 'default_internal is CP932' do
      before do
        @m.prepare("insert into t (utf8,cp932,eucjp,bin) values (?,?,?,?)").execute @utf8, @cp932, @eucjp, @bin
        v = $VERBOSE
        $VERBOSE = false
        Encoding.default_internal = 'CP932'
        $VERBOSE = v
      end
      it 'is converted to CP932' do
        result = @m.query('select "あいう"').fetch == ["\x82\xA0\x82\xA2\x82\xA4".force_encoding("CP932")]
        assert{ result }
      end
      it 'data is stored as is' do
        assert{ @m.query('select hex(utf8),hex(cp932),hex(eucjp),hex(bin) from t').fetch == ['E38184E3828DE381AF', '82A282EB82CD', 'A4A4A4EDA4CF', '00017F80FEFF'] }
      end
      it 'By simple query, charset of retrieved data is connection charset' do
        assert{ @m.query('select utf8,cp932,eucjp,bin from t').fetch == [@cp932, @cp932, @cp932, @bin] }
      end
      it 'By prepared statement, charset of retrieved data is connection charset except for binary' do
        assert{ @m.prepare('select utf8,cp932,eucjp,bin from t').execute.fetch == [@cp932, @cp932, @cp932, @bin] }
      end
    end

    describe 'query with CP932 encoding' do
      it 'is converted to UTF-8' do
        assert{ @m.query('select HEX("あいう")'.encode("CP932")).fetch == ["E38182E38184E38186"] }
      end
    end

    describe 'prepared statement with CP932 encoding' do
      it 'is converted to UTF-8' do
        assert{ @m.prepare('select HEX("あいう")'.encode("CP932")).execute.fetch == ["E38182E38184E38186"] }
      end
    end

    describe 'The encoding of data are correspond to charset of column:' do
      before do
        @m.prepare("insert into t (utf8,cp932,eucjp,bin) values (?,?,?,?)").execute @utf8, @cp932, @eucjp, @bin
      end
      it 'data is stored as is' do
        assert{ @m.query('select hex(utf8),hex(cp932),hex(eucjp),hex(bin) from t').fetch == ['E38184E3828DE381AF', '82A282EB82CD', 'A4A4A4EDA4CF', '00017F80FEFF'] }
      end
      it 'By simple query, charset of retrieved data is connection charset' do
        assert{ @m.query('select utf8,cp932,eucjp,bin from t').fetch == [@utf8, @utf8, @utf8, @bin] }
      end
      it 'By prepared statement, charset of retrieved data is connection charset except for binary' do
        assert{ @m.prepare('select utf8,cp932,eucjp,bin from t').execute.fetch == [@utf8, @utf8, @utf8, @bin] }
      end
    end

    describe 'The encoding of data are different from charset of column:' do
      before do
        @m.prepare("insert into t (utf8,cp932,eucjp,bin) values (?,?,?,?)").execute @utf8, @utf8, @utf8, @utf8
      end
      it 'stored data is converted' do
        assert{ @m.query("select hex(utf8),hex(cp932),hex(eucjp),hex(bin) from t").fetch == ["E38184E3828DE381AF", "82A282EB82CD", "A4A4A4EDA4CF", "E38184E3828DE381AF"] }
      end
      it 'By simple query, charset of retrieved data is connection charset' do
        assert{ @m.query("select utf8,cp932,eucjp,bin from t").fetch == [@utf8, @utf8, @utf8, @utf8.dup.force_encoding('ASCII-8BIT')] }
      end
      it 'By prepared statement, charset of retrieved data is connection charset except for binary' do
        assert{ @m.prepare("select utf8,cp932,eucjp,bin from t").execute.fetch == [@utf8, @utf8, @utf8, @utf8.dup.force_encoding("ASCII-8BIT")] }
      end
    end

    describe 'The data include invalid byte code:' do
      it 'raises Encoding::InvalidByteSequenceError' do
        cp932 = "\x01\xFF\x80".force_encoding("CP932")
        expect { @m.prepare("insert into t (cp932) values (?)").execute cp932 }.to raise_error Encoding::InvalidByteSequenceError
      end
    end
  end

  it 'connect_attrs' do
    m = Mysql.connect(MYSQL_SERVER, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT, MYSQL_SOCKET, connect_attrs: {hoge: 'fuga'})
    if m.server_version >= 50600
      h = m.query("select * from performance_schema.session_connect_attrs where processlist_id=connection_id()").fetch_hash
      assert{ h['ATTR_NAME'] == 'hoge' && h['ATTR_VALUE'] == 'fuga' }
    end
  end

  it 'disconnect from server' do
    m = Mysql.connect(MYSQL_SERVER, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT, MYSQL_SOCKET)
    m.query('kill connection_id()') rescue nil
    expect { m.query('select 1') }.to raise_error Mysql::ClientError::ServerLost, 'Lost connection to server during query'
  end

  it 'disconnect from client' do
    m = Mysql.connect(MYSQL_SERVER, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT, MYSQL_SOCKET)
    m.close
    expect { m.query('select 1') }.to raise_error Mysql::ClientError, 'MySQL client is not connected'
  end
end
