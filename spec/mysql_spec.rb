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

describe 'Mysql#conninfo' do
  before do
    @m = Mysql.allocate
    class << @m
      public :conninfo
    end
  end
  describe 'with no argument' do
    it 'return empty' do
      @m.conninfo().should == [{:flag=>0}, {}]
    end
  end
  describe 'with one hash' do
    it 'split to param and option' do
      ret = @m.conninfo({:host=>"localhost", :connect_timeout=>0})
      ret.should == [{:host=>"localhost", :flag=>0}, {:connect_timeout=>0}]
    end
  end
  describe 'with two hashes' do
    it 'return argument as it is' do
      ret = @m.conninfo({:host=>"localhost"}, {:connect_timeout=>0})
      ret.should == [{:host=>"localhost", :flag=>0}, {:connect_timeout=>0}]
    end
  end
  describe 'with two hashes and first hash have unknown parameter' do
    it 'raises ArgumentError' do
      proc{@m.conninfo({:hoge=>"fuga"}, {})}.should raise_error(ArgumentError, 'Unknown parameter: :hoge')
    end
  end
  describe 'with traditional arguments' do
    it 'return parameter and option hash' do
      ret = @m.conninfo("localhost", "user", "pass", "dbname", 1234, "socket", 567)
      ret.should == [{:host=>"localhost", :user=>"user", :password=>"pass", :db=>"dbname", :port=>1234, :socket=>"socket", :flag=>567}, {}]
    end
  end
  describe 'with traditional arguments (one argument)' do
    it 'return parameter and option hash' do
      ret = @m.conninfo("localhost")
      ret.should == [{:host=>"localhost", :user=>nil, :password=>nil, :db=>nil, :port=>nil, :socket=>nil, :flag=>nil}, {}]
    end
  end
  describe 'with traditional arguments and option hash' do
    it 'return parameter and option hash' do
      ret = @m.conninfo("localhost", "user", "pass", "dbname", 1234, "socket", 567, :connect_timeout=>0)
      ret.should == [{:host=>"localhost", :user=>"user", :password=>"pass", :db=>"dbname", :port=>1234, :socket=>"socket", :flag=>567}, {:connect_timeout=>0}]
    end
  end
  describe 'with URI format string' do
    it 'return parameter and option hash' do
      ret = @m.conninfo("mysql://user:pass@localhost:1234/dbname?socket=socket&flag=567&connect_timeout=0")
      ret.should == [{:host=>"localhost", :user=>"user", :password=>"pass", :db=>"dbname", :port=>1234, :socket=>"socket", :flag=>567}, {:connect_timeout=>0}]
    end
  end
  describe 'with URI format string(without port)' do
    it 'return default port' do
      ret = @m.conninfo("mysql://user:pass@localhost/dbname?socket=socket&flag=567&connect_timeout=0")
      ret.should == [{:host=>"localhost", :user=>"user", :password=>"pass", :db=>"dbname", :port=>3306, :socket=>"socket", :flag=>567}, {:connect_timeout=>0}]
    end
  end
  describe 'with URI format string and option' do
    it 'return parameter and option hash' do
      ret = @m.conninfo("mysql://user:pass@localhost:1234/dbname?socket=socket&flag=567", :connect_timeout=>0)
      ret.should == [{:host=>"localhost", :user=>"user", :password=>"pass", :db=>"dbname", :port=>1234, :socket=>"socket", :flag=>567}, {:connect_timeout=>0}]
    end
  end
  describe 'with URI object' do
    it 'return parameter and option hash' do
      uri = URI::Generic.new("mysql", "user", "localhost", 1234, nil, "/dbname", nil, "socket=socket&flag=567&connect_timeout=0", nil)
      uri.password = "pass"
      ret = @m.conninfo(uri)
      ret.should == [{:host=>"localhost", :user=>"user", :password=>"pass", :db=>"dbname", :port=>1234, :socket=>"socket", :flag=>567}, {:connect_timeout=>0}]
    end
  end
  describe 'with URI object which scheme is not mysql' do
    it 'raises ArgumentError' do
      uri = URI::Generic.new("hoge", "user", "localhost", 1234, nil, "/dbname", nil, "socket=socket&flag=567&connect_timeout=0", nil)
      proc{@m.conninfo(uri)}.should raise_error(ArgumentError, 'Invalid scheme: hoge')
    end
  end
  describe 'with URI object and option' do
    it 'return parameter and option hash' do
      uri = URI::Generic.new("mysql", "user", "localhost", 1234, nil, "/dbname", nil, "socket=socket&flag=567", nil)
      uri.password = "pass"
      ret = @m.conninfo(uri, :connect_timeout=>0)
      ret.should == [{:host=>"localhost", :user=>"user", :password=>"pass", :db=>"dbname", :port=>1234, :socket=>"socket", :flag=>567}, {:connect_timeout=>0}]
    end
  end
  describe 'with unknown object as first argument' do
    it 'raises ArgumentError' do
      proc{@m.conninfo(123)}.should raise_error(ArgumentError, "Invalid argument: 123")
    end
  end
  describe 'with option that shoule be flag' do
    it 'treat it as flag' do
      ret = @m.conninfo({}, {:connect_timeout=>0, :local_files=>true})
      ret.first.should == {:flag=>Mysql::CLIENT_LOCAL_FILES}
    end
  end
  describe 'with unknown option' do
    it 'raises ArgumentError' do
      proc{@m.conninfo({}, {:hoge=>123})}.should raise_error(ArgumentError, "Unknown option: :hoge")
    end
  end
end

describe 'Mysql.new with block' do
  it 'return block value' do
    Mysql.new{123}.should == 123
  end
end

describe 'Mysql.connect' do
  describe 'with URI string' do
    it 'connect to server' do
      uristr = "mysql://mysql.example.com:12345/dbname"
      Mysql::Protocol.should_receive(:new).with("mysql.example.com", 12345, nil, nil, nil, nil).and_return mock("Protocol", :null_object=>true)
      Mysql.connect(uristr)
    end
    it 'connect to server (localhost)' do
      uristr = "mysql://localhost/dbname?socket=/tmp/mysql.sock"
      Mysql::Protocol.should_receive(:new).with("localhost", 3306, "/tmp/mysql.sock", nil, nil, nil).and_return mock("Protocol", :null_object=>true)
      Mysql.connect(uristr)
    end
  end
  describe 'with URI object' do
    it 'connect to server' do
      uri = URI.parse "mysql://mysql.example.com:12345/dbname"
      Mysql::Protocol.should_receive(:new).with("mysql.example.com", 12345, nil, nil, nil, nil).and_return mock("Protocol", :null_object=>true)
      Mysql.connect(uri)
    end
  end
  describe 'with traditional arguments' do
    it 'connect to server' do
      Mysql::Protocol.should_receive(:new).with("mysql.example.com", 12345, nil, nil, nil, nil).and_return mock("Protocol", :null_object=>true)
      Mysql.connect "mysql.example.com", "username", "password", "dbname", 12345
    end
  end
  describe 'with block' do
    it 'return block value' do
      Mysql.connect(URL){123}.should == 123
    end
  end
end

describe 'Mysql' do
  before do
    @mysql = Mysql.connect URL
  end

  describe '#escape_string' do
    it 'escape special character' do
      @mysql.escape_string("abc'def\"ghi\0jkl%mno\npqr\rstu\x1avwx").should == "abc\\'def\\\"ghi\\0jkl%mno\\npqr\\rstu\\Zvwx"
    end
  end

  describe '#query with no result' do
    it 'return nil' do
      @mysql.query("create temporary table t (i int, c char(10))").should == nil
    end
  end

  describe '#query with single record result' do
    before do
      @res = @mysql.query "select 1,'abc',1.23"
    end
    it 'return values' do
      @res.fetch_row.should == [1, "abc", "1.23"]
      @res.fetch_row.should == nil
    end
    it 'returned value is tainted' do
      @res.fetch_row[1].should be_tainted
    end
  end

  describe '#query with null column result' do
    before do
      @res = @mysql.query "select 1,'abc',null"
    end
    it 'return result NULL as nil' do
      @res.fetch_row.should == [1, "abc", nil]
    end
  end

  describe '#query with multiple record result' do
    before do
      @res = @mysql.query "select 1,'abc',null union select 2,'def',1.23"
    end
    it 'return multiple record result' do
      @res.fetch_row.should == [1, "abc", nil]
      @res.fetch_row.should == [2, "def", "1.23"]
      @res.fetch_row.should == nil
    end
  end

  describe '#query with block' do
    it 'return self' do
      @mysql.query("select 1"){}.should == @mysql
    end
  end

  describe 'normal query and prepared statement returns same values for several columns' do
    it 'return appropriate value' do
      @mysql.query <<EOS
create temporary table t (
  bit bit(8),
  ti tinyint,
  si smallint,
  mi mediumint,
  i int,
  bi bigint,
  f float,
  d double,
  de decimal,
  date date,
  dt datetime,
  ts timestamp,
  time time,
  y year(4),
  c char(10),
  v varchar(10),
  bin binary(10),
  vb varbinary(10),
  tb tinyblob,
  tt tinytext,
  b blob,
  t text,
  mb mediumblob,
  mt mediumtext,
  lb longblob,
  lt longtext,
  en enum('1','2','3'),
  se set('1','2','3')
)
EOS
      @mysql.query "insert into t values (5,1,1,1,1,1,1.0,1.0,1,'2009-07-02','2009-07-02 07:38:45','2009-07-02 07:38:45','07:38:45',2009,'a','b','c','d','e','f','g','h','i','j','k','l','2','1,2,3')"
      expected = [
        "\005", 1, 1, 1, 1, 1, 1.0, 1.0, "1", Mysql::Time.new(2009,7,2,0,0,0),
        Mysql::Time.new(2009,7,2,7,38,45), Mysql::Time.new(2009,7,2,7,38,45),
        Mysql::Time.new(0,0,0,7,38,45), 2009,
        'a','b',"c\0\0\0\0\0\0\0\0\0",'d','e','f','g','h','i','j','k','l',
        '2','1,2,3'
      ]
      @mysql.query("select * from t").fetch.should == expected
      @mysql.prepare("select * from t").execute.fetch.should == expected
    end
  end

  describe '#statement without block' do
    it 'returns Mysql::Statement object' do
      @mysql.statement.should be_kind_of(Mysql::Statement)
    end
  end

  describe '#statement with block' do
    it 'returns value block returns' do
      @mysql.statement{123}.should === 123
    end
  end

  describe '#prepare without block' do
    it 'returns Mysql::Statement object' do
      @mysql.prepare("select 1").should be_kind_of(Mysql::Statement)
    end
  end

  describe '#prepare with block' do
    it 'returns value block returns' do
      @mysql.prepare("select 1,2"){|st| st.execute.fetch}.should == [1, 2]
    end
  end

  describe '#query as prepared statement' do
    before do
      @mysql.query "create temporary table t (id int auto_increment primary key) auto_increment=123"
      @mysql.query "insert into t values (?)", 0
    end
    it 'set affected_rows' do
      @mysql.affected_rows.should == 1
    end
    it 'set insert_id' do
      @mysql.insert_id.should == 123
    end
    it 'set warning_count' do
      @mysql.warning_count.should == 0
    end
  end

  describe 'simple query and prepared statement:' do
    describe 'signed integer:' do
      before do
        @mysql.query "create temporary table t (i1 tinyint, i2 smallint, i3 mediumint, i4 int, i8 bigint)"
        @st = @mysql.prepare "insert into t values (?,?,?,?,?)"
        @st2 = @mysql.prepare "select * from t"
      end
      after do
        @mysql.query "drop temporary table t"
      end
      it 'nil: store NULL' do
        rec = [nil, nil, nil, nil, nil]
        @st.execute *rec
        @st2.execute.fetch.should == rec
        @mysql.query("select * from t").fetch.should == rec
      end
      it '0: store 0' do
        rec = [0, 0, 0, 0, 0]
        @st.execute *rec
        @st2.execute.fetch.should == rec
        @mysql.query("select * from t").fetch.should == rec
      end
      it 'positive number: store correct value' do
        rec = [123, 123, 123, 123, 123]
        @st.execute *rec
        @st2.execute.fetch.should == rec
        @mysql.query("select * from t").fetch.should == rec
      end
      it 'negative number: store correct value' do
        rec = [-123, -123, -123, -123, -123]
        @st.execute *rec
        @st2.execute.fetch.should == rec
        @mysql.query("select * from t").fetch.should == rec
      end
      it 'positive maxmum number: store correct value' do
        rec = [127, 32767, 8388607, 2147483647, 9223372036854775807]
        @st.execute *rec
        @st2.execute.fetch.should == rec
        @mysql.query("select * from t").fetch.should == rec
      end
      it 'negative number: store correct value' do
        rec = [-128, -32768, -8388608, -2147483648, -9223372036854775808]
        @st.execute *rec
        @st2.execute.fetch.should == rec
        @mysql.query("select * from t").fetch.should == rec
      end
      it 'large value: ProtocolError' do
        proc{@st.execute(18446744073709551616,0,0,0,0)}.should raise_error(Mysql::ProtocolError, 'value too large: 18446744073709551616')
      end
    end
    describe 'unsigned integer:' do
      before do
        @mysql.query "create temporary table t (i1 tinyint unsigned, i2 smallint unsigned, i3 mediumint unsigned, i4 int unsigned, i8 bigint unsigned)"
        @st = @mysql.prepare "insert into t values (?,?,?,?,?)"
        @st2 = @mysql.prepare "select * from t"
      end
      after do
        @mysql.query "drop temporary table t"
      end
      it 'nil: store NULL' do
        rec = [nil, nil, nil, nil, nil]
        @st.execute *rec
        @st2.execute.fetch.should == rec
        @mysql.query("select * from t").fetch.should == rec
      end
      it '0: store 0' do
        rec = [0, 0, 0, 0, 0]
        @st.execute *rec
        @st2.execute.fetch.should == rec
        @mysql.query("select * from t").fetch.should == rec
      end
      it 'positive number: store correct value' do
        rec = [123, 123, 123, 123, 123]
        @st.execute *rec
        @st2.execute.fetch.should == rec
        @mysql.query("select * from t").fetch.should == rec
      end
      it 'maximum number: store correct value' do
        rec = [255, 65535, 16777215, 4294967295, 18446744073709551615]
        @st.execute *rec
        @st2.execute.fetch.should == rec
        @mysql.query("select * from t").fetch.should == rec
      end
      it 'large value: ProtocolError' do
        proc{@st.execute(-9223372036854775809,0,0,0,0)}.should raise_error(Mysql::ProtocolError, 'value too large: -9223372036854775809')
      end
    end
    describe 'floating point number:' do
      before do
        @mysql.query "create temporary table t (f float, d double)"
        @st = @mysql.prepare "insert into t values (?,?)"
        @st2 = @mysql.prepare "select * from t"
      end
      after do
        @mysql.query "drop temporary table t"
      end
      it 'nil: store NULL' do
        rec = [nil, nil]
        @st.execute *rec
        @st2.execute.fetch.should == rec
        @mysql.query("select * from t").fetch.should == rec
      end
      it '0: store 0' do
        rec = [0, 0]
        @st.execute *rec
        @st2.execute.fetch.should == rec
        @mysql.query("select * from t").fetch.should == rec
      end
      it 'positve number: store correct value' do
        rec = [1.25, 1.25]
        @st.execute *rec
        @st2.execute.fetch.should == rec
        @mysql.query("select * from t").fetch.should == rec
      end
      it 'negative number: store correct value' do
        rec = [-1.25, -1.25]
        @st.execute *rec
        @st2.execute.fetch.should == rec
        @mysql.query("select * from t").fetch.should == rec
      end
      it 'minimum number: store correct value' do
        @st.execute(1.175494351e-38, 2.2250738585072014e-308)
        @st.execute(-1.175494351e-38, -2.2250738585072014e-308)
        res = @st2.execute
        f, d = res.fetch
        (f-1.17549435082229e-38).abs.should <= 1.0e-52
        d.should == 2.2250738585072014E-308
        f, d = res.fetch
        (f+1.17549435082229e-38).abs.should <= 1.0e-52
        d.should == -2.2250738585072014E-308
      end
      it 'maximum number: store correct value' do
        @st.execute(3.402823466E+38, 1.7976931348623157E+308)
        @st.execute(-3.402823466E+38, -1.7976931348623157E+308)
        res = @st2.execute
        f, d = res.fetch
        (f-3.40282346638529e+38).abs.should <= 1.0e+24
        d.should == 1.7976931348623157E+308
        f, d = res.fetch
        (f+3.40282346638529e+38).abs.should <= 1.0e+24
        d.should == -1.7976931348623157E+308
      end
    end
    describe 'string:' do
      before do
        @mysql.query "create temporary table t (c1 varchar(10), c2 varchar(1000), c3 mediumtext, c4 longtext)"
        @st = @mysql.prepare "insert into t values (?,?,?,?)"
        @st2 = @mysql.prepare "select * from t"
      end
      after do
        @mysql.query "drop temporary table t"
      end
      it 'nil: store NULL' do
        rec = [nil, nil, nil, nil]
        @st.execute *rec
        @st2.execute.fetch.should == rec
        @mysql.query("select * from t").fetch.should == rec
      end
      it 'empty string: store empty string' do
        rec = ["", "", "", ""]
        @st.execute *rec
        @st2.execute.fetch.should == rec
        @mysql.query("select * from t").fetch.should == rec
      end
      it 'string: store string' do
        rec = ["aaa", "a"*500, "a"*100000, ""]
        @st.execute *rec
        @st2.execute.fetch.should == rec
        @mysql.query("select * from t").fetch.should == rec
      end
      it 'long string: store string' do
        n, v = @mysql.query("show variables like 'max_allowed_packet'").fetch
        if v.to_i > 30000000
          @st.execute "aaa", "a"*500, "a"*100000, "a"*16777216
          @st2.execute.fetch.map(&:length).should == [3, 500, 100000, 16777216]
          @mysql.query("select * from t").fetch.map(&:length).should == [3, 500, 100000, 16777216]
        end
      end
    end
    describe 'DECIMAL:' do
      before do
        @mysql.query "create temporary table t (d decimal(10,5))"
        @st = @mysql.prepare "insert into t values (?)"
        @st2 = @mysql.prepare "select * from t"
      end
      after do
        @mysql.query "drop temporary table t"
      end
      it 'nil: store NULL' do
        @st.execute nil
        @st2.execute.fetch.should == [nil]
        @mysql.query("select * from t").fetch.should == [nil]
      end
      it 'number: store correct value' do
        @st.execute 123.456
        @st2.execute.fetch.should == ["123.45600"]
        @mysql.query("select * from t").fetch.should == ["123.45600"]
      end
      it 'string: store correct value' do
        @st.execute "123.456"
        @st2.execute.fetch.should == ["123.45600"]
        @mysql.query("select * from t").fetch.should == ["123.45600"]
      end
    end
    describe 'datetime:' do
      before do
        @mysql.query "create temporary table t (d date, dt datetime, ts timestamp null default null, t time, y2 year(2), y4 year(4))"
        @st = @mysql.prepare "insert into t values (?,?,?,?,?,?)"
        @st2 = @mysql.prepare "select * from t"
      end
      after do
        @mysql.query "drop temporary table t"
      end
      it 'nil: store NULL' do
        @st.execute nil,nil,nil,nil,nil,nil
        @st2.execute.fetch.should == [nil,nil,nil,nil,nil,nil]
      end
      it 'value as string: store correct value' do
        @st.execute '2008-10-23','2008-10-23 21:04:07','2008-10-23 21:04:07','21:04:07','99','2008'
        expect = [
          Mysql::Time.new(2008,10,23),
          Mysql::Time.new(2008,10,23,21,4,7),
          Mysql::Time.new(2008,10,23,21,4,7),
          Mysql::Time.new(0,0,0,21,4,7),
          99,
          2008,
        ]
        @st2.execute.fetch.should == expect
        @mysql.query("select * from t").fetch.should == expect
      end
      it 'value as numeric: store correct value' do
        @st.execute 20081023,20081023210407,20081023210407,210407,8,2008
        expect = [
          Mysql::Time.new(2008,10,23),
          Mysql::Time.new(2008,10,23,21,4,7),
          Mysql::Time.new(2008,10,23,21,4,7),
          Mysql::Time.new(0,0,0,21,4,7),
          8,
          2008,
        ]
        @st2.execute.fetch.should == expect
        @mysql.query("select * from t").fetch.should == expect
      end
      it 'value as Mysql::Time: store correct value' do
        d = Mysql::Time.new(2008,10,23,21,4,7)
        @st.execute d,d,d,d,d,d
        expect = [
          Mysql::Time.new(2008,10,23),
          Mysql::Time.new(2008,10,23,21,4,7),
          Mysql::Time.new(2008,10,23,21,4,7),
          Mysql::Time.new(0,0,0,21,4,7),
          8,
          2008,
        ]
        @st2.execute.fetch.should == expect
        @mysql.query("select * from t").fetch.should == expect
      end
      describe 'retrieve TIME over 24 hour:' do
        it 'return correct value' do
          @mysql.query "insert into t (t) values ('101:11:22')"
          st = @mysql.prepare "select t from t"
          res = st.execute
          res.fetch.should == [Mysql::Time.new(0,0,0,101,11,22)]
          res.fetch.should == nil
          @mysql.query("select t from t").fetch.should == [Mysql::Time.new(0,0,0,101,11,22)]
        end
      end
      describe 'retrieve negative TIME:' do
        it 'return negative value' do
          @mysql.query "insert into t (t) values ('-100:11:22')"
          st = @mysql.prepare "select t from t"
          res = st.execute
          res.fetch.should == [Mysql::Time.new(0,0,0,100,11,22,true)]
          res.fetch.should == nil
          @mysql.query("select t from t").fetch.should == [Mysql::Time.new(0,0,0,100,11,22,true)]
        end
      end
      describe 'retrieve YEAR 99:' do
        it 'return 99' do
          @mysql.query "insert into t (y2) values (99)"
          st = @mysql.prepare "select y2 from t"
          res = st.execute
          res.fetch.should == [99]
          res.fetch.should == nil
          @mysql.query("select y2 from t").fetch.should == [99]
        end
      end
    end
    describe 'bit:' do
      before do
        @mysql.query "create temporary table t (b1 bit(1), b32 bit(32), b64 bit(64))"
        @st = @mysql.prepare "insert into t values (?,?,?)"
        @st2 = @mysql.prepare "select * from t"
      end
      it 'nil: store NULL' do
        @st.execute nil, nil, nil
        @st2.execute.fetch.should == [nil, nil, nil]
      end
      it '0: store 0' do
        @st.execute 0, 0, 0
        expect = ["\0", "\0\0\0\0", "\0\0\0\0\0\0\0\0"]
        @st2.execute.fetch.should == expect
        @mysql.query("select * from t").fetch.should == expect
      end
      it 'positive number: store correct value' do
        @st.execute 1, 128, 128
        expect = ["\1", "\0\0\0\200", "\0\0\0\0\0\0\0\200"]
        @st2.execute.fetch.should == expect
        @mysql.query("select * from t").fetch.should == expect
      end
      it 'maximum number: store correct value' do
        @st.execute 1, 2**32-1, 2**64-1
        expect = ["\1", "\377\377\377\377", "\377\377\377\377\377\377\377\377"]
        @st2.execute.fetch.should == expect
        @mysql.query("select * from t").fetch.should == expect
      end
    end
    describe 'mix some type:' do
      it 'return valid value' do
        @mysql.prepare("select ?,?,?").execute(nil,123,"abc").fetch.should == [nil,123,"abc"]
        @mysql.query("select null,123,'abc'").fetch.should == [nil,123,"abc"]
      end
    end
    describe 'unknown type:' do
      it 'ProtocolError' do
        proc{@mysql.prepare("select ?").execute(Object.new)}.should raise_error(Mysql::ProtocolError, 'class Object is not supported')
      end
    end
    describe 'parameter mismatch:' do
      it 'raise ClientError' do
        proc{@mysql.prepare("select ?").execute(1,2)}.should raise_error(Mysql::ClientError, 'parameter count mismatch')
      end
    end
  end

  describe '#charset=string' do
    it 'return string' do
      (@mysql.charset = "ujis").should == "ujis"
    end
    it '@mysql.charset is Mysql::Charset' do
      @mysql.charset = "ujis"
      @mysql.charset.should == Mysql::Charset.by_name("ujis")
    end
    it 'client charset is set' do
      @mysql.charset = "ujis"
      n, v = @mysql.query("show variables like 'character_set_client'").fetch
      v.should == "ujis"
    end
  end

  describe '#charset=obj_of_Mysql::Charset' do
    it 'return obj' do
      cs = Mysql::Charset.by_name "ujis"
      (@mysql.charset = cs).should == cs
    end
  end
end

describe 'Mysql::Statement' do
  before do
    my = Mysql.connect URL
    my.query "create temporary table t (a int not null, b char(10))"
    my.query "insert into t values (123,'abc'),(456,'def')"
    @st = my.statement
  end
  describe '#prepare' do
    it 'returns self' do
      @st.prepare("select 1").should == @st
    end
  end
  describe '#prepare with invalid query' do
    it 'raise ServerError' do
      proc{@st.prepare("xxxx")}.should raise_error(Mysql::ServerError, "You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near 'xxxx' at line 1")
      @st.sqlstate.should == "42000"
    end
  end
  describe '#execute' do
    it 'returns Result object' do
      @st.prepare "select 1"
      @st.execute.should be_kind_of Mysql::Result
    end
  end
  describe '#execute without prepare' do
    it 'raise ClientError' do
      proc{@st.execute}.should raise_error(Mysql::ClientError, 'not prepared')
    end
  end
  describe '#execute without required parameter' do
    it 'raise ClientError' do
      @st.prepare "select ?"
      proc{@st.execute}.should raise_error(Mysql::ClientError, 'parameter count mismatch')
    end
  end
  describe '#execute with invalid parameter' do
    it 'raise ServerError' do
      @st.prepare "insert into t values (?,?)"
      proc{@st.execute(nil, nil)}.should raise_error(Mysql::ServerError, "Column 'a' cannot be null")
      @st.sqlstate.should == "23000"
    end
  end
end

describe 'Mysql::Result' do
  before do
    my = Mysql.connect URL
    my.query "create temporary table t (a int, b char(10))"
    my.query "insert into t values (123,'abc'),(456,'def')"
    @res = my.query "select a,b from t"
    @enum_class = Enumerable::Enumerator rescue Enumerator
  end
  it '#fetch_row returns Array of String' do
    @res.fetch_row.should == [123, "abc"]
  end
  it '#fetch_hash returns Hash that key is column name' do
    @res.fetch_hash.should == {"a"=>123, "b"=>"abc"}
  end
  it '#fetch_hash(true) returns Hash that key is table name and column name' do
    @res.fetch_hash(true).should == {"t.a"=>123, "t.b"=>"abc"}
  end
  it '#each without block returns Enumerator' do
    e = @res.each
    e.should be_kind_of(@enum_class)
    e.entries.should == [[123,"abc"], [456,"def"]]
  end
  it '#each with block returns self' do
    rec = []
    @res.each{|r| rec.push r}.should == @res
    rec.should == [[123,"abc"], [456,"def"]]
  end
  it '#each_hash without block returns Enumerator' do
    e = @res.each_hash
    e.should be_kind_of(@enum_class)
    e.entries.should == [{"a"=>123,"b"=>"abc"}, {"a"=>456,"b"=>"def"}]
  end
  it '#each_hash with block returns self' do
    rec = []
    @res.each_hash{|r| rec.push r}.should == @res
    rec.should == [{"a"=>123,"b"=>"abc"}, {"a"=>456,"b"=>"def"}]
  end
end

describe 'Mysql::Field' do
  before do
    my = Mysql.connect URL
    my.query "create temporary table t (a int not null primary key, b int null, c char(10))"
    @a, @b, @c = my.prepare("select a,b,c from t").fields
  end
  describe '#is_num?' do
    it 'is true for numeric column' do
      @a.is_num?.should == true
    end
    it 'is true for string column' do
      @c.is_num?.should == false
    end
  end
  describe '#is_not_null?' do
    it 'is true for not null column' do
      @a.is_not_null?.should == true
    end
    it 'is false for null column' do
      @b.is_not_null?.should == false
    end
  end
  describe '#is_pri_key?' do
    it 'is true for primary key' do
      @a.is_pri_key?.should == true
    end
    it 'is false for primary key' do
      @b.is_pri_key?.should == false
    end
  end
end

describe 'Mysql::Time' do
  it '#== is true for same time' do
    t1 = Mysql::Time.new 2009, 2, 24, 1, 57, 15
    t2 = Mysql::Time.new 2009, 2, 24, 1, 57, 15
    (t1 == t2).should == true
  end
  it '#eql? is true for same time' do
    t1 = Mysql::Time.new 2009, 2, 24, 1, 57, 15
    t2 = Mysql::Time.new 2009, 2, 24, 1, 57, 15
    t1.eql?(t2).should == true
  end
  it '#to_s returns YYYY-MM-DD HH:MM:SS string' do
    t = Mysql::Time.new 2009, 2, 24, 1, 57, 15
    t.to_s.should == "2009-02-24 01:57:15"
  end
  it '#to_s for 0000-00-00 returns HH:MM:SS string' do
    t = Mysql::Time.new 0, 0, 0, 1, 57, 15
    t.to_s.should == "01:57:15"
  end
end

describe 'Mysql::ServerError' do
  before do
    my = Mysql.connect URL
    begin
      my.query("hoge")
    rescue Mysql::Error => @err
      nil
    end
  end
  it '#error is message' do
    @err.error.should == "You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near 'hoge' at line 1"
  end
  it '#errno is error number' do
    @err.errno.should == 1064
    @err.errno.should == Mysql::ServerError::ER_PARSE_ERROR
  end
  it '#sqlstate is sqlstate code' do
    @err.sqlstate.should == "42000"
  end
end

describe 'Mysql::Protocol' do
  describe '.new with "localhost"' do
    it 'use UNIXSocket' do
      UNIXSocket.should_receive(:new).with("socketfile")
      Mysql::Protocol.new("localhost", 9999, "socketfile", 0, 0, 0)
    end
  end
  describe '.new with "127.0.0.1"' do
    it 'use TCPSocket' do
      TCPSocket.should_receive(:new).with("127.0.0.1", 9999)
      Mysql::Protocol.new("127.0.0.1", 9999, "socketfile", 0, 0, 0)
    end
  end
  describe '.new over timeout' do
    it 'raises ClientError' do
      UNIXSocket.stub!(:new).and_return{sleep 999}
      proc{Mysql::Protocol.new("localhost", 9999, "socketfile", 1, 0, 0)}.should raise_error(Mysql::ClientError, 'connection timeout')
    end
  end
  describe '#read' do
    before do
      @sock = mock("Socket")
      UNIXSocket.stub!(:new).and_return @sock
    end
    it 'returns one packet' do
      @sock.should_receive(:read).and_return("\x06\x00\x00\x00", "abcdef")
      prot = Mysql::Protocol.new("localhost", 0, "socketfile", 0, 0, 0)
      prot.read.should == "abcdef"
    end
    describe 'invalid seq' do
      it 'raises ProtocolError' do
        @sock.should_receive(:read).and_return("\x06\x00\x00\x03")
        prot = Mysql::Protocol.new("localhost", 0, "socketfile", 0, 0, 0)
        proc{prot.read}.should raise_error(Mysql::ProtocolError, 'invalid packet: sequence number mismatch(3 != 0(expected))')
      end
    end
  end
end

describe 'Mysql::Protocol::ExecutePacket#null_bitmap:' do
  before do
    class Mysql::Protocol::ExecutePacket
      public :null_bitmap
    end
  end
  it 'return null bitmap' do
    st = Mysql::Protocol::ExecutePacket.allocate
    st.null_bitmap([nil]).should == "\x01"
    st.null_bitmap([nil,nil]).should == "\x03"
    st.null_bitmap([1,nil,nil]).should == "\x06"
    st.null_bitmap([nil,nil,nil,nil,nil,nil,nil,nil,nil]).should == "\xff\x01"
    st.null_bitmap([nil,1,nil,1,1,nil,nil,1,nil]).should == "\x65\x01"
  end
end
