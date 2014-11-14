# coding: binary
require 'test/unit'
require 'test/unit/rr'
begin
  require 'test/unit/notify'
rescue LoadError
  # ignore
end

require 'mysql'

class TestMysqlPacket < Test::Unit::TestCase
  def self._(s)
    s.unpack('H*').first
  end

  def subject
    Mysql::Packet.new(data)
  end

  sub_test_case '#lcb' do
    [
      ["\xfb",                                 nil],
      ["\xfc\x01\x02",                         0x0201],
      ["\xfd\x01\x02\x03",                     0x030201],
      ["\xfe\x01\x02\x03\x04\x05\x06\x07\x08", 0x0807060504030201],
      ["\x01",                                 0x01],
    ].each do |data, result|
      sub_test_case "for '#{_ data}'" do
        define_method(:data){ data }
        test '' do
          assert{ subject.lcb == result }
        end
      end
    end
  end

  sub_test_case '#lcs' do
    [
      ["\x03\x41\x42\x43", 'ABC'],
      ["\x01",             ''],
      ["",                 nil],
    ].each do |data, result|
      sub_test_case "for '#{_ data}'" do
        define_method(:data){ data }
        test '' do
          assert{ subject.lcs == result }
        end
      end
    end
  end

  sub_test_case '#read' do
    define_method(:data){'ABCDEFGHI'}
    test '' do
      assert{ subject.read(7) == 'ABCDEFG' }
    end
  end

  sub_test_case '#string' do
    define_method(:data){"ABC\0DEF"}
    test 'should NUL terminated String' do
      assert{ subject.string == 'ABC' }
    end
  end

  sub_test_case '#utiny' do
    [
      ["\x01", 0x01],
      ["\xFF", 0xff],
    ].each do |data, result|
      sub_test_case "for '#{_ data}'" do
        define_method(:data){data}
        test '' do
          assert{ subject.utiny == result }
        end
      end
    end
  end

  sub_test_case '#ushort' do
    [
      ["\x01\x02", 0x0201],
      ["\xFF\xFE", 0xfeff],
    ].each do |data, result|
      sub_test_case "for '#{_ data}'" do
        define_method(:data){data}
        test '' do
          assert{ subject.ushort == result }
        end
      end
    end
  end

  sub_test_case '#ulong' do
    [
      ["\x01\x02\x03\x04", 0x04030201],
      ["\xFF\xFE\xFD\xFC", 0xfcfdfeff],
    ].each do |data, result|
      sub_test_case "for '#{_ data}'" do
        define_method(:data){data}
        test '' do
          assert{ subject.ulong == result }
        end
      end
    end
  end

  sub_test_case '#eof?' do
    [
      ["\xfe\x00\x00\x00\x00", true],
      ["ABCDE", false],
    ].each do |data, result|
      sub_test_case "for '#{_ data}'" do
        define_method(:data){data}
        test '' do
          assert{ subject.eof? == result }
        end
      end
    end
  end

  sub_test_case 'Mysql::Packet.lcb' do
    [
      [nil,      "\xfb"],
      [1,        "\x01"],
      [250,      "\xfa"],
      [251,      "\xfc\xfb\x00"],
      [65535,    "\xfc\xff\xff"],
      [65536,    "\xfd\x00\x00\x01"],
      [16777215, "\xfd\xff\xff\xff"],
      [16777216, "\xfe\x00\x00\x00\x01\x00\x00\x00\x00"],
      [0xffffffffffffffff, "\xfe\xff\xff\xff\xff\xff\xff\xff\xff"],
    ].each do |val, result|
      sub_test_case "with #{val}" do
        test '' do
          assert{ Mysql::Packet.lcb(val) == result }
        end
      end
    end
  end

  sub_test_case 'Mysql::Packet.lcs' do
    test '' do
      assert{ Mysql::Packet.lcs("hoge") == "\x04hoge" }
      assert{ Mysql::Packet.lcs("あいう".force_encoding("UTF-8")) == "\x09\xe3\x81\x82\xe3\x81\x84\xe3\x81\x86" }
    end
  end
end
