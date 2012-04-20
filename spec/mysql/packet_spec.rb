$LOAD_PATH.unshift "#{File.dirname __FILE__}/../lib"

require 'mysql/packet'

describe Mysql::Packet do
  def self._(s)
    s.unpack('H*').first
  end
  subject{Mysql::Packet.new(data)}
  describe '#lcb' do
    [
      ["\xfb",                                 nil],
      ["\xfc\x01\x02",                         0x0201],
      ["\xfd\x01\x02\x03",                     0x030201],
      ["\xfe\x01\x02\x03\x04\x05\x06\x07\x08", 0x0807060504030201],
      ["\x01",                                 0x01],
    ].each do |data, result|
      context "for '#{_ data}'" do
        let(:data){data}
      it{subject.lcb.should == result}
      end
    end
  end

  describe '#lcs' do
    context "for '03414243'" do
      let(:data){"\x03\x41\x42\x43"}
      it{subject.lcs.should == 'ABC'}
    end
  end

  describe '#read' do
    let(:data){'ABCDEFGHI'}
    it{subject.read(7).should == 'ABCDEFG'}
  end

  describe '#string' do
    let(:data){"ABC\0DEF"}
    it 'should NUL terminated String' do
      subject.string.should == 'ABC'
    end
  end

  describe '#utiny' do
    [
      ["\x01", 0x01],
      ["\xFF", 0xff],
    ].each do |data, result|
      context "for '#{_ data}'" do
        let(:data){data}
        it{subject.utiny.should == result}
      end
    end
  end

  describe '#ushort' do
    [
      ["\x01\x02", 0x0201],
      ["\xFF\xFE", 0xfeff],
    ].each do |data, result|
      context "for '#{_ data}'" do
        let(:data){data}
        it{subject.ushort.should == result}
      end
    end
  end

  describe '#ulong' do
    [
      ["\x01\x02\x03\x04", 0x04030201],
      ["\xFF\xFE\xFD\xFC", 0xfcfdfeff],
    ].each do |data, result|
      context "for '#{_ data}'" do
        let(:data){data}
        it{subject.ulong.should == result}
      end
    end
  end

  describe '#eof?' do
    [
      ["\xfe\x00\x00\x00\x00", true],
      ["ABCDE", false],
    ].each do |data, result|
      context "for '#{_ data}'" do
        let(:data){data}
        it{subject.eof?.should == result}
      end
    end
  end

end
