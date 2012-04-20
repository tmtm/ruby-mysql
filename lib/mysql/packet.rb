class Mysql
  class Packet
    def initialize(data)
      @data = data
    end

    def lcb
      return nil if @data.empty?
      case v = utiny
      when 0xfb
        return nil
      when 0xfc
        return ushort
      when 0xfd
        c, v = utiny, ushort
        return (v << 8)+c
      when 0xfe
        v1, v2 = ulong, ulong
        return (v2 << 32)+v1
      else
        return v
      end
    end

    def lcs
      len = self.lcb
      return nil unless len
      @data.slice!(0, len)
    end

    def read(len)
      @data.slice!(0, len)
    end

    def string
      str = @data.unpack('Z*').first
      @data.slice!(0, str.length+1)
      str
    end

    def utiny
      @data.slice!(0, 1).unpack('C').first
    end

    def ushort
      @data.slice!(0, 2).unpack('v').first
    end

    def ulong
      @data.slice!(0, 4).unpack('V').first
    end

    def eof?
      @data[0] == ?\xfe && @data.length == 5
    end

    def to_s
      @data
    end

  end
end
