require 'digest/sha1'

class Mysql
  class Authenticator
    class MysqlNativePassword
      # @param protocol [Mysql::Protocol]
      def initialize(protocol)
        @protocol = protocol
      end

      # @return [String]
      def name
        'mysql_native_password'
      end

      # @param passwd [String]
      # @param scramble [String]
      # @yield [String] hashed password
      # @return [Mysql::Packet]
      def authenticate(passwd, scramble)
        yield hash_password(passwd, scramble)
        @protocol.read
      end

      # @param passwd [String]
      # @param scramble [String]
      # @return [String] hashed password
      def hash_password(passwd, scramble)
        return '' if passwd.nil? or passwd.empty?
        hash1 = Digest::SHA1.digest(passwd)
        hash2 = Digest::SHA1.digest(hash1)
        hash3 = Digest::SHA1.digest(scramble + hash2)
        hash1.unpack("C*").zip(hash3.unpack("C*")).map{|a, b| a ^ b}.pack("C*")
      end
    end
  end
end
