require 'digest/sha2'

class Mysql
  class Authenticator
    class CachingSha2Password
      # @param protocol [Mysql::Protocol]
      def initialize(protocol)
        @protocol = protocol
      end

      # @return [String]
      def name
        'caching_sha2_password'
      end

      # @param passwd [String]
      # @param scramble [String]
      # @yield [String] hashed password
      # @return [Mysql::Packet]
      def authenticate(passwd, scramble)
        yield hash_password(passwd, scramble)
        pkt = @protocol.read
        data = pkt.to_s
        if data.size == 2 && data[0] == "\x01"
          case data[1]
          when "\x03"  # fast_auth_success
            # OK
          when "\x04"  # perform_full_authentication
            raise 'Authentication requires secure connection (not supported)'
          else
            raise "invalid auth reply packet: #{data.inspect}"
          end
          pkt = @protocol.read
        end
        return pkt
      end

      # @param passwd [String]
      # @param scramble [String]
      # @return [String] hashed password
      def hash_password(passwd, scramble)
        return '' if passwd.nil? or passwd.empty?
        hash1 = Digest::SHA256.digest(passwd)
        hash2 = Digest::SHA256.digest(hash1)
        hash3 = Digest::SHA256.digest(hash2 + scramble)
        hash1.unpack("C*").zip(hash3.unpack("C*")).map{|a, b| a ^ b}.pack("C*")
      end
    end
  end
end
