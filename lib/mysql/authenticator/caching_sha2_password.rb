require 'digest/sha2'

class Mysql
  class Authenticator
    # caching_sha2_password
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
            if @protocol.client_flags & CLIENT_SSL != 0
              @protocol.write passwd+"\0"
            elsif !@protocol.get_server_public_key
              raise ClientError::AuthPluginErr, 'Authentication requires secure connection'
            else
              @protocol.write "\2"  # request public key
              pkt = @protocol.read
              pkt.utiny # skip
              pubkey = pkt.to_s
              hash = (passwd+"\0").unpack("C*").zip(scramble.unpack("C*")).map{|a, b| a ^ b}.pack("C*")
              enc = OpenSSL::PKey::RSA.new(pubkey).public_encrypt(hash, OpenSSL::PKey::RSA::PKCS1_OAEP_PADDING)
              @protocol.write enc
            end
          else
            raise ClientError, "invalid auth reply packet: #{data.inspect}"
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
