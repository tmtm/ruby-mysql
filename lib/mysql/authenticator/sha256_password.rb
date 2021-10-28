require 'openssl'

class Mysql
  class Authenticator
    class Sha256Password
      # @param protocol [Mysql::Protocol]
      def initialize(protocol)
        @protocol = protocol
      end

      # @return [String]
      def name
        'sha256_password'
      end

      # @param passwd [String]
      # @param scramble [String]
      # @yield [String] hashed password
      # @return [Mysql::Packet]
      def authenticate(passwd, scramble)
        yield "\x01"  # request public key
        pkt = @protocol.read
        data = pkt.to_s
        if data[0] == "\x01"
          pkt.utiny # skip
          pubkey = pkt.to_s
          hash = (passwd+"\0").unpack("C*").zip(scramble.unpack("C*")).map{|a, b| a ^ b}.pack("C*")
          enc = OpenSSL::PKey::RSA.new(pubkey).public_encrypt(hash, OpenSSL::PKey::RSA::PKCS1_OAEP_PADDING)
          @protocol.write enc
          pkt = @protocol.read
        end
        return pkt
      end
    end
  end
end
