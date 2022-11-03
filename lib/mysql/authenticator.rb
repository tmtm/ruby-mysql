class Mysql
  # authenticator
  class Authenticator
    @plugins = {}

    # @param plugin [String]
    def self.plugin_class(plugin)
      return @plugins[plugin] if @plugins[plugin]

      raise ClientError, "invalid plugin name: #{plugin}" unless plugin.match?(/\A\w+\z/)
      begin
        require_relative "authenticator/#{plugin}"
      rescue LoadError
        return nil
      end
      class_name = plugin.gsub(/(?:^|_)(.)/){$1.upcase}
      raise ClientError, "#{class_name} is undefined" unless self.const_defined? class_name
      klass = self.const_get(class_name)
      @plugins[plugin] = klass
      return klass
    end

    def initialize(protocol)
      @protocol = protocol
    end

    # @param plugin [String]
    def get(plugin)
      self.class.plugin_class(plugin)
    end

    # @param plugin [String]
    def get!(plugin)
      get(plugin) or raise ClientError, "unknown plugin: #{plugin}"
    end

    def authenticate(user, passwd, db, scramble, plugin_name, connect_attrs)
      plugin = (get(plugin_name) || DummyPlugin).new(@protocol)
      pkt = plugin.authenticate(passwd, scramble) do |hashed|
        @protocol.write Protocol::AuthenticationPacket.serialize(@protocol.client_flags, 1024**3, @protocol.charset.number, user, hashed, db, plugin.name, connect_attrs)
      end
      while true
        res = Protocol::AuthenticationResultPacket.parse(pkt)
        case res.result
        when 0  # OK
          break
        when 2  # multi factor auth
          raise ClientError, 'multi factor authentication is not supported'
        when 254  # change auth plugin
          plugin = get!(res.auth_plugin).new(@protocol)
          pkt = plugin.authenticate(passwd, res.scramble) do |hashed|
            if passwd.nil? || passwd.empty?
              @protocol.write "\0"
            else
              @protocol.write hashed
            end
          end
        else
          raise ClientError, "invalid packet: #{pkt}"
        end
      end
    end

    # dummy plugin
    class DummyPlugin
      # @param protocol [Mysql::Protocol]
      def initialize(protocol)
        @protocol = protocol
      end

      # @return [String]
      def name
        ''
      end

      # @param passwd [String]
      # @param scramble [String]
      # @yield [String] hashed password
      # @return [Mysql::Packet]
      def authenticate(passwd, scramble)  # rubocop:disable Lint/UnusedMethodArgument
        yield ''
        @protocol.read
      end
    end
  end
end
