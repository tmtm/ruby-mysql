class Mysql
  # @!visibility public
  # Field class
  class Field
    # @return [String] database name
    attr_reader :db
    # @return [String] table name
    attr_reader :table
    # @return [String] original table name
    attr_reader :org_table
    # @return [String] field name
    attr_reader :name
    # @return [String] original field name
    attr_reader :org_name
    # @return [Integer] charset id number
    attr_reader :charsetnr
    # @return [Integer] field length
    attr_reader :length
    # @return [Integer] field type
    attr_reader :type
    # @return [Integer] flag
    attr_reader :flags
    # @return [Integer] number of decimals
    attr_reader :decimals
    # @return [String] defualt value
    attr_reader :default
    alias def default

    # @private
    attr_accessor :result

    # @attr [Protocol::FieldPacket] packet
    def initialize(packet)
      @db, @table, @org_table, @name, @org_name, @charsetnr, @length, @type, @flags, @decimals, @default =
        packet.db, packet.table, packet.org_table, packet.name, packet.org_name, packet.charsetnr, packet.length, packet.type, packet.flags, packet.decimals, packet.default
      @flags |= NUM_FLAG if is_num_type?
      @max_length = nil
    end

    # @return [Hash] field information
    def to_hash
      {
        "name"       => @name,
        "table"      => @table,
        "def"        => @default,
        "type"       => @type,
        "length"     => @length,
        "max_length" => max_length,
        "flags"      => @flags,
        "decimals"   => @decimals,
      }
    end

    # @private
    def inspect
      "#<Mysql::Field:#{@name}>"
    end

    # @return [Boolean] true if numeric field.
    def is_num?
      @flags & NUM_FLAG != 0
    end

    # @return [Boolean] true if not null field.
    def is_not_null?
      @flags & NOT_NULL_FLAG != 0
    end

    # @return [Boolean] true if primary key field.
    def is_pri_key?
      @flags & PRI_KEY_FLAG != 0
    end

    # @return [Integer] maximum width of the field for the result set
    def max_length
      return @max_length if @max_length
      @max_length = 0
      @result&.calculate_field_max_length
      @max_length
    end

    attr_writer :max_length

    private

    def is_num_type?
      [TYPE_DECIMAL, TYPE_TINY, TYPE_SHORT, TYPE_LONG, TYPE_FLOAT, TYPE_DOUBLE, TYPE_LONGLONG, TYPE_INT24].include?(@type) || (@type == TYPE_TIMESTAMP && (@length == 14 || @length == 8))
    end
  end
end
