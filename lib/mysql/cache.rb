# Copyright (C) 2008 TOMITA Masahiro
# mailto:tommy@tmtm.org

class Mysql
  class Cache
    def initialize(size)
      @size = size || 0
      @cache = {}
      @timestamp = {}
    end
    def get(key)
      if @size <= 0
        return yield key
      end
      if @cache.key? key
        @timestamp[key] = ::Time.now
        return @cache[key]
      end
      if @cache.size >= @size
        oldest_key = @timestamp.min_by{|k,v| v}.first
        @cache.delete oldest_key
      end
      @cache[key] = yield key
    end
  end
end
