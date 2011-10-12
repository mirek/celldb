require 'rubygems'
require 'digest/tiger'

class CellDB

  class Digest

    class << self

      def digest(data)
        h = []
        (data.size / 1024).times do |i|
          h << ::Digest::Tiger.digest(data[(1024 * i)..(1024 * (i + 1))])
        end
        h << ::Digest::Tiger.digest(data[((data.size / 1024) * 1024)..(((data.size / 1024) * 1024) + (data.size % 1024))])
        while h.size > 1
          h = h.each_slice(2).map do |hl, hr|
            ::Digest::Tiger.digest(hl + (hr || ''))
          end
        end
        h.first
      end
      
      def hex_digest(data)
        digest_to_hex(digest(data))
      end

      def digest_to_hex(digest)
        if digest && digest.size == 24
          sprintf("%02X" * 24, *digest.bytes)
        else
          sprintf("--" * 24)
        end
      end

    end

  end

end
