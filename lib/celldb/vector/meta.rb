
# # f.write_at_index 2, [Digest::Tiger.digest('bar2'), 5]
# 
# f.remove_at 1
# 
# f.read_all do |hash, retain_count|
#   puts [sprintf("%02X" * 24, *hash.bytes), retain_count].join("\t")
# end

class CellDB
  
  class Vector
    
    class Meta

      # Array[index] -> [ index, digest, retain_count ] tuple
      attr_accessor :indices

      # Hash[digest] -> [ index, digest, retain_count ] tuple
      attr_accessor :digests
      
      INDEX_INDEX        = 0
      DIGEST_INDEX       = 1
      RETAIN_COUNT_INDEX = 2

      def initialize(options = {})
        @file = CellDB::Vector::File.new :path          => options[:path],
                                         :writable      => true,
                                         :pack_size     => 24 + 8,
                                         :pack_template => 'a24 Q'

        @indices = []
        @digests = {}
        @file.read_all do |index, digest, retain_count|
          entry = []
          entry[INDEX_INDEX] = index
          entry[DIGEST_INDEX] = digest
          entry[RETAIN_COUNT_INDEX] = retain_count
          @indices << entry
          @digests[digest] = entry
        end
      end
      
      # Returns [index, digest, retain_count] entry.
      # If the retain_count == 1, then the digest was new
      # If the retain_count > 1 then the digest was already there
      def retain(digest, retain_count = 1)
        entry = nil
        if @digests.has_key?(digest)
          entry = @digests[digest]
          entry[RETAIN_COUNT_INDEX] += retain_count
        else
          entry = []
          entry[INDEX_INDEX] = count
          entry[DIGEST_INDEX] = digest
          entry[RETAIN_COUNT_INDEX] = retain_count
          @indices << entry
          @digests[digest] = entry
        end
        @file.write_at entry[INDEX_INDEX], [entry[DIGEST_INDEX], entry[RETAIN_COUNT_INDEX]]
        [entry[INDEX_INDEX], entry[RETAIN_COUNT_INDEX]]
      end

      def release(digest, retain_count = -1)
        entry = nil
        if digest_exists?(digest)
          entry = @digests[digest]
          if (entry[RETAIN_COUNT_INDEX] += retain_count) == 0
            @file.remove_at entry[INDEX_INDEX]
          else
            @file.write_at entry[INDEX_INDEX], [entry[DIGEST_INDEX], entry[RETAIN_COUNT_INDEX]]
          end
        end
        [entry[INDEX_INDEX], entry[RETAIN_COUNT_INDEX]]
      end
      
      def digest_exists?(digest)
        @digests.has_key?(digest)
      end

      def count
        @indices.size
      end

      def close
        @file.close
      end
      
      def self.dump_entry(entry)
        if entry.is_a?(Array) && entry.size == 3
          puts [entry[INDEX_INDEX], sprintf("%02X" * 24, *entry[DIGEST_INDEX].bytes), entry[RETAIN_COUNT_INDEX]].join("\t")
        else
          puts ['-', '-' * 24 * 2, '-'].join("\t")
        end
      end
      
      def dump
        @indices.each do |entry|
          self.class.dump_entry entry
        end
      end

    end
    
  end
  
end

# m = CellDB::Vector::Meta.new :path => 'foo/bar.dat'
# CellDB::Vector::Meta.dump_entry m.release(Digest::Tiger.digest('bar'))
# puts
# m.dump
# m.close
