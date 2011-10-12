
class CellDB
  
  class Vector
    
    attr_accessor :level

    def initialize(options = {})
      @level = options[:level].to_i
      @root = options[:root] || "cell.db"
      @meta = Meta.new(:path => options[:meta_path] || sprintf("%s/%02d.ma", @root, @level))
      @data = Blocks.new(:path => options[:data_path] || sprintf("%s/%02d.db", @root, @level), :level => @level)
    end

    def retain_count(digest)
      raise 'Not impmeneted'
    end

    def retain(digest)
      raise 'Not impmeneted'
    end
    
    def release(digest)
      raise 'Not impmeneted'
    end
    
    def read(digest)
      raise 'Not impmeneted'
    end
    
    def block_size
      1 << @level
    end
    
    # Returns [index, retain_count, digest] or nil
    def write(data)
      result = nil
      if data.size == block_size
        digest = CellDB::Digest.digest(data)
        index, retain_count = @meta.retain(digest)
        if retain_count == 1 # aka new data
          data_offset_after_write = @data.write_at(index, 0, block_size, data)
          # raise "#{data_index} != #{index} || #{CellDB::Digest.digest_to_hex(data_digest)} != #{CellDB::Digest.digest_to_hex(digest)}" unless ((data_index == index) && (data_digest == digest))
        end
        result = [index, retain_count, digest]
      else
        raise "data.size #{data.size} != block_size #{block_size}"
      end
      result
    end

    def close
      @meta.close
      @data.close
    end

  end
  
end
