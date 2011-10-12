
class CellDB
  
  class Vector
    
    # Blocks array
    class Blocks

      # Level of the blocks vector.
      attr_accessor :level

      # Path to the blocks vector, ie. cell.db/01.db
      attr_accessor :path

      def initialize(options = {})
        @path = options[:path]
        @level = options[:level]
        
        # Make sure the path is valid
        unless ::File.exists?(@path)
          FileUtils.mkdir_p(::File.dirname(@path))
          FileUtils.touch(@path)
        end
        
        @file = open(@path, 'r+')
      end
      
      def pack_size
        1 << @level
      end
      
      def close
        @file.close
      end
      
      # TODO: Potentially dangerous, make sure when writing < 0 indexes we've got proper file size (not in the middle of writing)
      def seek_at(index)
        @file.seek(index << @level, index < 0 ? IO::SEEK_END : IO::SEEK_SET)
      end
      
      def read_at(index, offset, length)
        seek_to index
        @file.seek offset, IO::SEEK_CUR
        @file.read length
      end
      
      def write_at(index, offset, length, data)
        seek_at index
        @file.seek offset, IO::SEEK_CUR
        @file.write data[0..length]
        @file.tell
      end
      
    end
    
  end
  
end

# b = CellDB::Vector::Blocks.new :path  => 'foo.dat',
#                                :level => 2
# b.write_at 1, 0, 4, '4444' 
# 
# b.close
