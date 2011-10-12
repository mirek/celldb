require 'rubygems'
require 'fileutils'
require 'digest/tiger'

class CellDB
  
  class Vector
    
    class File
      
      class << self
        
        def touch(path)
          unless ::File.exists?(path)
            FileUtils.mkdir_p(::File.dirname(path))
            FileUtils.touch(path)
          end
          path
        end
        
      end
      
      attr_accessor :path
      attr_accessor :pack_size
      attr_accessor :pack_template
      
      def initialize(options = {})
        @path = options[:path]
        CellDB::Vector::File.touch(@path)
        @file = open(@path, options[:writable] ? 'r+' : 'r')
        @pack_size = options[:pack_size]
        @pack_template = options[:pack_template]
      end
      
      def readonly?
        @file.readonly?
      end
      
      def writable?
        @file.writable?
      end
      
      def eof?
        @file.eof?
      end
      
      def seek_at(index)
        @file.seek(@pack_size * index, index < 0 ? IO::SEEK_END : IO::SEEK_SET)
      end
      
      def read_at(index)
        seek_at(index)
        read
      end
      
      def read_last
        seek_at -1
        read
      end
      
      def read
        @file.read(@pack_size).unpack(@pack_template)
      end
      
      # Yields [index, hash, retain_count]
      def read_all(&block)
        seek_at 0
        i = -1
        yield *([i += 1] + read) until eof?
      end
      
      def write_at(index, data)
        seek_at index
        write data
      end
      
      def write(data)
        @file.write(data.pack(@pack_template))
      end
      
      def remove_at(index)
        write_at index, read_last
        trim_last
      end
      
      def trim_last
        seek_at -1
        @file.truncate @file.tell
      end
      
      def close
        @file.close
      end
      
    end
    
  end
  
end
