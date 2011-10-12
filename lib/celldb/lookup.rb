
class CellDB
  
  class Key
    
    attr_accessor :byte
    attr_accessor :keys
    attr_accessor :refs
    
    def initialize(options = {})
      @byte = options[:byte]
      @keys = []
      @refs = []
    end
    
    def set(key, ref)
      byte = key[0]
      if byte
        unless @refs.find_index(byte)
          @refs
        end
      end
    end
    
    def dump(n = 0)
      puts (' ' * n) + byte.chr + ':'
      @refs.each do |ref|
        puts (' ' * n) + '- ' + ref
      end
      @keys.each do | key|
        key.dump(n + 1)
      end
    end
    
  end
  
end


# root = CellDB::Key.new
# root.set 'foo', 1
# root.set 'bar', 2
# root.dump
