require 'celldb/vector'
require 'celldb/vector/file'
require 'celldb/vector/meta'
require 'celldb/vector/data'
require 'celldb/digest'

class CellDB

  class << self
    
    # Returns an array of levels for specified number, in other words offsets of bits == 1
    # Please note the array is returned in descending order to support "natural" slicing, see slice method.
    def levels_for(n)
      n.to_i.to_s(2).split('').reverse.each_with_index.reject { |bit, i| bit == '0' }.map { |bit, i| i }.reverse
    end
    
    def slice_with_block(data)
      offset = 0
      levels_for(data.size).each do |level|
        length = 1 << level
        yield level, offset, data[offset..(offset + length - 1)]
        offset += length
      end
    end
    
    # Yields [level, offset, data] for data
    def slice(data, &block)
      if block_given?
        slice_with_block data, &block
      else
        slices = []
        slice_with_block(data) do |level, offset, data|
          slices << [level, offset, data]
        end
        slices
      end
    end
    
    def keyize(data)
      [data.size, slice(data).map do |level, offset, data|
        CellDB::Digest.digest(data)
      end]
    end
    
    def key_to_human_readable(size, digests, limit = 4)
      sprintf("[%d %s]", size, digests.map { |digest| CellDB::Digest.digest_to_hex(digest)[0..(limit - 1)] }.join(' ') )
    end

  end

  attr_accessor :level_range
  attr_accessor :levels
  attr_accessor :root

  def initialize(options = {})
    @level_range = options[:level_range] || (0..(32 - 1))
    @root = options[:root] || "cell.db"
    @levels = @level_range.map { |i| Vector.new(:level => i, :root => "#{@root}") }
    @key_paths = @level_range.map { |i| CellDB::Vector::File.touch(sprintf("#{@root}/%02d.kdb", i)) }
    @key_files = @key_paths.map { |key_path| open(key_path, 'r+') }
  end
  
  def close
    @levels.each { |level| level.close }
    @key_files.each { |key_file| key_file.close }
  end

  # Returns [size, digests] tuple, where digest.size == levels_for(size) and levels appear as the most significant cell first
  def write(bytes)
    size = nil
    digests = nil
    if bytes
      size = 0
      digests = CellDB.slice(bytes).map do |level, offset, data|
        size += data.size
        @levels[level].write(data).last
      end
    end
    [size, digests]
  end

  # Read data with size and digests, where digests.size == levels_for(size) and levels appear as the most significant cell first
  def read_with_key(size, digests)
    CellDB.levels_for(size).each_with_index do |level, i|
      @levels[level].read(digests[i]) || raise
    end.join
  end
  
  def key_size(n)
    8 + 24 * n
  end
  
  def key_format(n)
    "Q" + " a24" * n
  end

  def pack_key(size, digests)
    ([size] + digests).pack(key_format(levels_for(n).size))
  end
  
  def unpack_key(data)
    data.unpack(key_format((data.size - 8) / 24)) if data
  end

  def append_key(size, digests)
    n = levels_for(size).size
    key_file = @key_files[n]
    key_file.seek 0, IO::SYNC_END
    key_file.write [size] + digests.flatten.pack(key_format(n))
    key_file.tell
  end
  
  def write_key(size, digests)
    key_file(levels_for(size).size).write(pack_key(size, digests))
  end

  def write_key_at(index, size, digests)
    n = levels_for(size).size
    seek_key_at(n, index)
    write_key(size, digests)
  end

  def key_file(n)
    @key_files[n]
  end

  def read_key(n)
    unpack_key(data) if key_size(n) == (data = key_file(n).read(key_size(n))).size
  end

  def seek_key_at(n, index)
    key_file(n).seek index * key_size(n), IO::SYNC_SET
  end

  def read_key_at(n, index)
    seek_key_at(n, index)
    read_key(n)
  end

end
