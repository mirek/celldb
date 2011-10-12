require 'rubygems'
require 'digest'
require 'json'

class Hdb0
  
  RESULT_ERROR_INDEX_OUT_OF_RANGE = -1
  RESULT_ERROR_DATA_SIZE          = -2
  
  attr_accessor :level
  attr_accessor :path
  attr_accessor :file_size
  attr_accessor :index
  
  def initialize(level, path = :default)
    @level = level
    @level_length = level ** 2
    if path == :default
      @path = sprintf("lvl%02d.hdb", level)
    else
      @path = path
    end
  end
  
  # private
  def write(index, data)
    if data.length != @level_length
      # TODO: check for avail space
      file = open(path, 'a+')
      file.seek(index << level, IO::SEEK_SET)
      file.write data
      file.close
      index
    else
      RESULT_ERROR_DATA_SIZE
    end
  end
  
  def set(index, data)
    if index <= @index
      write(index, data)
    else
      RESULT_ERROR_INDEX_OUT_OF_RANGE
    end
  end
  
  def add(data)
    write(@index = @index + 1, data)
  end
  
  def get(index)
    if index <= @index
      IO.read(path, 1 << level, index << level)
    else
      RESULT_ERROR_INDEX_OUT_OF_RANGE
    end    
  end
  
end


class Hash

  PACK_TEMPLATE = "C Q a24"
  PACK_SIZE = 1 + 8 + 24

  attr_accessor :in_use
  attr_accessor :last_used_at
  attr_accessor :hash
  
  # Not serialized
  attr_accessor :index
  
  class << self
    
    def unpack(buffer)
      hash = Hash.new
      hash.in_use, hash.last_used_at, hash.hash = buffer.unpack(PACK_TEMPLATE)
      hash
    end
    
    def hash(blob)
      h = []
      (blob.size / 1024).times do |i|
        h << Digest::Tiger.digest(blob[(1024 * i)..(1024 * (i + 1))])
      end
      h << Digest::Tiger.digest(blob[((blob.size / 1024) * 1024)..(((blob.size / 1024) * 1024) + (blob.size % 1024))])
      while h.size > 1
        h = h.each_slice(2).map do |hl, hr|
          Digest::Tiger.digest(hl + (hr || ''))
        end
      end
      h.first
    end
    
  end
  
  # options:
  #   :blob
  def initialize(options = {})
    @last_used_at = Time.now.to_i
    @in_use = 1
    @hash = options.has_key?(:blob) ? Hash.hash(options[:blob]) : nil
    @index = -1
  end
  
  def pack
    array = [@in_use, @last_used_at, @hash].flatten
    puts array
      
    array.pack(PACK_TEMPLATE)
  end
  
end

class Hdb
  
  attr_accessor :level
  
  attr_accessor :hashes_filename
  attr_accessor :hashes_file
  
  attr_accessor :blobs_fileanme
  attr_accessor :blobs_file
  
  attr_accessor :hashes
  
  def initialize(options = {})
    @level = options[:level] || 16
    
    @hashes_filename = options[:hashes_filename] || sprintf("hdb-%d.hdh", @level)
    @hashes_file = open(@hashes_filename, "a+")
    
    @blobs_filename = options[:blobs_filename] || sprintf("hdb-%d.hdb", @level)
    @blobs_file = open(@blobs_filename, "a+")
    
    @hashes = {}
    hashes_load
  end
  
  def hashes_load
    @hashes_file.seek(0, IO::SEEK_SET)
    i = 0
    until @hashes_file.eof?
      hash = Hash.unpack(@hashes_file.read(Hash::PACK_SIZE))
      hashes[hash.hash] = hash
      i += 1
    end
  end
  
  def hashes_save
    @hashes_file.truncate
    @hashes_file.seek(0, IO::SEEK_SET)
  end
  
  def close
    @hash_file.close
    @data_file.close
  end
  
  def put(blob)
    hash = Hash.new :blob => blob
    unless exists?(hash.hash)
      
      # puts [hash.hash, @hashes[hash.hash].hash, hash.hash == @hashes[hash.hash].hash].join(' - ')
      
      if (blob.length <= (1 << level))
        at = @blobs_file.seek(0, IO::SEEK_END)
        @blobs_file.write blob
        @blobs_file.write "\0" * ((1 << level) - blob.length)
        hash.index = at << level
        @hashes[hash.hash] = hash
        @hashes_file.write(hash.pack)
      else
        printf("ERROR: blob length %d, expected <= %d", blob.length, at >> level)
      end
    else
      puts "already there"
    end
    hash
  end
  
  def exists?(hash)
    @hashes.has_key?(hash)
  end
  
  def get(hash)
  end
  
  def del(hash)
  end
  
end

class H
  attr_accessor :levels
  attr_accessor :hdbs
  attr_accessor :keys
  
  def initialize(options = {})
    @levels = options[:levels] || 10..20
    @hdbs = Hash[* @levels.map { |level| [level, Hdb.new(:level => level)] }.flatten]
    @keys = {}
    load
  end
  
  def save
    open("hdb.dat", "w").write(JSON.generate(@keys))
  end
  
  def load
    begin
      @keys = JSON.parse(open("hdb.dat").read)
    rescue
    end
  end
  
  def levels_for_size(n)
    ((n % 1024 > 0 ? [10] : []) + @levels.select do |level|
      ((1 << level) & n) > 0
    end).reverse
  end
  
  def del(key)
  end
  
  def set(key, blob)
    i = 0
    v = []
    levels_for_size(blob.size).each do |level|
      n = 1 << level
      hash = @hdbs[level].put(blob[i..(i+n)])
      v += [i, level, hash]
      i += n
    end
    if @keys.has_key?(key)
      del(key)
    end
    @keys[key] = v
    save
  end
  
  def get(key)
  end
  
  # def get(level, hash)
  #   @hdbs[level].get(hash)
  # end
  
end

h = H.new
h.set("foo2", "bar")

# hdb64k = Hdb.new :level => 16
# hdb64k.put "Hello"
# 
# puts "hashes:"
# puts hdb64k.hashes
# hdb64k.close

