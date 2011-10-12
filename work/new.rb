
# require 'hdb/array'

module Hdb

  # Returns number of 1 bits in a number
  def nbits(n)
    n.to_i.to_s(2).split('').select { |bit| bit == '1' }.size
  end

  # Returns an array of 1 bit positions 
  def nlevels(n)
    n.to_i.to_s(2).split('').reverse.each_with_index.map { |bit, i| [i, bit] }.reject { |tuple| tuple.last == '0' }
  end

end
