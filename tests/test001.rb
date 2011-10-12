
require 'test_helper'

class Test001 < Test::Unit::TestCase

  def setup
    FileUtils.rm_r("test001.db")
    @celldb = CellDB.new :root => "test001.db", :level_range => 0..9
  end

  def teardown
  end

  def test_digest
    assert_equal CellDB::Digest.digest_to_hex(CellDB::Digest.digest("")), "3293AC630C13F0245F92BBB1766E16167A4E58492DDE73F3"
  end
  
  def test_levels_for
    assert_equal [10], CellDB.levels_for(1024)
    assert_equal (0..9).to_a.reverse, CellDB.levels_for(1023)
  end
  
  def test_slice
    assert_equal ['01234567', 'ABCD', '01', 'A'], CellDB.slice("01234567ABCD01A").map(&:last)
  end
  
  def test_write
    size, digests = @celldb.write("Hello World!")
    # puts "#{size}: "
    # puts digests.map { |digest| CellDB::Digest.digest_to_hex(digest) }.join("\n")
  end
  
  def test_keyize
    key = CellDB.keyize("Hello World!")
    puts CellDB.key_to_human_readable(*key)
    # puts [key.first, key.last.map { |digest| CellDB::Digest.digest_to_hex(digest) } ]
  end

end
