require_relative "test_helper"

class PgSliceTest < Minitest::Test
  def test_basic
    [
      "prep Posts createdAt day",
      "add_partitions Posts --intermediate --past 1 --future 1",
      "fill Posts",
      "analyze Posts",
      "swap Posts",
      "fill Posts --swapped",
      "add_partitions Posts --future 3",
      "unswap Posts",
      "unprep Posts"
    ].each do |command|
      puts "pgslice #{command}"
      puts
      PgSlice::Client.new("#{command} --url pgslice_test".split(" ")).perform
      puts
    end

    assert true
  end
end
