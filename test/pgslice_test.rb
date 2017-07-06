require_relative "test_helper"

class PgSliceTest < Minitest::Test
  def test_basic
    [
      "prep posts created_at day",
      "add_partitions posts --intermediate --past 1 --future 1",
      "fill posts",
      "analyze posts",
      "swap posts",
      "fill posts --swapped",
      "add_partitions posts --future 3",
      "unswap posts",
      "unprep posts"
    ].each do |command|
      puts "pgslice #{command}"
      puts
      PgSlice::Client.new("#{command} --url pgslice_test".split(" ")).perform
      puts
    end

    assert true
  end
end
