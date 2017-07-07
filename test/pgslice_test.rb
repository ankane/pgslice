require_relative "test_helper"

class PgSliceTest < Minitest::Test
  def test_day
    assert_period("day")
  end

  def test_month
    assert_period("month")
  end

  def assert_period(period)
    [
      "prep Posts createdAt #{period}",
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
