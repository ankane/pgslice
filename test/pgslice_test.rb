require_relative "test_helper"

class PgSliceTest < Minitest::Test
  def test_day
    assert_period("day")
  end

  def test_month
    assert_period("month")
  end

  private

  def assert_period(period)
    run_command "prep Posts createdAt #{period}"
    run_command "add_partitions Posts --intermediate --past 1 --future 1"
    run_command "fill Posts"
    run_command "analyze Posts"
    run_command "swap Posts"
    run_command "fill Posts --swapped"
    run_command "add_partitions Posts --future 3"
    run_command "unswap Posts"
    run_command "unprep Posts"
    assert true
  end

  def run_command(command)
    puts "pgslice #{command}"
    puts
    PgSlice::Client.new("#{command} --url pgslice_test".split(" ")).perform
    puts
  end
end
