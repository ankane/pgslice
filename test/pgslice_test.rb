require_relative "test_helper"

class PgSliceTest < Minitest::Test
  def test_day
    assert_period("day")
  end

  def test_month
    assert_period("month")
  end

  def test_no_partition
    run_command "prep Posts --no-partition"
    run_command "fill Posts"
    run_command "swap Posts"
    run_command "unswap Posts"
    run_command "unprep Posts"
    assert true
  end

  private

  def assert_period(period)
    run_command "prep Posts createdAt #{period}"
    run_command "add_partitions Posts --intermediate --past 1 --future 1"
    now = Time.now
    time_format = period == "month" ? "%Y%m" : "%Y%m%d"
    assert_foreign_key "Posts_#{now.strftime(time_format)}"
    run_command "fill Posts"
    run_command "analyze Posts"
    run_command "swap Posts"
    run_command "fill Posts --swapped"
    run_command "add_partitions Posts --future 3"
    days = period == "month" ? 90 : 3
    assert_foreign_key "Posts_#{(now + days * 86400).strftime(time_format)}"
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

  def assert_foreign_key(table_name)
    result = $conn.exec <<-SQL
      SELECT pg_get_constraintdef(oid) AS def
      FROM pg_constraint
      WHERE contype = 'f' AND conrelid = '"#{table_name}"'::regclass
    SQL
    assert !result.detect { |row| row["def"] =~ /\AFOREIGN KEY \(.*\) REFERENCES "Users"\("Id"\)\z/ }.nil?
  end
end
