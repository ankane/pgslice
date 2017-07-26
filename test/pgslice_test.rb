require_relative "test_helper"

class PgSliceTest < Minitest::Test
  def test_day
    assert_period("day")
  end

  def test_month
    assert_period("month")
  end

  def test_foreign_keys
    assert has_foreign_key?("Posts"), "Original table is missing foreign key"
    run_command "prep Posts createdAt month"
    assert has_foreign_key?("Posts_intermediate"), "Intermediate table is missing foreign key"
    run_command "add_partitions Posts --intermediate --past 1 --future 1"
    assert has_foreign_key?("Posts_#{Time.now.strftime("%Y%m")}"), "Partition is missing foreign key"
    run_command "unprep Posts"
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

  def has_foreign_key?(table_name)
    @conn ||= PG::Connection.open(dbname: "pgslice_test")
    result = @conn.exec <<-SQL
      SELECT pg_get_constraintdef(oid) AS def
      FROM pg_constraint
      WHERE contype = 'f' AND conrelid = '"#{table_name}"'::regclass
    SQL
    !!result.detect { |row| row['def'] =~ /\AFOREIGN KEY \(.*\) REFERENCES "Users"\("Id"\)\z/ }
  end
end
