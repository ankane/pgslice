require_relative "test_helper"

class ForeignKeyTest < Minitest::Test
  def setup
    create_tables
  end

  def teardown
    drop_tables
  end

  def test_foreign_keys
    assert has_foreign_key?("Posts"), "Original table is missing foreign key"
    pgslice "prep Posts createdAt month"
    pgslice "add_partitions Posts --intermediate --past 1 --future 1"
    assert has_foreign_key?("Posts_intermediate"), "Intermediate table is missing foreign key"
    assert has_foreign_key?("Posts_#{Time.now.strftime("%Y%m")}"), "Partition is missing foreign key"
  end

  def pgslice(command)
    PgSlice::Client.new("#{command} --url pgslice_test".split(" ")).perform
  end

  def has_foreign_key?(table_name)
    result = conn.exec <<-SQL
      SELECT pg_get_constraintdef(oid) AS def
      FROM pg_constraint
      WHERE contype = 'f' AND conrelid = '"#{table_name}"'::regclass
    SQL
    !!result.detect { |row| row['def'] =~ /\AFOREIGN KEY \(.*\) REFERENCES "Users"\("Id"\)\z/ }
  end
end
