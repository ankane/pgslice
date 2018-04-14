require_relative "test_helper"

class PgSliceTest < Minitest::Test
  def test_day
    assert_period "day"
  end

  def test_month
    assert_period "month"
  end

  def test_no_partition
    run_command "prep Foo.Posts --no-partition"
    run_command "fill Foo.Posts"
    run_command "swap Foo.Posts"
    run_command "unswap Foo.Posts"
    run_command "unprep Foo.Posts"
    assert true
  end

  def test_trigger_based
    assert_period "month", trigger_based: true
  end

  private

  def assert_period(period, trigger_based: false)
    run_command "prep Foo.Posts createdAt #{period} #{"--trigger-based" if trigger_based}"
    run_command "add_partitions Foo.Posts --intermediate --past 1 --future 1"
    now = Time.now
    time_format = period == "month" ? "%Y%m" : "%Y%m%d"
    partition_name = "Foo.Posts_#{now.strftime(time_format)}"
    assert_foreign_key partition_name
    run_command "fill Foo.Posts"
    run_command "analyze Foo.Posts"
    run_command "swap Foo.Posts"
    run_command "fill Foo.Posts --swapped"
    run_command "add_partitions Foo.Posts --future 3"
    days = period == "month" ? 90 : 3
    assert_foreign_key "Foo.Posts_#{(now + days * 86400).strftime(time_format)}"

    # test insert works
    insert_result = $conn.exec('INSERT INTO "Foo"."Posts" ("createdAt") VALUES (NOW()) RETURNING "Id"').first
    if server_version_num >= 100000 && !trigger_based
      assert insert_result["Id"]
    else
      assert_nil insert_result
      assert 10001, $conn.exec('SELECT COUNT(*) FROM "Foo"."Posts"').first["count"].to_i
    end

    # test insert with null field
    error = assert_raises(PG::ServerError) do
      $conn.exec('INSERT INTO "Foo"."Posts" ("UserId") VALUES (1)')
    end
    assert_includes error.message, "partition"

    # test adding column
    add_column "Foo.Posts", "updatedAt"
    assert_column "Foo.Posts", "updatedAt"
    assert_column partition_name, "updatedAt"

    run_command "unswap Foo.Posts"
    run_command "unprep Foo.Posts"
    assert true
  end

  def run_command(command)
    puts "pgslice #{command}"
    puts
    PgSlice::Client.new("#{command} --url pgslice_test".split(" ")).perform
    puts
  end

  def add_column(table, column)
    $conn.exec("ALTER TABLE #{quote_table(table)} ADD COLUMN \"#{column}\" timestamp")
  end

  def assert_column(table, column)
    assert $conn.exec("SELECT * FROM #{quote_table(table)} LIMIT 1").first.key?(column), "Missing column #{column} on #{table}"
  end

  def assert_foreign_key(table_name)
    result = $conn.exec <<-SQL
      SELECT pg_get_constraintdef(oid) AS def
      FROM pg_constraint
      WHERE contype = 'f' AND conrelid = '#{quote_table(table_name)}'::regclass
    SQL
    assert !result.detect { |row| row["def"] =~ /\AFOREIGN KEY \(.*\) REFERENCES "Foo"."Users"\("Id"\)\z/ }.nil?, "Missing foreign key on #{table_name}"
  end

  def quote_table(table)
    table.split(".", 2).map { |v| PG::Connection.quote_ident(v) }.join(".")
  end

  def server_version_num
    $conn.exec("SHOW server_version_num").first["server_version_num"].to_i
  end
end
