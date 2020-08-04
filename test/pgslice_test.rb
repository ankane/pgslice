require_relative "test_helper"

class PgSliceTest < Minitest::Test
  def test_day
    assert_period "day"
  end

  def test_month
    assert_period "month"
  end

  def test_year
    assert_period "year"
  end

  def test_date
    assert_period "year", column: "createdAt"
  end

  def test_timestamptz
    assert_period "year", column: "createdAtTz"
  end

  def test_no_partition
    run_command "prep Posts --no-partition"
    run_command "fill Posts"
    run_command "swap Posts"
    run_command "unswap Posts"
    run_command "unprep Posts"
    assert true
  end

  def test_trigger_based
    assert_period "month", trigger_based: true
  end

  def test_trigger_based_timestamptz
    assert_period "month", trigger_based: true, column: "createdAtTz"
  end

  private

  def assert_period(period, column: "createdAt", trigger_based: false)
    run_command "prep Posts #{column} #{period} #{"--trigger-based" if trigger_based}"
    run_command "add_partitions Posts --intermediate --past 1 --future 1"

    now = Time.now.utc
    time_format = case period
      when "day"
        "%Y%m%d"
      when "month"
        "%Y%m"
      else
        "%Y"
      end
    partition_name = "Posts_#{now.strftime(time_format)}"
    assert_primary_key partition_name
    assert_index partition_name
    assert_foreign_key partition_name

    declarative = server_version_num >= 100000 && !trigger_based

    if declarative
      refute_primary_key "Posts_intermediate"
    else
      assert_primary_key "Posts_intermediate"
    end

    if declarative && server_version_num < 110000
      refute_index "Posts_intermediate"
    else
      assert_index "Posts_intermediate"
    end

    run_command "fill Posts"
    run_command "analyze Posts"

    run_command "swap Posts"
    assert table_exists?("Posts")
    assert table_exists?("Posts_retired")
    refute table_exists?("Posts_intermediate")
    # TODO check sequence ownership

    run_command "fill Posts --swapped"
    run_command "add_partitions Posts --future 3"
    days = case period
      when "day"
        3
      when "month"
        90
      else
        365 * 3
      end
    new_partition_name = "Posts_#{(now + days * 86400).strftime(time_format)}"
    assert_primary_key new_partition_name
    assert_index new_partition_name
    assert_foreign_key new_partition_name

    # test insert works
    insert_result = $conn.exec('INSERT INTO "Posts" ("' + column + '") VALUES (\'' + now.iso8601 + '\') RETURNING "Id"').first
    if declarative
      assert insert_result["Id"]
    else
      assert_equal 10001, $conn.exec('SELECT COUNT(*) FROM "Posts"').first["count"].to_i
      assert_equal 0, $conn.exec('SELECT COUNT(*) FROM ONLY "Posts"').first["count"].to_i
      assert_nil insert_result
    end

    # test insert with null field
    error = assert_raises(PG::ServerError) do
      $conn.exec('INSERT INTO "Posts" ("UserId") VALUES (1)')
    end
    assert_includes error.message, "partition"

    # test foreign key
    error = assert_raises(PG::ServerError) do
      $conn.exec('INSERT INTO "Posts" ("' + column + '", "UserId") VALUES (NOW(), 1)')
    end
    assert_includes error.message, "violates foreign key constraint"

    # test adding column
    add_column "Posts", "updatedAt"
    assert_column "Posts", "updatedAt"
    assert_column partition_name, "updatedAt"

    run_command "analyze Posts --swapped"

    run_command "unswap Posts"
    assert table_exists?("Posts")
    assert table_exists?("Posts_intermediate")
    refute table_exists?("Posts_retired")
    # TODO check sequence ownership

    run_command "unprep Posts"
    assert table_exists?("Posts")
    refute table_exists?("Posts_intermediate")

    assert true
  end

  def run_command(command)
    if verbose?
      puts "$ pgslice #{command}"
      puts
    end
    stdout, stderr = capture_io do
      PgSlice::CLI.start("#{command} --url #{$url}".split(" "))
    end
    assert_equal "", stderr
    if verbose?
      puts stdout
      puts
    end
  end

  def add_column(table, column)
    $conn.exec("ALTER TABLE \"#{table}\" ADD COLUMN \"#{column}\" timestamp")
  end

  def assert_column(table, column)
    assert ($conn.exec("SELECT * FROM \"#{table}\" LIMIT 1").first || {}).key?(column), "Missing column #{column} on #{table}"
  end

  def table_exists?(table_name)
    result = $conn.exec <<~SQL
      SELECT * FROM information_schema.tables
      WHERE table_schema = 'public' AND table_name = '#{table_name}'
    SQL
    result.any?
  end

  def primary_key(table_name)
    result = $conn.exec <<~SQL
      SELECT pg_get_constraintdef(oid) AS def
      FROM pg_constraint
      WHERE contype = 'p' AND conrelid = '"#{table_name}"'::regclass
    SQL
    result.first
  end

  def assert_primary_key(table_name)
    result = primary_key(table_name)
    assert_match "PRIMARY KEY (\"Id\")", result["def"]
  end

  def refute_primary_key(table_name)
    assert_nil primary_key(table_name), "Unexpected primary key on #{table_name}"
  end

  def index(table_name)
    result = $conn.exec <<~SQL
      SELECT pg_get_indexdef(indexrelid)
      FROM pg_index
      WHERE indrelid = '"#{table_name}"'::regclass AND indisprimary = 'f'
    SQL
    result.first
  end

  def assert_index(table_name)
    assert index(table_name), "Missing index on #{table_name}"
  end

  def refute_index(table_name)
    refute index(table_name), "Unexpected index on #{table_name}"
  end

  def assert_foreign_key(table_name)
    result = $conn.exec <<~SQL
      SELECT pg_get_constraintdef(oid) AS def
      FROM pg_constraint
      WHERE contype = 'f' AND conrelid = '"#{table_name}"'::regclass
    SQL
    assert !result.detect { |row| row["def"] =~ /\AFOREIGN KEY \(.*\) REFERENCES "Users"\("Id"\)\z/ }.nil?, "Missing foreign key on #{table_name}"
  end

  def server_version_num
    $conn.exec("SHOW server_version_num").first["server_version_num"].to_i
  end

  def verbose?
    ENV["VERBOSE"]
  end
end
