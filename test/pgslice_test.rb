require_relative "test_helper"

class PgSliceTest < Minitest::Test
  def setup
    $conn.exec File.read("test/support/schema.sql")
  end

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
    assert_period "year", column: "createdOn"
  end

  def test_timestamptz
    assert_period "year", column: "createdAtTz"
  end

  def test_no_partition
    run_command "prep Posts --no-partition"
    assert table_exists?("Posts_intermediate")
    assert_equal 0, count("Posts_intermediate")

    run_command "fill Posts"
    assert_equal 10000, count("Posts_intermediate")

    run_command "swap Posts"
    assert !table_exists?("Posts_intermediate")
    assert table_exists?("Posts_retired")

    run_command "unswap Posts"
    assert table_exists?("Posts_intermediate")
    assert !table_exists?("Posts_retired")

    run_command "unprep Posts"
    assert !table_exists?("Posts_intermediate")
  end

  def test_trigger_based
    assert_period "month", trigger_based: true
  end

  def test_trigger_based_timestamptz
    assert_period "month", trigger_based: true, column: "createdAtTz"
  end

  def test_v2
    assert_period "month", version: 2
  end

  def test_tablespace
    assert_period "day", tablespace: true
  end

  def test_tablespace_trigger_based
    assert_period "month", trigger_based: true, tablespace: true
  end

  private

  def assert_period(period, column: "createdAt", trigger_based: false, tablespace: false, version: nil)
    $conn.exec('CREATE STATISTICS my_stats ON "Id", "UserId" FROM "Posts"')

    if server_version_num >= 120000 && !trigger_based
      $conn.exec('ALTER TABLE "Posts" ADD COLUMN "Gen" INTEGER GENERATED ALWAYS AS ("Id" * 10) STORED')
    end

    run_command "prep Posts #{column} #{period} #{"--trigger-based" if trigger_based} #{"--test-version #{version}" if version}"
    assert table_exists?("Posts_intermediate")

    run_command "add_partitions Posts --intermediate --past 1 --future 1 #{"--tablespace pg_default" if tablespace}"
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

    declarative = !trigger_based

    if declarative
      refute_primary_key "Posts_intermediate"
    else
      assert_primary_key "Posts_intermediate"
    end

    if declarative && version == 2
      refute_index "Posts_intermediate"
    else
      assert_index "Posts_intermediate"
    end

    assert_equal 0, count("Posts_intermediate")
    run_command "fill Posts"
    assert_equal 10000, count("Posts_intermediate")

    # insert into old table
    $conn.exec('INSERT INTO "Posts" ("' + column + '") VALUES (\'' + now.iso8601 + '\') RETURNING "Id"').first

    run_command "analyze Posts"

    # TODO check sequence ownership
    output = run_command "swap Posts"
    assert_match "SET LOCAL lock_timeout = '5s';", output
    assert table_exists?("Posts")
    assert table_exists?("Posts_retired")
    refute table_exists?("Posts_intermediate")

    assert_equal 10000, count("Posts")
    run_command "fill Posts --swapped"
    assert_equal 10001, count("Posts")

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
    assert_equal 10002, count("Posts")
    if declarative
      assert insert_result["Id"]
    else
      assert_nil insert_result
      assert_equal 0, count("Posts", only: true)
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
    assert_column new_partition_name, "updatedAt"

    run_command "analyze Posts --swapped"

    # pg_stats_ext view available with Postgres 12+
    assert_statistics "Posts" if server_version_num >= 120000 && !trigger_based

    # TODO check sequence ownership
    run_command "unswap Posts"
    assert table_exists?("Posts")
    assert table_exists?("Posts_intermediate")
    refute table_exists?("Posts_retired")
    assert table_exists?(partition_name)
    assert table_exists?(new_partition_name)

    run_command "unprep Posts"
    assert table_exists?("Posts")
    refute table_exists?("Posts_intermediate")
    refute table_exists?(partition_name)
    refute table_exists?(new_partition_name)
  end

  def run_command(command)
    if verbose?
      puts "$ pgslice #{command}"
      puts
    end
    stdout, stderr = capture_io do
      PgSlice::CLI.start("#{command} --url #{$url}".split(" "))
    end
    if verbose?
      puts stdout
      puts
    end
    assert_equal "", stderr
    stdout
  end

  def add_column(table, column)
    $conn.exec("ALTER TABLE \"#{table}\" ADD COLUMN \"#{column}\" timestamp")
  end

  def assert_column(table, column)
    assert_includes $conn.exec("SELECT * FROM \"#{table}\" LIMIT 0").fields, column
  end

  def table_exists?(table_name)
    result = $conn.exec <<~SQL
      SELECT * FROM information_schema.tables
      WHERE table_schema = 'public' AND table_name = '#{table_name}'
    SQL
    result.any?
  end

  def count(table_name, only: false)
    result = $conn.exec <<~SQL
      SELECT COUNT(*) FROM #{only ? "ONLY " : ""}"#{table_name}"
    SQL
    result.first["count"].to_i
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

  # extended statistics are built on partitioned tables
  # https://github.com/postgres/postgres/commit/20b9fa308ebf7d4a26ac53804fce1c30f781d60c
  # (backported to Postgres 10)
  def assert_statistics(table_name)
    result = $conn.exec <<~SQL
      SELECT n_distinct
      FROM pg_stats_ext
      WHERE tablename = '#{table_name}'
    SQL
    assert result.any?, "Missing extended statistics on #{table_name}"
    assert_equal '{"1, 2": 10002}', result.first["n_distinct"]
  end

  def server_version_num
    $conn.exec("SHOW server_version_num").first["server_version_num"].to_i
  end

  def verbose?
    ENV["VERBOSE"]
  end
end
