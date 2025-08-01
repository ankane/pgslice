require_relative "test_helper"

class PgSliceTest < Minitest::Test
  def setup
    execute File.read("test/support/schema.sql")
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

    assert_analyzed "Posts_intermediate" do
      run_command "analyze Posts"
    end

    run_command "swap Posts"
    assert !table_exists?("Posts_intermediate")
    assert table_exists?("Posts_retired")

    assert_analyzed "Posts" do
      run_command "analyze Posts --swapped"
    end

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

  def test_prep_missing_table
    assert_error "Table not found", "prep Items"
  end

  def test_add_partitions_missing_table
    assert_error "Table not found", "add_partitions Items"
  end

  def test_add_partitions_non_partitioned_table
    assert_error "No settings found", "add_partitions Posts"
  end

  def test_fill_missing_table
    assert_error "Table not found", "fill Items"
  end

  def test_analyze_missing_table
    assert_error "Table not found", "analyze Items"
  end

  def test_swap_missing_table
    assert_error "Table not found", "swap Items"
  end

  def test_unswap_missing_table
    assert_error "Table not found", "unswap Items"
  end

  def test_unprep_missing_table
    assert_error "Table not found", "unprep Items"
  end

  private

  def assert_period(period, column: "createdAt", trigger_based: false, tablespace: false, version: nil)
    execute %!CREATE STATISTICS my_stats ON "Id", "UserId" FROM "Posts"!

    if !trigger_based
      execute %!ALTER TABLE "Posts" ADD COLUMN "Gen" INTEGER GENERATED ALWAYS AS ("Id" * 10) STORED!
    end

    run_command "prep Posts #{column} #{period} #{"--trigger-based" if trigger_based} #{"--test-version #{version}" if version}"
    assert table_exists?("Posts_intermediate")

    run_command "add_partitions Posts --intermediate --past 1 --future 1 #{"--tablespace pg_default" if tablespace}"
    now = Time.now.utc
    time_format =
      case period
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
    execute %!INSERT INTO "Posts" (#{quote_ident(column)}) VALUES ($1) RETURNING "Id"!, [now.iso8601]

    assert_analyzed "Posts%", 4 do
      run_command "analyze Posts"
    end

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
    days =
      case period
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
    insert_result = execute(%!INSERT INTO "Posts" (#{quote_ident(column)}) VALUES ($1) RETURNING "Id"!, [now.iso8601]).first
    assert_equal 10002, count("Posts")
    if declarative
      assert insert_result["Id"]
    else
      assert_nil insert_result
      assert_equal 0, count("Posts", only: true)
    end

    # test insert with null field
    error = assert_raises(PG::ServerError) do
      execute %!INSERT INTO "Posts" ("UserId") VALUES (1)!
    end
    assert_includes error.message, "partition"

    # test foreign key
    error = assert_raises(PG::ServerError) do
      execute %!INSERT INTO "Posts" (#{quote_ident(column)}, "UserId") VALUES (NOW(), 1)!
    end
    assert_includes error.message, "violates foreign key constraint"

    # test adding column
    add_column "Posts", "updatedAt"
    assert_column "Posts", "updatedAt"
    assert_column partition_name, "updatedAt"
    assert_column new_partition_name, "updatedAt"

    assert_analyzed "Posts%", 6 do
      run_command "analyze Posts --swapped"
    end

    # pg_stats_ext view available with Postgres 12+
    assert_statistics "Posts" if !trigger_based

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

  def assert_error(message, command)
    run_command command, error: message
  end

  def run_command(command, error: nil)
    if verbose?
      puts "$ pgslice #{command}"
      puts
    end
    stdout, stderr = capture_io do
      PgSlice::CLI.start("#{command} --url #{url}".split(" "))
    end
    if verbose?
      puts stdout
      puts
    end
    if error
      assert_match error, stderr
    else
      assert_equal "", stderr
    end
    stdout
  end

  def add_column(table, column)
    execute "ALTER TABLE #{quote_ident(table)} ADD COLUMN #{quote_ident(column)} timestamp"
  end

  def assert_column(table, column)
    assert_includes execute("SELECT * FROM #{quote_ident(table)} LIMIT 0").fields, column
  end

  def table_exists?(table_name)
    query = <<~SQL
      SELECT * FROM information_schema.tables
      WHERE table_schema = 'public' AND table_name = $1
    SQL
    result = execute(query, [table_name])
    result.any?
  end

  def count(table_name, only: false)
    result = execute <<~SQL
      SELECT COUNT(*) FROM #{only ? "ONLY " : ""}#{quote_ident(table_name)}
    SQL
    result.first["count"].to_i
  end

  def primary_key(table_name)
    query = <<~SQL
      SELECT pg_get_constraintdef(oid) AS def
      FROM pg_constraint
      WHERE contype = 'p' AND conrelid = $1::regclass
    SQL
    result = execute(query, [quote_ident(table_name)])
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
    query = <<~SQL
      SELECT pg_get_indexdef(indexrelid)
      FROM pg_index
      WHERE indrelid = $1::regclass AND indisprimary = 'f'
    SQL
    result = execute(query, [quote_ident(table_name)])
    result.first
  end

  def assert_index(table_name)
    assert index(table_name), "Missing index on #{table_name}"
  end

  def refute_index(table_name)
    refute index(table_name), "Unexpected index on #{table_name}"
  end

  def assert_foreign_key(table_name)
    query = <<~SQL
      SELECT pg_get_constraintdef(oid) AS def
      FROM pg_constraint
      WHERE contype = 'f' AND conrelid = $1::regclass
    SQL
    result = execute(query, [quote_ident(table_name)])
    assert !result.detect { |row| row["def"] =~ /\AFOREIGN KEY \(.*\) REFERENCES "Users"\("Id"\)\z/ }.nil?, "Missing foreign key on #{table_name}"
  end

  def assert_analyzed(table_pattern, expected = 1)
    execute("SELECT pg_stat_reset()")
    yield
    last_analyzed = execute("SELECT relname, last_analyze FROM pg_stat_user_tables WHERE relname LIKE $1", [table_pattern])
    # https://github.com/postgres/postgres/commit/375aed36ad83f0e021e9bdd3a0034c0c992c66dc
    if server_version_num >= 150000
      assert_equal expected, last_analyzed.count { |v| v["last_analyze"] }
    end
  end

  # extended statistics are built on partitioned tables
  # https://github.com/postgres/postgres/commit/20b9fa308ebf7d4a26ac53804fce1c30f781d60c
  # (backported to Postgres 10)
  def assert_statistics(table_name)
    query = <<~SQL
      SELECT n_distinct
      FROM pg_stats_ext
      WHERE tablename = $1
    SQL
    result = execute(query, [table_name])
    assert result.any?, "Missing extended statistics on #{table_name}"
    assert_equal %!{"1, 2": 10002}!, result.first["n_distinct"]
  end

  def server_version_num
    execute("SHOW server_version_num").first["server_version_num"].to_i
  end

  def url
    @url ||= ENV["PGSLICE_URL"] || "postgres:///pgslice_test"
  end

  def connection
    @connection ||= PG::Connection.new(url)
  end

  def execute(query, params = [])
    if params.any?
      connection.exec_params(query, params)
    else
      connection.exec(query)
    end
  end

  def quote_ident(value)
    PG::Connection.quote_ident(value)
  end

  def verbose?
    ENV["VERBOSE"]
  end
end
