require "pgslice/version"
require "slop"
require "pg"
require "active_support/all"

module PgSlice
  class Error < StandardError; end

  class Client
    attr_reader :arguments, :options

    SQL_FORMAT = {
      day: "YYYYMMDD",
      month: "YYYYMM"
    }

    def initialize(args)
      parse_args(args)
      @command = @arguments.shift
    end

    def perform
      return if @exit

      case @command
      when "prep"
        prep
      when "add_partitions"
        add_partitions
      when "fill"
        fill
      when "swap"
        swap
      when "unswap"
        unswap
      when "unprep"
        unprep
      when nil
        log "Commands: add_partitions, fill, prep, swap, unprep, unswap"
      else
        abort "Unknown command: #{@command}"
      end
    ensure
      @connection.close if @connection
    end

    protected

    # commands

    def prep
      table, column, period = arguments
      intermediate_table = "#{table}_intermediate"
      trigger_name = self.trigger_name(table)

      abort "Usage: pgslice prep <table> <column> <period>" if arguments.length != 3
      abort "Table not found: #{table}" unless table_exists?(table)
      abort "Table already exists: #{intermediate_table}" if table_exists?(intermediate_table)
      abort "Column not found: #{column}" unless columns(table).include?(column)
      abort "Invalid period: #{period}" unless SQL_FORMAT[period.to_sym]

      queries = []

      queries << <<-SQL
CREATE TABLE #{intermediate_table} (
  LIKE #{table} INCLUDING INDEXES INCLUDING DEFAULTS
);
      SQL

      sql_format = SQL_FORMAT[period.to_sym]
      queries << <<-SQL
CREATE FUNCTION #{trigger_name}()
RETURNS trigger AS $$
BEGIN
  EXECUTE 'INSERT INTO public.#{table}_' || to_char(NEW.#{column}, '#{sql_format}') || ' VALUES ($1.*)' USING NEW;
  RETURN NULL;
END;
$$ LANGUAGE plpgsql;
      SQL

      queries << <<-SQL
CREATE TRIGGER #{trigger_name}
BEFORE INSERT ON #{intermediate_table}
FOR EACH ROW EXECUTE PROCEDURE #{trigger_name}();
      SQL

      run_queries(queries)
    end

    def unprep
      table = arguments.first
      intermediate_table = "#{table}_intermediate"
      trigger_name = self.trigger_name(table)

      abort "Usage: pgslice unprep <table>" if arguments.length != 1
      abort "Table not found: #{intermediate_table}" unless table_exists?(intermediate_table)

      queries = [
        "DROP TABLE #{intermediate_table} CASCADE;",
        "DROP FUNCTION #{trigger_name}();"
      ]
      run_queries(queries)
    end

    def add_partitions
      original_table = arguments.first
      table = options[:intermediate] ? "#{original_table}_intermediate" : original_table
      trigger_name = self.trigger_name(original_table)

      abort "Usage: pgslice add_partitions <table>" if arguments.length != 1
      abort "Table not found: #{table}" unless table_exists?(table)

      future = options[:future]
      past = options[:past]
      range = (-1 * past)..future

      # ensure table has trigger
      abort "No trigger on table: #{table}\nDid you mean to use --intermediate?" unless has_trigger?(trigger_name, table)

      period, field, name_format, inc, today = settings_from_table(original_table, table)

      days = range.map { |n| today + (n * inc) }
      queries = []

      days.each do |day|
        partition_name = "#{original_table}_#{day.strftime(name_format)}"
        next if table_exists?(partition_name)

        date_format = "%Y-%m-%d"

        queries << <<-SQL
CREATE TABLE #{partition_name} (
  LIKE #{table} INCLUDING INDEXES INCLUDING DEFAULTS,
  CHECK (#{field} >= '#{day.strftime(date_format)}'::date AND #{field} < '#{(day + inc).strftime(date_format)}'::date)
) INHERITS (#{table});
        SQL
      end

      run_queries(queries) if queries.any?
    end

    def fill
      table = arguments.first

      abort "Usage: pgslice fill <table>" if arguments.length != 1

      if options[:swapped]
        source_table = retired_name(table)
        dest_table = table
      else
        source_table = table
        dest_table = intermediate_name(table)
      end

      abort "Table not found: #{source_table}" unless table_exists?(source_table)
      abort "Table not found: #{dest_table}" unless table_exists?(dest_table)

      period, field, name_format, inc, today = settings_from_table(table, dest_table)

      date_format = "%Y-%m-%d"
      existing_tables = self.existing_tables(like: "#{table}_%").select { |t| /#{Regexp.escape("#{table}_")}(\d{4,6})/.match(t) }
      starting_time = DateTime.strptime(existing_tables.first.last(8), name_format)
      ending_time = DateTime.strptime(existing_tables.last.last(8), name_format) + inc

      primary_key = self.primary_key(table)
      max_source_id = max_id(source_table, primary_key)
      max_dest_id = max_id(dest_table, primary_key)

      starting_id = max_dest_id + 1
      fields = columns(source_table).join(", ")
      batch_size = options[:batch_size]

      log "Overview"
      log "#{source_table} max #{primary_key}: #{max_source_id}"
      log "#{dest_table} max #{primary_key}: #{max_dest_id}"
      log "time period: #{starting_time.to_date} -> #{ending_time.to_date}"
      log

      log "Batches"
      while starting_id <= max_source_id
        log "#{starting_id}..#{[starting_id + batch_size - 1, max_source_id].min}"

        query = "INSERT INTO #{dest_table} (#{fields}) SELECT #{fields} FROM #{source_table} WHERE #{primary_key} >= #{starting_id} AND #{primary_key} < #{starting_id + batch_size} AND #{field} >= '#{starting_time.strftime(date_format)}'::date AND #{field} < '#{ending_time.strftime(date_format)}'::date"
        log query if options[:debug]
        execute(query)

        starting_id += batch_size

        if options[:sleep] && starting_id <= max_source_id
          sleep(options[:sleep])
        end
      end
    end

    def swap
      table = arguments.first
      intermediate_table = intermediate_name(table)
      retired_table = retired_name(table)

      abort "Usage: pgslice swap <table>" if arguments.length != 1
      abort "Table not found: #{table}" unless table_exists?(table)
      abort "Table not found: #{intermediate_table}" unless table_exists?(intermediate_table)
      abort "Table already exists: #{retired_table}" if table_exists?(retired_table)

      queries = [
        "ALTER TABLE #{table} RENAME TO #{retired_table};",
        "ALTER TABLE #{intermediate_table} RENAME TO #{table};"
      ]
      run_queries(queries)
    end

    def unswap
      table = arguments.first
      intermediate_table = intermediate_name(table)
      retired_table = retired_name(table)

      abort "Usage: pgslice unswap <table>" if arguments.length != 1
      abort "Table not found: #{table}" unless table_exists?(table)
      abort "Table not found: #{retired_table}" unless table_exists?(retired_table)
      abort "Table already exists: #{intermediate_table}" if table_exists?(intermediate_table)

      queries = [
        "ALTER TABLE #{table} RENAME TO #{intermediate_table};",
        "ALTER TABLE #{retired_table} RENAME TO #{table};"
      ]
      run_queries(queries)
    end

    # arguments

    def parse_args(args)
      opts = Slop.parse(args) do |o|
        o.boolean "--intermediate"
        o.boolean "--swapped"
        o.boolean "--debug"
        o.float "--sleep"
        o.integer "--future", default: 3
        o.integer "--past", default: 3
        o.integer "--batch-size", default: 10000
        o.boolean "--dry-run", default: false
        o.on "-v", "--version", "print the version" do
          log PgSlice::VERSION
          @exit = true
        end
      end
      @arguments = opts.arguments
      @options = opts.to_hash
    rescue Slop::Error => e
      abort e.message
    end

    # output

    def log(message = nil)
      $stderr.puts message
    end

    def log_sql(message = nil)
      $stdout.puts message
    end

    def abort(message)
      raise PgSlice::Error, message
    end

    # database connection

    def connection
      @connection ||= begin
        abort "Set PGSLICE_URL" unless ENV["PGSLICE_URL"]
        uri = URI.parse(ENV["PGSLICE_URL"])
        uri_parser = URI::Parser.new
        config = {
          host: uri.host,
          port: uri.port,
          dbname: uri.path.sub(/\A\//, ""),
          user: uri.user,
          password: uri.password,
          connect_timeout: 1
        }.reject { |_, value| value.to_s.empty? }
        config.map { |key, value| config[key] = uri_parser.unescape(value) if value.is_a?(String) }
        PG::Connection.new(config)
      end
    end

    def execute(query, params = [])
      connection.exec_params(query, params).to_a
    end

    def run_queries(queries)
      connection.transaction do
        execute("SET client_min_messages TO warning")
        log_sql "BEGIN;"
        log_sql
        queries.each do |query|
          log_sql query
          log_sql
          execute(query) unless options[:dry_run]
        end
        log_sql "COMMIT;"
        log_sql
      end
    end

    def existing_tables(like:)
      query = "SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = $1 AND tablename LIKE $2"
      execute(query, ["public", like]).map { |r| r["tablename"] }.sort
    end

    def table_exists?(table)
      existing_tables(like: table).any?
    end

    def columns(table)
      execute("SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = $1", [table]).map{ |r| r["column_name"] }
    end

    # http://stackoverflow.com/a/20537829
    def primary_key(table)
      query = <<-SQL
        SELECT
          pg_attribute.attname,
          format_type(pg_attribute.atttypid, pg_attribute.atttypmod)
        FROM
          pg_index, pg_class, pg_attribute, pg_namespace
        WHERE
          pg_class.oid = $2::regclass AND
          indrelid = pg_class.oid AND
          nspname = $1 AND
          pg_class.relnamespace = pg_namespace.oid AND
          pg_attribute.attrelid = pg_class.oid AND
          pg_attribute.attnum = any(pg_index.indkey) AND
          indisprimary
      SQL
      row = execute(query, ["public", table])[0]
      row && row["attname"]
    end

    def max_id(table, primary_key)
      execute("SELECT MAX(#{primary_key}) FROM #{table}")[0]["max"].to_i
    end

    def has_trigger?(trigger_name, table)
      execute("SELECT 1 FROM pg_trigger WHERE tgname = $1 AND tgrelid = $2::regclass", [trigger_name, "public.#{table}"]).any?
    end

    # helpers

    def trigger_name(table)
      "#{table}_insert_trigger"
    end

    def intermediate_name(table)
      "#{table}_intermediate"
    end

    def retired_name(table)
      "#{table}_retired"
    end

    def settings_from_table(original_table, table)
      trigger_name = self.trigger_name(original_table)
      function_def = execute("select pg_get_functiondef(oid) from pg_proc where proname = $1", [trigger_name])[0]["pg_get_functiondef"]
      sql_format = SQL_FORMAT.find { |_, f| function_def.include?("'#{f}'") }
      abort "Could not read settings" unless sql_format
      period = sql_format[0]
      field = /to_char\(NEW\.(\w+),/.match(function_def)[1]

      today = Time.now
      case period
      when :day
        name_format = "%Y%m%d"
        inc = 1.day
        today = today.beginning_of_day
      else
        name_format = "%Y%m"
        inc = 1.month
        today = today.beginning_of_month
      end

      [period, field, name_format, inc, today]
    end
  end
end
