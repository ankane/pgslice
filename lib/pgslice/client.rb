module PgSlice
  class Client < Thor
    check_unknown_options!

    class_option :url, desc: "Database URL"
    class_option :dry_run, type: :boolean, default: false, desc: "Print statements without executing"

    map %w[--version -v] => :version

    def self.exit_on_failure?
      true
    end

    SQL_FORMAT = {
      day: "YYYYMMDD",
      month: "YYYYMM",
      year: "YYYY"
    }

    def initialize(*args)
      $client = self
      $stdout.sync = true
      $stderr.sync = true
      super
    end

    desc "prep TABLE [COLUMN] [PERIOD]", "Create an intermediate table for partitioning"
    option :partition, type: :boolean, default: true, desc: "Partition the table"
    option :trigger_based, type: :boolean, default: false, desc: "Use trigger-based partitioning"
    def prep(table, column=nil, period=nil)
      table = create_table(table)
      intermediate_table = table.intermediate_table
      trigger_name = table.trigger_name

      unless options[:partition]
        abort "Usage: \"pgslice prep TABLE --no-partition\"" if column || period
        abort "Can't use --trigger-based and --no-partition" if options[:trigger_based]
      end
      assert_table(table)
      assert_no_table(intermediate_table)

      if options[:partition]
        abort "Usage: \"pgslice prep TABLE COLUMN PERIOD\"" if !(column && period)
        abort "Column not found: #{column}" unless table.columns.include?(column)
        abort "Invalid period: #{period}" unless SQL_FORMAT[period.to_sym]
      end

      queries = []

      declarative = server_version_num >= 100000 && !options[:trigger_based]

      if declarative && options[:partition]
        queries << <<-SQL
CREATE TABLE #{quote_table(intermediate_table)} (LIKE #{quote_table(table)} INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING STORAGE INCLUDING COMMENTS) PARTITION BY RANGE (#{quote_ident(column)});
        SQL

        if server_version_num >= 110000
          index_defs = table.index_defs
          index_defs.each do |index_def|
            queries << make_index_def(index_def, intermediate_table)
          end
        end

        # add comment
        cast = table.column_cast(column)
        queries << <<-SQL
COMMENT ON TABLE #{quote_table(intermediate_table)} is 'column:#{column},period:#{period},cast:#{cast}';
        SQL
      else
        queries << <<-SQL
CREATE TABLE #{quote_table(intermediate_table)} (LIKE #{quote_table(table)} INCLUDING ALL);
        SQL

        table.foreign_keys.each do |fk_def|
          queries << "ALTER TABLE #{quote_table(intermediate_table)} ADD #{fk_def};"
        end
      end

      if options[:partition] && !declarative
        queries << <<-SQL
CREATE FUNCTION #{quote_ident(trigger_name)}()
    RETURNS trigger AS $$
    BEGIN
        RAISE EXCEPTION 'Create partitions first.';
    END;
    $$ LANGUAGE plpgsql;
        SQL

        queries << <<-SQL
CREATE TRIGGER #{quote_ident(trigger_name)}
    BEFORE INSERT ON #{quote_table(intermediate_table)}
    FOR EACH ROW EXECUTE PROCEDURE #{quote_ident(trigger_name)}();
        SQL

        cast = table.column_cast(column)
        queries << <<-SQL
COMMENT ON TRIGGER #{quote_ident(trigger_name)} ON #{quote_table(intermediate_table)} is 'column:#{column},period:#{period},cast:#{cast}';
        SQL
      end

      run_queries(queries)
    end

    desc "unprep TABLE", "Undo prep"
    def unprep(table)
      table = create_table(table)
      intermediate_table = table.intermediate_table
      trigger_name = table.trigger_name

      assert_table(intermediate_table)

      queries = [
        "DROP TABLE #{quote_table(intermediate_table)} CASCADE;",
        "DROP FUNCTION IF EXISTS #{quote_ident(trigger_name)}();"
      ]
      run_queries(queries)
    end

    desc "add_partitions TABLE", "Add partitions"
    option :intermediate, type: :boolean, default: false, desc: "Add to intermediate table"
    option :past, type: :numeric, default: 0, desc: "Number of past partitions to add"
    option :future, type: :numeric, default: 0, desc: "Number of future partitions to add"
    def add_partitions(table)
      original_table = create_table(table)
      table = options[:intermediate] ? original_table.intermediate_table : original_table
      trigger_name = original_table.trigger_name

      assert_table(table)

      future = options[:future]
      past = options[:past]
      range = (-1 * past)..future

      period, field, cast, needs_comment, declarative = fetch_settings(original_table, table)
      unless period
        message = "No settings found: #{table}"
        message = "#{message}\nDid you mean to use --intermediate?" unless options[:intermediate]
        abort message
      end

      queries = []

      if needs_comment
        queries << "COMMENT ON TRIGGER #{quote_ident(trigger_name)} ON #{quote_table(table)} is 'column:#{field},period:#{period},cast:#{cast}';"
      end

      # today = utc date
      today = round_date(Time.now.utc.to_date, period)

      schema_table =
        if !declarative
          table
        elsif options[:intermediate]
          original_table
        else
          original_table.existing_partitions(period).last
        end

      # indexes automatically propagate in Postgres 11+
      index_defs =
        if !declarative || server_version_num < 110000
          schema_table.index_defs
        else
          []
        end

      fk_defs = schema_table.foreign_keys
      primary_key = schema_table.primary_key

      added_partitions = []
      range.each do |n|
        day = advance_date(today, period, n)

        partition = Table.new(original_table.schema, "#{original_table.name}_#{day.strftime(name_format(period))}")
        next if partition.exists?
        added_partitions << partition

        if declarative
          queries << <<-SQL
CREATE TABLE #{quote_table(partition)} PARTITION OF #{quote_table(table)} FOR VALUES FROM (#{sql_date(day, cast, false)}) TO (#{sql_date(advance_date(day, period, 1), cast, false)});
          SQL
        else
          queries << <<-SQL
CREATE TABLE #{quote_table(partition)}
    (CHECK (#{quote_ident(field)} >= #{sql_date(day, cast)} AND #{quote_ident(field)} < #{sql_date(advance_date(day, period, 1), cast)}))
    INHERITS (#{quote_table(table)});
          SQL
        end

        queries << "ALTER TABLE #{quote_table(partition)} ADD PRIMARY KEY (#{primary_key.map { |k| quote_ident(k) }.join(", ")});" if primary_key.any?

        index_defs.each do |index_def|
          queries << make_index_def(index_def, partition)
        end

        fk_defs.each do |fk_def|
          queries << "ALTER TABLE #{quote_table(partition)} ADD #{fk_def};"
        end
      end

      unless declarative
        # update trigger based on existing partitions
        current_defs = []
        future_defs = []
        past_defs = []
        name_format = self.name_format(period)
        existing_tables = original_table.existing_partitions(period)
        existing_tables = (existing_tables + added_partitions).uniq(&:name).sort_by(&:name)

        existing_tables.each do |existing_table|
          day = DateTime.strptime(existing_table.name.split("_").last, name_format)
          partition = Table.new(original_table.schema, "#{original_table.name}_#{day.strftime(name_format(period))}")

          sql = "(NEW.#{quote_ident(field)} >= #{sql_date(day, cast)} AND NEW.#{quote_ident(field)} < #{sql_date(advance_date(day, period, 1), cast)}) THEN
              INSERT INTO #{quote_table(partition)} VALUES (NEW.*);"

          if day.to_date < today
            past_defs << sql
          elsif advance_date(day, period, 1) < today
            current_defs << sql
          else
            future_defs << sql
          end
        end

        # order by current period, future periods asc, past periods desc
        trigger_defs = current_defs + future_defs + past_defs.reverse

        if trigger_defs.any?
          queries << <<-SQL
CREATE OR REPLACE FUNCTION #{quote_ident(trigger_name)}()
    RETURNS trigger AS $$
    BEGIN
        IF #{trigger_defs.join("\n        ELSIF ")}
        ELSE
            RAISE EXCEPTION 'Date out of range. Ensure partitions are created.';
        END IF;
        RETURN NULL;
    END;
    $$ LANGUAGE plpgsql;
          SQL
        end
      end

      run_queries(queries) if queries.any?
    end

    desc "fill TABLE", "Fill the partitions in batches"
    option :batch_size, type: :numeric, default: 10000, desc: "Batch size"
    option :swapped, type: :boolean, default: false, desc: "Use swapped table"
    option :source_table, desc: "Source table"
    option :dest_table, desc: "Destination table"
    option :start, type: :numeric, desc: "Primary key to start"
    option :where, desc: "Conditions to filter"
    option :sleep, type: :numeric, desc: "Seconds to sleep between batches"
    def fill(table)
      table = create_table(table)
      source_table = create_table(options[:source_table]) if options[:source_table]
      dest_table = create_table(options[:dest_table]) if options[:dest_table]

      if options[:swapped]
        source_table ||= table.retired_table
        dest_table ||= table
      else
        source_table ||= table
        dest_table ||= table.intermediate_table
      end

      assert_table(source_table)
      assert_table(dest_table)

      period, field, cast, _needs_comment, declarative = fetch_settings(table, dest_table)

      if period
        name_format = self.name_format(period)

        existing_tables = table.existing_partitions(period)
        if existing_tables.any?
          starting_time = DateTime.strptime(existing_tables.first.name.split("_").last, name_format)
          ending_time = advance_date(DateTime.strptime(existing_tables.last.name.split("_").last, name_format), period, 1)
        end
      end

      schema_table = period && declarative ? existing_tables.last : table

      primary_key = schema_table.primary_key[0]
      abort "No primary key" unless primary_key

      max_source_id = nil
      begin
        max_source_id = source_table.max_id(primary_key)
      rescue PG::UndefinedFunction
        abort "Only numeric primary keys are supported"
      end

      max_dest_id =
        if options[:start]
          options[:start]
        elsif options[:swapped]
          dest_table.max_id(primary_key, where: options[:where], below: max_source_id)
        else
          dest_table.max_id(primary_key, where: options[:where])
        end

      if max_dest_id == 0 && !options[:swapped]
        min_source_id = source_table.min_id(primary_key, field, cast, starting_time, options[:where])
        max_dest_id = min_source_id - 1 if min_source_id
      end

      starting_id = max_dest_id
      fields = source_table.columns.map { |c| quote_ident(c) }.join(", ")
      batch_size = options[:batch_size]

      i = 1
      batch_count = ((max_source_id - starting_id) / batch_size.to_f).ceil

      if batch_count == 0
        log_sql "/* nothing to fill */"
      end

      while starting_id < max_source_id
        where = "#{quote_ident(primary_key)} > #{starting_id} AND #{quote_ident(primary_key)} <= #{starting_id + batch_size}"
        if starting_time
          where << " AND #{quote_ident(field)} >= #{sql_date(starting_time, cast)} AND #{quote_ident(field)} < #{sql_date(ending_time, cast)}"
        end
        if options[:where]
          where << " AND #{options[:where]}"
        end

        query = <<-SQL
/* #{i} of #{batch_count} */
INSERT INTO #{quote_table(dest_table)} (#{fields})
    SELECT #{fields} FROM #{quote_table(source_table)}
    WHERE #{where}
        SQL

        run_query(query)

        starting_id += batch_size
        i += 1

        if options[:sleep] && starting_id <= max_source_id
          sleep(options[:sleep])
        end
      end
    end

    desc "swap TABLE", "Swap the intermediate table with the original table"
    option :lock_timeout, default: "5s", desc: "Lock timeout"
    def swap(table)
      table = create_table(table)
      intermediate_table = table.intermediate_table
      retired_table = table.retired_table

      assert_table(table)
      assert_table(intermediate_table)
      assert_no_table(retired_table)

      queries = [
        "ALTER TABLE #{quote_table(table)} RENAME TO #{quote_no_schema(retired_table)};",
        "ALTER TABLE #{quote_table(intermediate_table)} RENAME TO #{quote_no_schema(table)};"
      ]

      table.sequences.each do |sequence|
        queries << "ALTER SEQUENCE #{quote_ident(sequence["sequence_name"])} OWNED BY #{quote_table(table)}.#{quote_ident(sequence["related_column"])};"
      end

      queries.unshift("SET LOCAL lock_timeout = '#{options[:lock_timeout]}';") if server_version_num >= 90300

      run_queries(queries)
    end

    desc "unswap TABLE", "Undo swap"
    def unswap(table)
      table = create_table(table)
      intermediate_table = table.intermediate_table
      retired_table = table.retired_table

      assert_table(table)
      assert_table(retired_table)
      assert_no_table(intermediate_table)

      queries = [
        "ALTER TABLE #{quote_table(table)} RENAME TO #{quote_no_schema(intermediate_table)};",
        "ALTER TABLE #{quote_table(retired_table)} RENAME TO #{quote_no_schema(table)};"
      ]

      table.sequences.each do |sequence|
        queries << "ALTER SEQUENCE #{quote_ident(sequence["sequence_name"])} OWNED BY #{quote_table(table)}.#{quote_ident(sequence["related_column"])};"
      end

      run_queries(queries)
    end

    desc "analyze TABLE", "Analyze tables"
    option :swapped, type: :boolean, default: false, desc: "Use swapped table"
    def analyze(table)
      table = create_table(table)
      parent_table = options[:swapped] ? table : table.intermediate_table

      existing_tables = table.existing_partitions
      analyze_list = existing_tables + [parent_table]
      run_queries_without_transaction(analyze_list.map { |t| "ANALYZE VERBOSE #{quote_table(t)};" })
    end

    desc "version", "Show version"
    def version
      log("pgslice #{PgSlice::VERSION}")
    end

    protected

    # output

    def log(message = nil)
      error message
    end

    def log_sql(message = nil)
      say message
    end

    def abort(message)
      raise Thor::Error, message
    end

    # database connection

    def connection
      @connection ||= begin
        url = options[:url] || ENV["PGSLICE_URL"]
        abort "Set PGSLICE_URL or use the --url option" unless url

        uri = URI.parse(url)
        params = CGI.parse(uri.query.to_s)
        # remove schema
        @schema = Array(params.delete("schema") || "public")[0]
        uri.query = URI.encode_www_form(params)

        ENV["PGCONNECT_TIMEOUT"] ||= "1"
        PG::Connection.new(uri.to_s)
      end
    rescue PG::ConnectionBad => e
      abort e.message
    rescue URI::InvalidURIError
      abort "Invalid url"
    end

    def schema
      connection # ensure called first
      @schema
    end

    def execute(query, params = [])
      connection.exec_params(query, params).to_a
    end

    def run_queries(queries)
      connection.transaction do
        execute("SET LOCAL client_min_messages TO warning") unless options[:dry_run]
        log_sql "BEGIN;"
        log_sql
        run_queries_without_transaction(queries)
        log_sql "COMMIT;"
      end
    end

    def run_query(query)
      log_sql query
      unless options[:dry_run]
        begin
          execute(query)
        rescue PG::ServerError => e
          abort("#{e.class.name}: #{e.message}")
        end
      end
      log_sql
    end

    def run_queries_without_transaction(queries)
      queries.each do |query|
        run_query(query)
      end
    end

    def server_version_num
      execute("SHOW server_version_num")[0]["server_version_num"].to_i
    end

    # helpers

    def sql_date(time, cast, add_cast = true)
      if cast == "timestamptz"
        fmt = "%Y-%m-%d %H:%M:%S UTC"
      else
        fmt = "%Y-%m-%d"
      end
      str = "'#{time.strftime(fmt)}'"
      add_cast ? "#{str}::#{cast}" : str
    end

    def name_format(period)
      case period.to_sym
      when :day
        "%Y%m%d"
      when :month
        "%Y%m"
      else
        "%Y"
      end
    end

    def round_date(date, period)
      date = date.to_date
      case period.to_sym
      when :day
        date
      when :month
        Date.new(date.year, date.month)
      else
        Date.new(date.year)
      end
    end

    def assert_table(table)
      abort "Table not found: #{table}" unless table.exists?
    end

    def assert_no_table(table)
      abort "Table already exists: #{table}" if table.exists?
    end

    def advance_date(date, period, count = 1)
      date = date.to_date
      case period.to_sym
      when :day
        date.next_day(count)
      when :month
        date.next_month(count)
      else
        date.next_year(count)
      end
    end

    def quote_ident(value)
      PG::Connection.quote_ident(value)
    end

    def quote_table(table)
      table.quote_table
    end

    def quote_no_schema(table)
      quote_ident(table.name)
    end

    def create_table(name)
      if name.include?(".")
        schema, name = name.split(".", 2)
      else
        schema = self.schema
      end
      Table.new(schema, name)
    end

    def make_index_def(index_def, table)
      index_def.sub(/ ON \S+ USING /, " ON #{quote_table(table)} USING ").sub(/ INDEX .+ ON /, " INDEX ON ") + ";"
    end

    def fetch_settings(original_table, table)
      trigger_name = original_table.trigger_name

      needs_comment = false
      trigger_comment = table.fetch_trigger(trigger_name)
      comment = trigger_comment || table.fetch_comment
      if comment
        field, period, cast = comment["comment"].split(",").map { |v| v.split(":").last } rescue [nil, nil, nil]
      end

      unless period
        needs_comment = true
        function_def = execute("SELECT pg_get_functiondef(oid) FROM pg_proc WHERE proname = $1", [trigger_name])[0]
        return [] unless function_def
        function_def = function_def["pg_get_functiondef"]
        sql_format = SQL_FORMAT.find { |_, f| function_def.include?("'#{f}'") }
        return [] unless sql_format
        period = sql_format[0]
        field = /to_char\(NEW\.(\w+),/.match(function_def)[1]
      end

      # backwards compatibility with 0.2.3 and earlier (pre-timestamptz support)
      unless cast
        cast = "date"
        # update comment to explicitly define cast
        needs_comment = true
      end

      [period, field, cast, needs_comment, !trigger_comment]
    end
  end
end
