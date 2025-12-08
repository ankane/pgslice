module PgSlice
  class CLI
    desc "synchronize TABLE", "Synchronize data between two tables"
    option :source_table, type: :string, desc: "Source table to compare (default: TABLE)"
    option :target_table, type: :string, desc: "Target table to compare (default: TABLE_intermediate)"
    option :primary_key, type: :string, desc: "Primary key column name"
    option :start, type: :string, desc: "Primary key value to start synchronization at"
    option :window_size, type: :numeric, default: 1000, desc: "Number of rows to synchronize per batch"
    option :delay, type: :numeric, default: 0, desc: "Base delay in seconds between batches (M)"
    option :delay_multiplier, type: :numeric, default: 0, desc: "Delay multiplier for batch time (P)"
    def synchronize(table_name)
      table = create_table(table_name)

      # Determine source and target tables
      source_table = options[:source_table] ? create_table(options[:source_table]) : table
      target_table = options[:target_table] ? create_table(options[:target_table]) : table.intermediate_table

      # Verify both tables exist
      assert_table(source_table)
      assert_table(target_table)

      # Get and verify schemas match
      source_schema = get_table_schema(source_table)
      target_schema = get_table_schema(target_table)
      verify_schemas_match(source_table, target_table, source_schema, target_schema)

      # Get primary key
      primary_key = options[:primary_key] || source_table.primary_key&.first
      abort "Primary key not found. Specify with --primary-key" unless primary_key
      abort "Primary key '#{primary_key}' not found in source table" unless source_schema[primary_key]

      # Determine starting value
      starting_id = options[:start]
      unless starting_id
        starting_id = get_min_id(source_table, primary_key)
        abort "No rows found in source table" unless starting_id
      end

      # Get parameters
      window_size = options[:window_size]
      base_delay = options[:delay]
      delay_multiplier = options[:delay_multiplier]
      dry_run = options[:dry_run]

      log "Synchronizing #{source_table} to #{target_table}"
      log "Mode: #{dry_run ? 'DRY RUN (logging only)' : 'WRITE (executing changes)'}"
      log "Primary key: #{primary_key}"
      log "Starting at: #{starting_id}"
      log "Window size: #{window_size}"
      log "Base delay: #{base_delay}s"
      log "Delay multiplier: #{delay_multiplier}"
      log

      # Statistics
      stats = {
        total_rows: 0,
        matching_rows: 0,
        rows_with_differences: 0,
        missing_rows: 0,
        extra_rows: 0,
        batches: 0
      }

      columns = source_schema.keys

      # Main synchronization loop
      first_batch = true
      loop do
        batch_start_time = Time.now

        # Fetch batch from source
        source_rows = fetch_batch(source_table, primary_key, starting_id, window_size, columns, first_batch)
        break if source_rows.empty?

        stats[:batches] += 1
        first_batch = false
        stats[:total_rows] += source_rows.size

        # Get primary keys and range from source batch
        source_pks = source_rows.map { |row| row[primary_key] }
        first_source_pk = source_rows.first[primary_key]
        last_source_pk = source_rows.last[primary_key]

        # Fetch corresponding rows from target using range query to catch deletions
        target_rows = fetch_rows_by_range(target_table, primary_key, first_source_pk, last_source_pk, columns)
        target_rows_by_pk = target_rows.each_with_object({}) { |row, hash| hash[row[primary_key]] = row }

        # Compare and generate fix queries
        fix_queries = []

        source_rows.each do |source_row|
          pk_value = source_row[primary_key]
          target_row = target_rows_by_pk[pk_value]

          if target_row.nil?
            # Missing row in target
            stats[:missing_rows] += 1
            fix_queries << generate_insert(target_table, source_row, columns)
          elsif rows_differ?(source_row, target_row, columns)
            # Rows differ
            stats[:rows_with_differences] += 1
            fix_queries << generate_update(target_table, primary_key, source_row, columns)
          else
            # Rows match
            stats[:matching_rows] += 1
          end
        end

        # Check for extra rows in target (rows in target but not in source batch)
        # Note: This only checks within the current batch window
        extra_pks = target_rows_by_pk.keys - source_pks
        extra_pks.each do |pk_value|
          stats[:extra_rows] += 1
          fix_queries << generate_delete(target_table, primary_key, pk_value)
        end

        # Get first and last primary key for logging
        first_pk = source_rows.first[primary_key]
        last_pk = source_rows.last[primary_key]
        pk_range = first_pk == last_pk ? "#{first_pk}" : "#{first_pk}...#{last_pk}"

        # Execute or log fix queries
        if fix_queries.any?
          log_with_timestamp "Batch #{stats[:batches]}: Found #{fix_queries.size} differences (keys in range #{pk_range})"
          if dry_run
            log_sql "-- Dry run mode: logging statements (not executing)"
            fix_queries.each { |query| log_sql query }
            log_sql
          else
            # In write mode, log truncated SQL and execute without auto-logging
            fix_queries.each { |query| log_sql truncate_sql_for_log(query) }
            run_queries(fix_queries, silent: true)
          end
        else
          log_with_timestamp "Batch #{stats[:batches]}: All #{source_rows.size} rows match (keys in range #{pk_range})"
        end

        # Update starting_id for next batch (use > not >=)
        starting_id = source_rows.last[primary_key]

        # Calculate adaptive delay: M + N*P
        batch_duration = Time.now - batch_start_time
        sleep_time = base_delay + (batch_duration * delay_multiplier)
        if sleep_time > 0
          log_with_timestamp "Sleeping #{sleep_time.round(2)}s (#{base_delay}s base + #{batch_duration.round(2)}s batch time * #{delay_multiplier} multiplier)"
          sleep(sleep_time)
        end

        # Break if we processed fewer rows than window size (last batch)
        break if source_rows.size < window_size
      end

      # Print summary
      log
      log "Synchronization complete"
      log "=" * 50
      log "Total batches: #{stats[:batches]}"
      log "Total rows compared: #{stats[:total_rows]}"
      log "Matching rows: #{stats[:matching_rows]}"
      log "Rows with differences: #{stats[:rows_with_differences]}"
      log "Missing rows: #{stats[:missing_rows]}"
      log "Extra rows: #{stats[:extra_rows]}"
    end

    private

    def log_with_timestamp(message)
      timestamp = Time.now.strftime("%Y-%m-%d %H:%M:%S")
      log "[#{timestamp}] #{message}"
    end

    def get_table_schema(table)
      query = <<~SQL
        SELECT column_name, data_type, character_maximum_length, numeric_precision, numeric_scale
        FROM information_schema.columns
        WHERE table_schema = $1 AND table_name = $2 AND is_generated = 'NEVER'
        ORDER BY ordinal_position
      SQL
      rows = execute(query, [table.schema, table.name])
      rows.each_with_object({}) do |row, hash|
        hash[row["column_name"]] = {
          data_type: row["data_type"],
          character_maximum_length: row["character_maximum_length"],
          numeric_precision: row["numeric_precision"],
          numeric_scale: row["numeric_scale"]
        }
      end
    end

    def verify_schemas_match(source_table, target_table, source_schema, target_schema)
      source_schema.each do |col_name, col_spec|
        target_spec = target_schema[col_name]
        abort "Column '#{col_name}' exists in #{source_table} but not in #{target_table}" unless target_spec

        if col_spec[:data_type] != target_spec[:data_type]
          abort "Column '#{col_name}' type mismatch: #{source_table} has #{col_spec[:data_type]}, #{target_table} has #{target_spec[:data_type]}"
        end
      end

      target_schema.each do |col_name, _|
        abort "Column '#{col_name}' exists in #{target_table} but not in #{source_table}" unless source_schema[col_name]
      end
    end

    def get_min_id(table, primary_key)
      query = "SELECT #{quote_ident(primary_key)} FROM #{quote_table(table)} ORDER BY #{quote_ident(primary_key)} LIMIT 1"
      result = execute(query)
      result.first&.values&.first
    end

    def fetch_batch(table, primary_key, starting_id, limit, columns, first_batch = false)
      column_list = columns.map { |c| quote_ident(c) }.join(", ")
      # Use >= for first batch to include starting_id, > for subsequent batches
      operator = first_batch ? ">=" : ">"
      query = <<~SQL
        SELECT #{column_list}
        FROM #{quote_table(table)}
        WHERE #{quote_ident(primary_key)} #{operator} #{quote(starting_id)}
        ORDER BY #{quote_ident(primary_key)}
        LIMIT #{limit.to_i}
      SQL
      execute(query)
    end

    def fetch_rows_by_pks(table, primary_key, pk_values, columns)
      return [] if pk_values.empty?

      column_list = columns.map { |c| quote_ident(c) }.join(", ")
      # Build IN clause with proper quoting
      pk_list = pk_values.map { |pk| quote(pk) }.join(", ")
      query = <<~SQL
        SELECT #{column_list}
        FROM #{quote_table(table)}
        WHERE #{quote_ident(primary_key)} IN (#{pk_list})
      SQL
      execute(query)
    end

    def fetch_rows_by_range(table, primary_key, first_pk, last_pk, columns)
      column_list = columns.map { |c| quote_ident(c) }.join(", ")
      query = <<~SQL
        SELECT #{column_list}
        FROM #{quote_table(table)}
        WHERE #{quote_ident(primary_key)} >= #{quote(first_pk)}
          AND #{quote_ident(primary_key)} <= #{quote(last_pk)}
        ORDER BY #{quote_ident(primary_key)}
      SQL
      execute(query)
    end

    def rows_differ?(source_row, target_row, columns)
      columns.any? { |col| source_row[col] != target_row[col] }
    end

    def generate_insert(table, row, columns)
      column_list = columns.map { |c| quote_ident(c) }.join(", ")
      value_list = columns.map { |c| quote(row[c]) }.join(", ")
      "INSERT INTO #{quote_table(table)} (#{column_list}) VALUES (#{value_list});"
    end

    def generate_update(table, primary_key, row, columns)
      set_clause = columns.reject { |c| c == primary_key }.map { |c| "#{quote_ident(c)} = #{quote(row[c])}" }.join(", ")
      "UPDATE #{quote_table(table)} SET #{set_clause} WHERE #{quote_ident(primary_key)} = #{quote(row[primary_key])};"
    end

    def generate_delete(table, primary_key, pk_value)
      "DELETE FROM #{quote_table(table)} WHERE #{quote_ident(primary_key)} = #{quote(pk_value)};"
    end

    def truncate_sql_for_log(sql)
      # For INSERT statements: show "INSERT INTO table... VALUES(first 20 chars...[truncated]"
      if sql =~ /\A(INSERT INTO [^\s]+)\s.*?\sVALUES\s*\((.*)\);?\z/i
        table_part = $1
        values_part = $2
        preview = values_part[0, 20]
        return "#{table_part}... VALUES(#{preview}...[truncated]"
      end

      # For UPDATE statements: show "UPDATE table... SET...[truncated]"
      if sql =~ /\A(UPDATE [^\s]+)\s+SET\s+(.*?)\s+WHERE/i
        table_part = $1
        set_part = $2
        preview = set_part[0, 20]
        return "#{table_part}... SET #{preview}...[truncated]"
      end

      # For DELETE statements: show "DELETE FROM table WHERE...[truncated]"
      if sql =~ /\A(DELETE FROM [^\s]+)\s+WHERE\s+(.*);?\z/i
        table_part = $1
        where_part = $2
        preview = where_part[0, 20]
        return "#{table_part}... WHERE #{preview}...[truncated]"
      end

      # Fallback: just show first 50 chars
      sql[0, 50] + "...[truncated]"
    end
  end
end
