module PgSlice
  class CLI
    desc "fill TABLE", "Fill the partitions in batches"
    option :batch_size, type: :numeric, default: 10000, desc: "Batch size"
    option :swapped, type: :boolean, default: false, desc: "Use swapped table"
    option :source_table, desc: "Source table"
    option :dest_table, desc: "Destination table"
    option :start, type: :string, desc: "Primary key to start (numeric or ULID)"
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

      period, field, cast, _, declarative, _ = dest_table.fetch_settings(table.trigger_name)

      if period
        name_format = self.name_format(period)

        partitions = dest_table.partitions
        if partitions.any?
          starting_time = partition_date(partitions.first, name_format)
          ending_time = advance_date(partition_date(partitions.last, name_format), period, 1)
        end
      end

      schema_table = period && declarative ? partitions.last : table

      primary_key = schema_table.primary_key[0]
      abort "No primary key" unless primary_key

      max_source_id = nil
      begin
        max_source_id = source_table.max_id(primary_key)
      rescue PG::UndefinedFunction
        abort "Only numeric and ULID primary keys are supported"
      end

      max_dest_id =
        if options[:start]
          # Convert to appropriate type
          start_val = options[:start]
          numeric_id?(start_val) ? start_val.to_i : start_val
        elsif options[:swapped]
          dest_table.max_id(primary_key, where: options[:where], below: max_source_id)
        else
          dest_table.max_id(primary_key, where: options[:where])
        end

      # Get the appropriate handler for the ID type
      # Prefer --start option, then max_source_id, then sample from table
      handler = if options[:start]
        id_handler(options[:start], connection, source_table, primary_key)
      elsif max_source_id
        id_handler(max_source_id, connection, source_table, primary_key)
      else
        # Sample a row to determine ID type
        sample_query = "SELECT #{quote_ident(primary_key)} FROM #{quote_table(source_table)} LIMIT 1"
        log_sql sample_query
        sample_result = execute(sample_query)[0]
        if sample_result && sample_result[primary_key]
          id_handler(sample_result[primary_key], connection, source_table, primary_key)
        else
          # Default to numeric if we can't determine
          Helpers::NumericHandler.new
        end
      end

      if (max_dest_id == 0 || max_dest_id == handler.min_value) && !options[:swapped]
        min_source_id = source_table.min_id(primary_key, field, cast, starting_time, options[:where])
        if min_source_id
          max_dest_id = handler.predecessor(min_source_id)
        end
      end

      starting_id = max_dest_id
      fields = source_table.columns.map { |c| quote_ident(c) }.join(", ")
      batch_size = options[:batch_size]

      i = 1
      batch_count = handler.batch_count(starting_id, max_source_id, batch_size)
      first_batch = true

      if batch_count == 0
        log_sql "/* nothing to fill */"
      end

      while handler.should_continue?(starting_id, max_source_id)
        where = handler.batch_where_condition(primary_key, starting_id, batch_size, first_batch && options[:start])
        if starting_time
          where << " AND #{quote_ident(field)} >= #{sql_date(starting_time, cast)} AND #{quote_ident(field)} < #{sql_date(ending_time, cast)}"
        end
        if options[:where]
          where << " AND #{options[:where]}"
        end

        batch_label = batch_count ? "#{i} of #{batch_count}" : "batch #{i}"
        
        if handler.is_a?(UlidHandler)
          # For ULIDs, use CTE with RETURNING to get max ID inserted
          query = <<~SQL
            /* #{batch_label} */
            WITH inserted_batch AS (
              INSERT INTO #{quote_table(dest_table)} (#{fields})
                  SELECT #{fields} FROM #{quote_table(source_table)}
                  WHERE #{where}
                  ORDER BY #{quote_ident(primary_key)}
                  LIMIT #{batch_size}
                  ON CONFLICT DO NOTHING
                  RETURNING #{quote_ident(primary_key)}
            )
            SELECT MAX(#{quote_ident(primary_key)}) as max_inserted_id FROM inserted_batch
          SQL
          
          log_sql query
          result = execute(query)
          max_inserted_id = result[0]["max_inserted_id"]
          puts "starting_id: #{starting_id}"
          puts "max_inserted_id: #{max_inserted_id}"
          
          # If no records were inserted, break the loop
          if max_inserted_id.nil?
            break
          end
          
          starting_id = max_inserted_id
        else
          query = <<~SQL
            /* #{batch_label} */
            INSERT INTO #{quote_table(dest_table)} (#{fields})
                SELECT #{fields} FROM #{quote_table(source_table)}
                WHERE #{where}
                ON CONFLICT DO NOTHING
          SQL
          
          run_query(query)
          starting_id = handler.next_starting_id(starting_id, batch_size)
        end
        
        i += 1
        first_batch = false

        if options[:sleep] && handler.should_continue?(starting_id, max_source_id)
          sleep(options[:sleep])
        end
      end
    end
  end
end
