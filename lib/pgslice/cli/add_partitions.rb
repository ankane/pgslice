module PgSlice
  class CLI
    desc "add_partitions TABLE", "Add partitions"
    option :intermediate, type: :boolean, default: false, desc: "Add to intermediate table"
    option :past, type: :numeric, default: 0, desc: "Number of past partitions to add"
    option :future, type: :numeric, default: 0, desc: "Number of future partitions to add"
    option :tablespace, type: :string, default: "", desc: "Tablespace to use"
    def add_partitions(table)
      original_table = create_table(table)
      table = options[:intermediate] ? original_table.intermediate_table : original_table
      trigger_name = original_table.trigger_name

      assert_table(table)

      future = options[:future]
      past = options[:past]
      tablespace = options[:tablespace]
      range = (-1 * past)..future

      period, field, cast, needs_comment, declarative, version = table.fetch_settings(original_table.trigger_name)
      unless period
        message = "No settings found: #{table}"
        message = "#{message}\nDid you mean to use --intermediate?" unless options[:intermediate]
        abort message
      end

      queries = []

      if needs_comment
        queries << "COMMENT ON TRIGGER #{quote_ident(trigger_name)} ON #{quote_table(table)} IS 'column:#{field},period:#{period},cast:#{cast}';"
      end

      # today = utc date
      today = round_date(Time.now.utc.to_date, period)

      schema_table =
        if !declarative
          table
        elsif options[:intermediate]
          original_table
        else
          table.partitions.last
        end

      # indexes automatically propagate in Postgres 11+
      if version < 3
        index_defs = schema_table.index_defs
        fk_defs = schema_table.foreign_keys
      else
        index_defs = []
        fk_defs = []
      end

      primary_key = schema_table.primary_key
      tablespace_str = tablespace.empty? ? "" : " TABLESPACE #{quote_ident(tablespace)}"

      added_partitions = []
      range.each do |n|
        day = advance_date(today, period, n)

        partition = Table.new(original_table.schema, "#{original_table.name}_#{day.strftime(name_format(period))}")
        next if partition.exists?
        added_partitions << partition

        if declarative
          queries << <<-SQL
CREATE TABLE #{quote_table(partition)} PARTITION OF #{quote_table(table)} FOR VALUES FROM (#{sql_date(day, cast, false)}) TO (#{sql_date(advance_date(day, period, 1), cast, false)})#{tablespace_str};
          SQL
        else
          queries << <<-SQL
CREATE TABLE #{quote_table(partition)}
    (CHECK (#{quote_ident(field)} >= #{sql_date(day, cast)} AND #{quote_ident(field)} < #{sql_date(advance_date(day, period, 1), cast)}))
    INHERITS (#{quote_table(table)})#{tablespace_str};
          SQL
        end

        queries << "ALTER TABLE #{quote_table(partition)} ADD PRIMARY KEY (#{primary_key.map { |k| quote_ident(k) }.join(", ")});" if primary_key.any?

        index_defs.each do |index_def|
          queries << make_index_def(index_def, partition)
        end

        fk_defs.each do |fk_def|
          queries << make_fk_def(fk_def, partition)
        end
      end

      unless declarative
        # update trigger based on existing partitions
        current_defs = []
        future_defs = []
        past_defs = []
        name_format = self.name_format(period)
        partitions = (table.partitions + added_partitions).uniq(&:name).sort_by(&:name)

        partitions.each do |partition|
          day = partition_date(partition, name_format)

          # note: does not support generated columns
          # could support by listing columns
          # but this would cause issues with schema changes
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
  end
end
