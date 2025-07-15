module PgSlice
  class CLI
    desc "prep_hash TABLE COLUMN PARTITIONS", "Create an intermediate table for hash partitioning", hide: true
    def prep_hash(table, column, partitions)
      table = create_table(table)
      intermediate_table = table.intermediate_table
      partitions = partitions.to_i

      assert_table(table)
      assert_no_table(intermediate_table)
      abort "Partitions must be greater than 0" if partitions <= 0

      queries = []

      including = ["DEFAULTS", "CONSTRAINTS", "STORAGE", "COMMENTS", "STATISTICS", "GENERATED"]
      if server_version_num >= 140000
        including << "COMPRESSION"
      end
      queries << <<~SQL
        CREATE TABLE #{quote_table(intermediate_table)} (LIKE #{quote_table(table)} #{including.map { |v| "INCLUDING #{v}" }.join(" ")}) PARTITION BY HASH (#{quote_ident(column)});
      SQL

      primary_key = table.primary_key
      if primary_key.include?(column)
        queries << "ALTER TABLE #{quote_table(intermediate_table)} ADD PRIMARY KEY (#{primary_key.map { |k| quote_ident(k) }.join(", ")});" if primary_key.any?
      end

      table.index_defs.each do |index_def|
        queries << make_index_def(index_def, intermediate_table)
      end

      table.foreign_keys.each do |fk_def|
        queries << make_fk_def(fk_def, intermediate_table)
      end

      partitions.times do |i|
        partition = Table.new(table.schema, "#{table.name}_#{i}")
        queries << <<~SQL
          CREATE TABLE #{quote_table(partition)} PARTITION OF #{quote_table(intermediate_table)} FOR VALUES WITH (MODULUS #{partitions}, REMAINDER #{i});
        SQL

        unless primary_key.include?(column)
          queries << "ALTER TABLE #{quote_table(partition)} ADD PRIMARY KEY (#{primary_key.map { |k| quote_ident(k) }.join(", ")});" if primary_key.any?
        end
      end

      run_queries(queries)
    end
  end
end
