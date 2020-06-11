module PgSlice
  class CLI
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
        queries << "ALTER SEQUENCE #{quote_ident(sequence["sequence_schema"])}.#{quote_ident(sequence["sequence_name"])} OWNED BY #{quote_table(table)}.#{quote_ident(sequence["related_column"])};"
      end

      run_queries(queries)
    end
  end
end
