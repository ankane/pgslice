module PgSlice
  class CLI
    desc "disable_retired_mirroring TABLE", "Disable mirroring triggers from TABLE to TABLE_retired"
    def disable_retired_mirroring(table_name)
      table = create_table(table_name)

      assert_table(table)

      disable_retired_mirroring_triggers(table)
      log("Retired mirroring triggers disabled for #{table_name}")
    end
  end
end
