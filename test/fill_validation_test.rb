require "minitest/autorun"
require_relative "../lib/pgslice"

PgSlice::CLI.exit_on_failure = false

class FillValidationTest < Minitest::Test
  class TableSetupReached < StandardError; end

  def test_rejects_invalid_batch_sizes_before_table_or_database_access
    [0, -1, 0.5, 1.5].each do |batch_size|
      cli = PgSlice::CLI.allocate
      cli.define_singleton_method(:options) { {batch_size: batch_size} }
      cli.define_singleton_method(:create_table) { |_| raise TableSetupReached }

      error = assert_raises(Thor::Error) { cli.fill("events") }
      assert_equal "Batch size must be a positive integer", error.message
    end
  end

  def test_accepts_positive_integer_batch_size_before_table_setup
    cli = PgSlice::CLI.allocate
    cli.define_singleton_method(:options) { {batch_size: 10} }
    cli.define_singleton_method(:create_table) { |_| raise TableSetupReached }

    assert_raises(TableSetupReached) { cli.fill("events") }
  end
end
