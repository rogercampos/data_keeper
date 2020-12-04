require "test_helper"

class DataKeeperTest < Minitest::Test
  def teardown
    super
    DataKeeper.clear_dumps!
  end

  def test_that_it_has_a_version_number
    assert ::DataKeeper::VERSION
  end

  def test_asserts_on_dump_type
    assert_raises(DataKeeper::InvalidDumpType) do
      DataKeeper.define_dump(:name, "invalid")
    end
  end

  def test_asserts_on_duplicated_table_name
    assert_raises(DataKeeper::InvalidDumpDefinition) do
      DataKeeper.define_dump(:name) do |d|
        d.table "foobar"
        d.table "foobar"
      end
    end
  end

  def test_asserts_dump_exists_on_load
    assert_raises(DataKeeper::DumpDoesNotExist) do
      DataKeeper.create_dump!("missing")
    end
  end
end
