require "test_helper"

class DataKeeperTest < BaseTest
  def test_asserts_on_dump_type
    assert_raises(DataKeeper::InvalidDumpType) do
      DataKeeper.define_dump(:name, "invalid")
    end
  end

  def test_asserts_on_duplicated_table_name
    DataKeeper.define_dump(:name) do |d|
      d.table "foobar"
      d.table "foobar"
    end

    assert_raises(DataKeeper::InvalidDumpDefinition) do
      DataKeeper.create_dump!(:name)
    end
  end

  def test_asserts_dump_exists_on_load
    assert_raises(DataKeeper::DumpDoesNotExist) do
      DataKeeper.create_dump!("missing")
    end
  end

  def test_partial_dump_creation_by_tables
    DataKeeper.define_dump(:name) do |d|
      d.table "users"
    end

    User.create! name: "Pepe"

    DataKeeper.create_dump! :name

    assert_equal 1, @storage.files.size
    assert_equal :name, @storage.files.first[:dump_name]
  end
end
