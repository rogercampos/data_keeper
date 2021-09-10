$LOAD_PATH.unshift File.expand_path("../lib", __dir__)
require "data_keeper"

require "minitest/autorun"
require 'active_record'

class SimpleStorage
  attr_reader :dir, :files

  def initialize
    @dir = File.join File.expand_path("../lib", __dir__), "tmp"
    @files = []
  end

  def save(file, filename, dump_name)
    FileUtils.mkdir_p File.join(@dir, "tmp")
    FileUtils.cp file.path, File.join(@dir, dump_name.to_s)
    @files.push(
      path: File.join(@dir, dump_name.to_s),
      filename: filename,
      dump_name: dump_name
    )
  end

  def retrieve(dump_name)
    yield File.open File.join(@dir, dump_name)
  end
end

class BaseTest < Minitest::Test
  def setup
    super
    @storage = SimpleStorage.new
    DataKeeper.storage = @storage
  end

  def teardown
    super
    DataKeeper.clear_dumps!
    User.delete_all
    Post.delete_all
    FileUtils.rmdir @storage.dir if File.file?(@storage.dir)
  end
end

DataKeeper.database_config = -> {
  {
    "username" => "elnner",
    "password" => '',
    "database" => "data_keeper_test"
  }
}

connection_opts = { adapter: "postgresql", database: "data_keeper_test", username: "elnner" }
ActiveRecord::Base.establish_connection(connection_opts)

begin
  ActiveRecord::Schema.define do
    create_table :users do |t|
      t.string :name
    end

    create_table :posts do |t|
      t.string :title
    end
  end
rescue ActiveRecord::StatementInvalid
end

class User < ActiveRecord::Base
end

class Post < ActiveRecord::Base
end
