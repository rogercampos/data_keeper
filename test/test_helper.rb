$LOAD_PATH.unshift File.expand_path("../lib", __dir__)
require "data_keeper"

require "minitest/autorun"

class SimpleStorage
  attr_reader :dir

  def initialize
    @dir = File.join File.expand_path("../lib", __dir__), "tmp"
  end

  def save(file, filename, dump_name)
    FileUtils.mkdir_p File.join(@dir, "tmp")
    File.cp file.path, File.join(@dir, dump_name)
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
    FileUtils.rmdir @storage.dir if File.file?(@storage.dir)
  end
end