require "data_keeper/version"
require "terrapin"
require "zlib"
require "rubygems/package"

require 'data_keeper/error'
require 'data_keeper/definition'
require 'data_keeper/dumper'
require 'data_keeper/loader'
require 'data_keeper/local_storage'
require 'data_keeper/database_helper'
require 'data_keeper/railtie' if defined?(Rails) && defined?(Rails::Railtie)

module DataKeeper
  DumpDoesNotExist = Class.new(Error)
  NoStorageDefined = Class.new(Error)

  @dumps = {}
  @storage = nil

  def self.define_dump(name, type = :partial, &block)
    @dumps[name.to_sym] = DefinitionBuilder.build(type, block)
  end

  def self.create_dump!(name)
    raise DumpDoesNotExist unless dump?(name)
    raise NoStorageDefined if @storage.nil?

    Dumper.new(name, @dumps[name.to_sym]).run! do |file, filename|
      @storage.save(file, filename, name)
    end
  end

  def self.fetch_and_load_dump!(name)
    raise DumpDoesNotExist unless dump?(name)
    raise NoStorageDefined if @storage.nil?

    @storage.retrieve(name) do |file|
      Loader.new(@dumps[name.to_sym], file).load!
    end
  end

  def self.load_dump!(name, path)
    raise DumpDoesNotExist unless File.file?(path)
    raise NoStorageDefined if @storage.nil?

    Loader.new(@dumps[name.to_sym], File.open(path)).load!
  end

  def self.dump?(name)
    @dumps.key?(name.to_sym)
  end

  def self.storage=(value)
    @storage = value
  end

  def self.clear_dumps!
    @dumps = {}
  end
end
