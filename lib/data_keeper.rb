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

  @dump_definition_builders = {}
  @storage = nil
  @database_config = -> { Rails.configuration.database_configuration[Rails.env] }

  def self.define_dump(name, type = :partial, &block)
    @dump_definition_builders[name.to_sym] = DefinitionBuilder.new(type, block)
  end

  def self.create_dump!(name)
    raise DumpDoesNotExist unless dump?(name)
    raise NoStorageDefined if @storage.nil?

    definition = @dump_definition_builders[name.to_sym].evaluate!

    Dumper.new(name, definition).run! do |file, filename|
      @storage.save(file, filename, name)
    end
  end

  def self.fetch_and_load_dump!(name)
    raise DumpDoesNotExist unless dump?(name)
    raise NoStorageDefined if @storage.nil?
    definition = @dump_definition_builders[name.to_sym].evaluate!

    @storage.retrieve(name) do |file|
      Loader.new(definition, file).load!
    end
  end

  def self.download_dump!(name, path)
    raise DumpDoesNotExist unless dump?(name)
    raise NoStorageDefined if @storage.nil?

    @storage.retrieve(name) do |file, filename|
      FileUtils.cp file.path, File.join(path, filename)
    end
  end

  def self.load_dump!(name, path)
    raise DumpDoesNotExist unless File.file?(path)
    raise NoStorageDefined if @storage.nil?

    definition = @dump_definition_builders[name.to_sym].evaluate!
    Loader.new(definition, File.open(path)).load!
  end

  def self.dump?(name)
    @dump_definition_builders.key?(name.to_sym)
  end

  def self.storage=(value)
    @storage = value
  end

  def self.database_config=(value)
    @database_config = value
  end

  def self.database_config
    @database_config
  end

  def self.clear_dumps!
    @dump_definition_builders = {}
  end
end
