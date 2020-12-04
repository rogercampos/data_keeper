module DataKeeper
  InvalidDumpType = Class.new(Error)
  InvalidDumpDefinition = Class.new(Error)

  class Definition
    attr_reader :type, :on_after_load_block

    def initialize(type, tables, sqls, on_after_load_block)
      @type = type
      @tables = tables
      @sqls = sqls
      @on_after_load_block = on_after_load_block
    end

    def full_tables
      @tables
    end

    def sqls
      @sqls
    end

    def full_tables_to_export
      full_tables + ['schema_migrations']
    end
  end

  class DefinitionBuilder
    attr_reader :tables, :raw_sqls, :on_after_load_block

    def initialize(definition_block)
      @tables = []
      @raw_sqls = {}
      instance_eval(&definition_block) if definition_block
    end

    def self.build(type, block)
      @type = type
      raise InvalidDumpType, "Invalid type! use :partial or :full" unless [:partial, :full].include?(type)

      builder = new(block)

      Definition.new(type, builder.tables, builder.raw_sqls, builder.on_after_load_block)
    end

    def table(name)
      raise InvalidDumpDefinition if @type == :full
      raise(InvalidDumpDefinition, "table already defined") if @tables.include?(name)
      @tables << name
    end

    def sql(table, name, &block)
      raise InvalidDumpDefinition if @type == :full
      raise(InvalidDumpDefinition, "sql already defined") if @raw_sqls.key?(name)
      @raw_sqls[name] = [table, block]
    end

    def on_after_load(&block)
      @on_after_load_block = block
    end
  end
end