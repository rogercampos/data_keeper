require 'data_keeper/database_config'

module DataKeeper
  class Loader
    include DatabaseConfig

    def initialize(dump, file)
      @dump = dump
      @file = file
      @psql_version = Terrapin::CommandLine.new('psql', '--version').run
                                           .match(/[0-9]{1,}\.[0-9]{1,}/)
                                           .to_s.to_f
    end

    def load!
      if @dump.type == :full
        load_full_database!
      else
        load_partial_database!
      end

      if @dump.on_after_load_block
        @dump.on_after_load_block.call
      end
    end

    private

    def load_full_database!
      ensure_schema_compatibility!

      pg_restore = Terrapin::CommandLine.new(
        'pg_restore',
        "#{connection_args} -j 4 --no-owner --dbname #{database} #{@file.path} 2>/dev/null",
        environment: psql_env
      )

      pg_restore.run(
        database: database,
        host: host,
        port: port
      )

      cmd = Terrapin::CommandLine.new(
        'psql',
        "#{connection_args} -d :database -c :sql",
        environment: psql_env
      )

      cmd.run(
        database: database,
        host: host,
        port: port,
        sql: "UPDATE ar_internal_metadata SET value = 'development'"
      )
    end

    def load_partial_database!
      inflate(@file.path) do |schema_path, tables_path, sql_files|
        ensure_schema_compatibility!

        pg_restore = Terrapin::CommandLine.new(
          'pg_restore',
          "#{connection_args} -j 4 --no-owner --dbname :database #{schema_path} 2>/dev/null",
          environment: psql_env
        )

        pg_restore.run(
          database: database,
          host: host,
          port: port
        )

        pg_restore = Terrapin::CommandLine.new(
          'pg_restore',
          "#{connection_args} --data-only -j 4 --no-owner --disable-triggers --dbname :database #{tables_path} 2>/dev/null",
          environment: psql_env
        )

        pg_restore.run(
          database: database,
          host: host,
          port: port
        )

        sql_files.each do |table, csv_path|
          cmd = Terrapin::CommandLine.new(
            'psql',
            "#{connection_args} -d :database -c :command < :csv_path",
            environment: psql_env
          )

          cmd.run(
            database: database,
            host: host,
            port: port,
            csv_path: csv_path,
            command: "ALTER TABLE #{table} DISABLE TRIGGER all; COPY #{table} FROM stdin DELIMITER ',' CSV HEADER"
          )
        end

        Rake::Task['db:environment:set'].invoke
      end
    end

    def ensure_schema_compatibility!
      cmd = Terrapin::CommandLine.new(
        'psql',
        "#{connection_args} -d :database -c :command",
        environment: psql_env
      )

      if @psql_version >= 11.0
        cmd.run(database: database, host: host, port: port, command: "drop schema if exists public")
      else
        cmd.run(database: database, host: host, port: port, command: "create schema if not exists public")
      end
    end

    class InflatedFiles
      attr_reader :errors

      def initialize(dump, paths)
        @dump = dump
        @paths = paths
        @errors = []
      end

      def valid?
        @errors = []

        validate("Schema file is missing") { !!schema_path } &&
          validate("Tables file is missing") { !!tables_path } &&
          validate("Not all sql custom dumps are present") do
            sql_dumps.size == @dump.sqls.keys.size
          end
      end

      def schema_path
        @schema_path ||= @paths.find { |x| File.basename(x) == "schema.dump" }
      end

      def tables_path
        @tables_path ||= @paths.find { |x| File.basename(x) == "tables.dump" }
      end

      def sql_dumps
        @sql_dumps ||= @dump.sqls.map do |name, (table, _proc)|
          path = @paths.find { |x| File.basename(x) == "#{name}.csv" }
          next unless path

          [table, path]
        end.compact
      end

      private

      def validate(error_message)
        result = yield
        @errors << error_message unless result
        result
      end
    end

    def inflate(path)
      Dir.mktmpdir do |dir|
        File.open(path, "rb") do |f|
          Gem::Package.new("").extract_tar_gz(f, dir)

          inflated_files = InflatedFiles.new(@dump, Dir.glob(File.join(dir, "*")))
          raise inflated_files.errors.join(", ") unless inflated_files.valid?

          yield(
            inflated_files.schema_path,
            inflated_files.tables_path,
            inflated_files.sql_dumps
          )
        end
      end
    end
  end
end