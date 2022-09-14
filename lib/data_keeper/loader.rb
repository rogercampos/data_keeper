require 'data_keeper/database_config'

module DataKeeper
  class Loader
    include DatabaseConfig

    def initialize(dump, file)
      @dump = dump
      @file = file
      @psql_version = build_terrapin_command('psql', '--version').run
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
        ActiveRecord::Base.establish_connection
        @dump.on_after_load_block.call
      end
    end

    private

    def log_redirect
      if Terrapin::CommandLine.logger
        ""
      else
        "  2>/dev/null"
      end
    end

    def build_terrapin_command(binary, args, docker_args = args)
      if DataKeeper.docker_config.any?
        Terrapin::CommandLine.new(
          'docker',
          "exec #{docker_env_params} -i #{DataKeeper.docker_config[:instance_name]} #{binary} #{docker_args}"
        )
      else
        Terrapin::CommandLine.new(
          binary,
          args,
          environment: psql_env
        )
      end
    end

    def set_ar_internal_metadata!
      cmd = build_terrapin_command("psql", "#{connection_args} -d :database -c :sql")

      cmd.run(
        database: database,
        host: host,
        port: port,
        sql: "DELETE from ar_internal_metadata"
      )

      cmd.run(
        database: database,
        host: host,
        port: port,
        sql: "INSERT into ar_internal_metadata (key, value, created_at, updated_at) VALUES ('environment', 'development', '2020-04-03 12:25:54.094209', '2020-04-03 12:25:54.094209')"
      )
    end

    def load_full_database!
      pg_restore = build_terrapin_command(
        "pg_restore",
        "#{connection_args} -j 4 --no-owner --dbname #{database} #{@file.path}#{log_redirect}",
        "#{connection_args} --no-owner --dbname #{database} < #{@file.path}#{log_redirect}"
      )

      pg_restore.run(
        database: database,
        host: host,
        port: port
      )

      set_ar_internal_metadata!
    end

    def load_partial_database!
      ensure_schema_compatibility!

      inflate(@file.path) do |schema_path, tables_path, sql_files, sequences_path|
        pg_restore = build_terrapin_command(
          "pg_restore",
          "#{connection_args} -j 4 --no-owner -s --dbname :database #{schema_path}#{log_redirect}",
          "#{connection_args} --no-owner -s --dbname :database < #{schema_path}#{log_redirect}"
        )

        pg_restore.run(
          database: database,
          host: host,
          port: port
        )

        pg_restore = build_terrapin_command(
          "pg_restore",
          "#{connection_args} --data-only -j 4 --no-owner --disable-triggers --dbname :database #{tables_path}#{log_redirect}",
          "#{connection_args} --data-only --no-owner --disable-triggers --dbname :database < #{tables_path}#{log_redirect}"
        )

        pg_restore.run(
          database: database,
          host: host,
          port: port
        )

        sql_files.each do |table, csv_path|
          cmd = build_terrapin_command("psql", "#{connection_args} -d :database -c :command < :csv_path")

          cmd.run(
            database: database,
            host: host,
            port: port,
            csv_path: csv_path,
            command: "ALTER TABLE #{table} DISABLE TRIGGER all; COPY #{table} FROM stdin DELIMITER ',' CSV HEADER"
          )
        end

        pg_restore = build_terrapin_command(
          "pg_restore",
          "#{connection_args} --data-only -j 4 --no-owner --disable-triggers --dbname :database #{sequences_path}#{log_redirect}",
          "#{connection_args} --data-only --no-owner --disable-triggers --dbname :database < #{sequences_path}#{log_redirect}"
        )

        pg_restore.run(
          database: database,
          host: host,
          port: port
        )

        set_ar_internal_metadata!
      end
    end

    def ensure_schema_compatibility!
      cmd = build_terrapin_command("psql", "#{connection_args} -d :database -c :command")

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
          validate("Not all sql custom dumps are present") { sql_dumps.size == @dump.sqls.keys.size } &&
          validate("Sequences file is missing") { !!sequences_path }
      end

      def schema_path
        @schema_path ||= @paths.find { |x| File.basename(x) == "schema.dump" }
      end

      def tables_path
        @tables_path ||= @paths.find { |x| File.basename(x) == "tables.dump" }
      end

      def sequences_path
        @sequences_path ||= @paths.find { |x| File.basename(x) == "sequences.dump" }
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
              inflated_files.sql_dumps,
              inflated_files.sequences_path
          )
        end
      end
    end
  end
end