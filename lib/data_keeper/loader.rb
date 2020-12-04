require 'data_keeper/database_config'

module DataKeeper
  class Loader
    include DatabaseConfig

    def initialize(dump, file)
      @dump = dump
      @file = file
    end

    def load!
      if @dump.type == :full
        load_full_database!
      else
        raise "Pending."
        # load_partial_database!
      end

      if @dump.on_after_load_block
        @dump.on_after_load_block.call
      end
    end

    private

    def load_full_database!
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
  end
end