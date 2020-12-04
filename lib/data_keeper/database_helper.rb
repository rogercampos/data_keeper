module DataKeeper
  class DatabaseHelper
    include DatabaseConfig

    def kill
      cmd = Terrapin::CommandLine.new(
        'psql',
        "-c :command #{connection_args} --dbname #{database} &> /dev/null || true",
        environment: psql_env
      )

      cmd.run(
        database: database,
        host: host,
        port: port,
        command: "SELECT pid, pg_terminate_backend(pid) as terminated FROM pg_stat_activity WHERE pid <> pg_backend_pid();"
      )
    end

    def self.kill
      new.kill
    end
  end
end