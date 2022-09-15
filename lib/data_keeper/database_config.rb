module DataKeeper
  module DatabaseConfig
    def database_connection_config
      @database_connection_config ||= DataKeeper.database_config.call
    end

    def psql_env
      env = { 'PGUSER' => username }
      env['PGPASSWORD'] = password if password
      env
    end

    def dumper_psql_env
      env = { 'PGUSER' => server_username }
      env['PGPASSWORD'] = server_password if server_password
      env
    end

    def docker_env_params
      psql_env.map do |k, v|
        "-e #{k}=#{v}"
      end.join(" ")
    end

    def server_username
      database_connection_config['username']
    end

    def server_password
      database_connection_config['password']
    end

    def username
      DataKeeper.docker_config[:pg_user] || database_connection_config['username']
    end

    def password
      DataKeeper.docker_config[:pg_password] || database_connection_config['password']
    end

    def host
      DataKeeper.docker_config[:pg_host] || database_connection_config['host'] || '127.0.0.1'
    end

    def database
      database_connection_config['database']
    end

    def port
      DataKeeper.docker_config[:pg_port] || database_connection_config['port'] || '5432'
    end

    def server_port
      database_connection_config['port'] || '5432'
    end

    def connection_args
      connection_opts = '--host=:host'
      connection_opts += ' --port=:port' if port
      connection_opts
    end

    def dumper_connection_args
      connection_opts = '--host=:host'
      connection_opts += ' --port=:port' if server_port
      connection_opts
    end
  end
end