module DataKeeper
  module DatabaseConfig
    def database_connection_config
      Rails.configuration.database_configuration[Rails.env]
    end

    def psql_env
      env = { 'PGUSER' => database_connection_config['username'] }
      env['PGPASSWORD'] = database_connection_config['password'] if database_connection_config['password']
      env
    end

    def host
      database_connection_config['host'] || '127.0.0.1'
    end

    def database
      database_connection_config['database']
    end

    def port
      database_connection_config['port']
    end

    def connection_args
      connection_opts = '--host=:host'
      connection_opts += ' --port=:port' if database_connection_config['port']
      connection_opts
    end
  end
end