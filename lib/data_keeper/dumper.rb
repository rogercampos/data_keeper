require 'data_keeper/database_config'
require 'tempfile'

module DataKeeper
  class Dumper
    include DatabaseConfig

    def initialize(name, definition)
      @dump_name = name
      @definition = definition
    end

    def run!(&block)
      if @definition.type == :full
        dump_full_database(&block)
      else
        dump_partial_database(&block)
      end
    end

    private

    def dump_full_database
      Tempfile.create do |file|
        cmd = Terrapin::CommandLine.new(
          'pg_dump',
          "#{dumper_connection_args} -x -Fc :database > :output_path",
          environment: dumper_psql_env
        )

        cmd.run(
          database: database,
          host: host,
          port: port,
          output_path: file.path
        )

        yield File.open(file.path), "#{filename}.dump"
      end
    end

    def dump_partial_database
      Tempfile.create do |file|
        file.binmode

        Zlib::GzipWriter.wrap(file) do |gzip|
          Gem::Package::TarWriter.new(gzip) do |tar|
            dump_schema(tar)
            dump_partial_tables(tar)
            dump_sqls(tar)
            dump_sequences(tar)
          end
        end

        yield File.open(file.path), "#{filename}.tar.gz"
      end
    end

    def dump_sqls(tar)
      @definition.sqls.each do |name, (_table, sql)|
        Tempfile.create do |table_file|
          cmd = Terrapin::CommandLine.new(
            'psql',
            "#{dumper_connection_args} -d :database -c :command > #{table_file.path}",
            environment: dumper_psql_env
          )

          cmd.run(
            database: database,
            host: host,
            port: port,
            command: "COPY (#{sql.call}) to STDOUT DELIMITER ',' CSV HEADER"
          )

          tar.add_file_simple("#{name}.csv", 0644, File.size(table_file.path)) do |io|
            table_file.reopen(table_file)

            while !table_file.eof?
              io.write(table_file.read(2048))
            end
          end
        end
      end
    end

    def dump_partial_tables(tar)
      Tempfile.create do |tables_dump_file|
        tables_dump_file.binmode
        table_args = @definition.full_tables_to_export.map { |table| "-t #{table}" }.join(' ')
        cmd = Terrapin::CommandLine.new(
          'pg_dump',
          "#{dumper_connection_args} -x -Fc :database #{table_args} > :output_path",
          environment: dumper_psql_env
        )

        cmd.run(
          database: database,
          host: host,
          port: port,
          output_path: tables_dump_file.path
        )

        tar.add_file_simple("tables.dump", 0644, File.size(tables_dump_file.path)) do |io|
          tables_dump_file.reopen(tables_dump_file)

          while !tables_dump_file.eof?
            io.write(tables_dump_file.read(2048))
          end
        end
      end
    end

    def dump_schema(tar)
      Tempfile.create do |schema_dump_file|
        schema_dump_file.binmode

        cmd = Terrapin::CommandLine.new(
          'pg_dump',
          "#{dumper_connection_args} -x --schema-only -Fc :database > :output_path",
          environment: dumper_psql_env
        )

        cmd.run(
          database: database,
          host: host,
          port: port,
          output_path: schema_dump_file.path
        )

        tar.add_file_simple("schema.dump", 0644, File.size(schema_dump_file.path)) do |io|
          schema_dump_file.reopen(schema_dump_file)

          while !schema_dump_file.eof?
            io.write(schema_dump_file.read(2048))
          end
        end
      end
    end

    def dump_sequences(tar)
      Tempfile.create do |sequences_dump_file|
        sequences_dump_file.binmode

        sequences_args = all_sequences_to_export.map { |table| "-t #{table}" }.join(' ')
        cmd = Terrapin::CommandLine.new(
          'pg_dump',
          "#{dumper_connection_args} -x -Fc :database #{sequences_args} > :output_path",
          environment: dumper_psql_env
        )

        cmd.run(database: database, host: host, port: port, output_path: sequences_dump_file.path)

        tar.add_file_simple("sequences.dump", 0644, File.size(sequences_dump_file.path)) do |io|
          sequences_dump_file.reopen(sequences_dump_file)

          while !sequences_dump_file.eof?
            io.write(sequences_dump_file.read(2048))
          end
        end
      end
    end

    def filename
      "#{@dump_name}-#{Time.now.strftime("%Y%m%d-%H%M")}"
    end

    def all_sequences_to_export
      cmd = Terrapin::CommandLine.new(
        'psql',
        "#{dumper_connection_args} -d :database -c :sql -A -R ',' -t",
        environment: dumper_psql_env
      )

      sequences = cmd.run(
        database: database,
        host: host,
        port: port,
        sql: "SELECT sequencename from pg_sequences;"
      )
      sequences.split(',').map{|x| x.strip}
    end
  end
end