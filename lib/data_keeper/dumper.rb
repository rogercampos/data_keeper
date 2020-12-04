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
        raise "Pending."
        # dump_partial_database(&block)
      end
    end

    private

    def dump_partial_database
      Tempfile.create do |file|
        Zlib::GzipWriter.wrap(file) do |gzip|
          Gem::Package::TarWriter.new(gzip) do |tar|

            Tempfile.create do |tables_dump_file|
              table_args = @definition.full_tables_to_export.map { |table| "-t #{table}" }.join(' ')
              cmd = Terrapin::CommandLine.new(
                'pg_dump',
                "#{connection_args} -x -Fc :database #{table_args} > :output_path",
                environment: psql_env
              )

              cmd.run(
                database: database,
                host: host,
                port: port,
                output_path: tables_dump_file.path
              )

              tar.add_file_simple("tables.gz", 0644, File.size(tables_dump_file.path)) do |io|
                tables_dump_file.reopen(tables_dump_file)

                while !tables_dump_file.eof?
                  io.write(tables_dump_file.read(2048))
                end
              end
            end

            @definition.sqls.each do |name, (_table, sql)|
              Tempfile.create do |table_file|
                cmd = Terrapin::CommandLine.new('psql', "#{connection_args} -d :database -c \"COPY (#{sql.call}) to STDOUT DELIMITER ',' CSV HEADER\" > #{table_file.path}", environment: psql_env)
                cmd.run(
                  database: database,
                  host: host,
                  port: port
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
        end

        yield file, "#{filename}.tar.gz"
      end
    end

    def dump_full_database
      Tempfile.create do |file|
        cmd = Terrapin::CommandLine.new(
          'pg_dump',
          "#{connection_args} -x -Fc :database > :output_path",
          environment: psql_env
        )

        cmd.run(
          database: database,
          host: host,
          port: port,
          output_path: file.path
        )

        yield file, "#{filename}.gz"
      end
    end

    def filename
      "#{@dump_name}-#{Time.now.strftime("%Y%m%d-%H%M")}"
    end
  end
end