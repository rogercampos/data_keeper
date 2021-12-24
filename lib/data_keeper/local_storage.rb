require 'sshkit'
require 'tempfile'
require 'fileutils'

module DataKeeper
  class LocalStorage
    include SSHKit::DSL

    def initialize(data)
      @local_store_dir = data[:local_store_dir]
      @remote_access = data[:remote_access]
    end

    def save(file, filename, dump_name)
      path = dump_path(dump_name, filename)
      FileUtils.mkdir_p(File.dirname(path))

      FileUtils.cp(file.path, path)
    end

    def retrieve(dump_name)
      tempfile = Tempfile.new
      local_store_dir = @local_store_dir
      last_dump_filename = nil

      on complete_host do
        last_dump_filename = capture :ls, "-1t #{File.join(local_store_dir, dump_name.to_s)} | head -n 1"

        download! File.join(local_store_dir, dump_name.to_s, last_dump_filename), tempfile.path
      end

      yield(tempfile, last_dump_filename)
    ensure
      tempfile.delete
    end

    private

    def complete_host
      "#{@remote_access[:user]}@#{@remote_access[:host]}:#{@remote_access[:port]}"
    end

    def dump_path(dump_name, filename)
      File.join(@local_store_dir, dump_name.to_s, filename)
    end
  end
end