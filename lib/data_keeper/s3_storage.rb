begin
  require 'aws-sdk-s3'
rescue LoadError
  raise "You must include the 'aws-sdk-s3' gem in your Gemfile in order to use this s3 storage."
end

module DataKeeper
  class S3Storage
    class Client
      NoSuchKey = Class.new(StandardError)

      def initialize(client_options:, bucket: nil)
        @client_options = client_options
        @client = Aws::S3::Client.new(client_options)
        @bucket = bucket
      end

      def delete_files(file_paths)
        @client.delete_objects(
          bucket: @bucket,
          delete: {
            objects: file_paths.map { |key| { key: key } }
          }
        )
      end

      def list_contents(prefix = '')
        @client.list_objects(bucket: @bucket, prefix: prefix).contents
      rescue Aws::S3::Errors::NoSuchKey
        raise NoSuchKey, prefix
      end

      # Streams all contents from `path` into the provided io object, calling #write to it.
      # io can be a File, or any other IO-like object.
      def stream_to_io(path, io, opts = {})
        @client.get_object(opts.merge(
          bucket: @bucket,
          key: path
        ), target: io)
      rescue Aws::S3::Errors::NoSuchKey
        raise NoSuchKey, path
      end

      # Uploads the given file into the target_path in the s3 bucket.
      # `file` must be a file stored locally. Can be either a raw string (path),
      # or a File/Tempfile object (close is up to you).
      def put_file(target_path, file, options = {})
        file.rewind if file.respond_to?(:rewind)

        s3 = Aws::S3::Resource.new(@client_options)
        obj = s3.bucket(@bucket).object(target_path)
        obj.upload_file(file, options)
      end
    end

    def initialize(bucket:, store_dir:, remote_access:, acl: "public-read", keep_amount: 3)
      @bucket = bucket
      @store_dir = store_dir
      @remote_access = remote_access
      @acl = acl
      @keep_amount = keep_amount
    end

    def save(file, filename, dump_name)
      path = dump_path(dump_name, filename)

      s3_client.put_file(path, file, acl: @acl)

      prefix = "#{@store_dir}#{dump_name.to_s}"

      keys_to_delete = s3_client.list_contents(prefix).sort_by(&:last_modified).reverse[@keep_amount..-1]

      return unless keys_to_delete

      s3_client.delete_files(keys_to_delete.map(&:key))

      true
    end

    def retrieve(dump_name)
      prefix = "#{@store_dir}#{dump_name.to_s}"
      last_dump = s3_client.list_contents(prefix).sort_by(&:last_modified).reverse.first

      Tempfile.create do |tmp_file|
        tmp_file.binmode
        s3_client.stream_to_io(last_dump.key, tmp_file)
        tmp_file.flush

        yield(tmp_file)
      end
    end

    private

    def s3_client
      @s3_client ||= Client.new(bucket: @bucket, client_options: @remote_access)
    end

    def dump_path(dump_name, filename)
      File.join(@store_dir, dump_name.to_s, "#{SecureRandom.alphanumeric(40)}-#{filename}")
    end
  end
end