namespace :data_keeper do
  task :kill do
    raise "Cannot be run in production!" if Rails.env.production?

    DataKeeper::DatabaseHelper.kill
  end

  desc "Fetches and loads the given dump in your local database. WARN: Will remove all your current data."
  task :pull, [:name] => [:kill, "db:drop", "db:create"] do |_t, args|
    raise "NOT IN PRODUCTION" if Rails.env.production?

    name = args[:name]

    if name.blank? || !DataKeeper.dump?(name)
      raise "Please use this rake task giving a name of a configured dump. Ex: bin/rake data_keeper:pull[full]"
    end

    DataKeeper.fetch_and_load_dump!(name)
  end

  desc "Loads the given dump (found on the given local path) and applies it to your local database. WARN: Will remove all your current data."
  task :load, [:name, :path] => [:kill, "db:drop", "db:create"] do |_t, args|
    raise "NOT IN PRODUCTION" if Rails.env.production?

    name = args[:name]
    path = args[:path]

    if name.blank? || !DataKeeper.dump?(name)
      raise "Please use this rake task giving a name of a configured dump."
    end

    unless File.file?(path)
      raise "The given file '#{path}' does not exist."
    end

    DataKeeper.load_dump!(name, path)
  end

  desc "Downloads the given dump raw file in the current directory"
  task :download, [:name] => [:environment] do |_t, args|
    name = args[:name]

    if name.blank? || !DataKeeper.dump?(name)
      raise "Please use this rake task giving a name of a configured dump. Ex: bin/rake data_keeper:pull[full]"
    end

    DataKeeper.download_dump!(name, ".")
  end
end