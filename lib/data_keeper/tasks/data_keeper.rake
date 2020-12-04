namespace :data_keeper do
  task :kill do
    raise "Cannot be run in production!" if Rails.env.production?

    DataKeeper::DatabaseHelper.kill
  end

  task :pull, [:name] => [:kill, "db:drop", "db:create"] do |t, args|
    name = args[:name]

    if name.blank?
      raise "Please use this rake task giving a name of a configured dump. Ex: bin/rake data_keeper:pull[full]"
    end

    DataKeeper.load_dump!(args[:name])
  end
end