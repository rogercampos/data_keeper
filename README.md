# DataKeeper

In a rails app using postgresql, DataKeeper is a tool to create dumps of your database in production to be used later on for local development. 

It automates the process of creating and storing them on the server, and applying them locally afterwards.

It supports full dumps, as well as partial dumps per specific tables or even specific rows (you provide a sql select). 
On partial dumps, note you'll need to manage possible issues around foreign keys and maybe other constraints. 

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'data_keeper'
```

And then execute:

    $ bundle install

Or install it yourself as:

    $ gem install data_keeper

## Usage

Configure the storage to use to save the generated dumps.

You can use a local storage, a simple option which stores the dumps in the same server running the code,
in a path of your choosing (consider that it must be writable by the user running this code in production).
You also configure how to reach that server from your local machine (currently only scp is supported), in
order to download these dumps later. Ex:

```ruby
DataKeeper.storage = DataKeeper::LocalStorage.new(
  local_store_dir: "/users/fredy/backups/...",
  remote_access: {
    type: "scp",
    host: "141.12.241.22",
    port: "8622",
    user: "fredy"
  }
)
```

Other storages, like S3, could be implemented, but currently this gem only ships with local storage.
If you want to do your own, you can assign as an storage whatever object that responds to:

- `#save(file, filename, dump_name)`, where file is a File object and filename a string. This method should save the given
  dump file. 

- `#retrieve(dump_name) { |file| (...) }`, which should retrieve the latest stored dump with the given dump_name.
  It should yield the given block passing the File object pointing to the retrieved dump file in the local filesystem,
  which is expected to be cleaned up on block termination.


Then, declare some dumps to work with:

```ruby
# Dump the whole database
DataKeeper.define_dump(:whole_database, :full)

# Dump only selected tables, and a custom SQL
DataKeeper.define_dump(:config) do |d|
  # Specific tables, all rows
  d.table "products"
  d.table "traits"

  # Only some rows in the "vouchers" table. MAKE SURE your sql returns only columns from the target table!
  d.sql(:vouchers, :used_vouchers) { Voucher.joins(cart: :order).where(orders: {status: "sent"}).to_sql }
  
  # Possible additional code to run after applying the dump locally
  d.on_after_load do
    User.create! email: "test@gmail.com", password: "password"
  end
end
```

Now, in production, you'll have run `DataKeeper.create_dump!("config")`, passing in the same of the dump
you defined before. Running this will create the dump file, from the server you run this code from,
and store it in the configured storage.

If you want to have always an up-to-date dump, you'll need to call this periodically, for example once per day.

Finally, to apply the dump locally, you can use the rake task:

`bin/rake data_keeper:pull[config]`

This will download the latest version available of the "config" dump, and apply it locally, destroying anything
in your current database. It will give you an error if you try to run this in a production environment.

Note when using raw sql, your statement is expected to return all columns for the configured table, in the default
order (`select *`). This uses pg's COPY from/to for the full table internally. 

## Development

After checking out the repo, run `bin/setup` to install dependencies. Then, run `rake test` to run the tests. You can also run `bin/console` for an interactive prompt that will allow you to experiment.

To install this gem onto your local machine, run `bundle exec rake install`. To release a new version, update the version number in `version.rb`, and then run `bundle exec rake release`, which will create a git tag for the version, push git commits and tags, and push the `.gem` file to [rubygems.org](https://rubygems.org).

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/[USERNAME]/data_keeper.


## License

The gem is available as open source under the terms of the [MIT License](https://opensource.org/licenses/MIT).


