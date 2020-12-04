- decouple from rails / ar

- implement partial loads. What about the structure? load from schema.rb first (db:schema:load or db:structure:apply),
  then load db.
  
- add anonymizing feature of certain columns

- skip download if present in cache. Clear cache.

- add s3 storage

- add option to apply dump but creating tables with "create table unlogged", can be useful for performance in tests
  or even locally
  