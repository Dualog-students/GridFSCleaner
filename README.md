# MongoDB GridFS orphaned chunks cleanup (MGOCC)

The MGOCC program searches trough every chunk in GridFS to determine whether a file and it's chunks are orphaned or not.

## Required stuff

Only three things are needed for this program to work:

* `MongoConnectionString` the connection string to your MongoDB cluster
* `DryRun` if you are an anxious individual you might want to see how many deletions would be performed
* Patience, a lot of patience.

## Legal stuff

GPL v3.0 so use this at your own risk.