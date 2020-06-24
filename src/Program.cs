using MongoDB.Bson;
using MongoDB.Driver;
using Serilog;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace GridFSCleaner
{
    internal class Program
    {
        private static async Task<int> Main()
        {
            Log.Logger = new LoggerConfiguration()
             .MinimumLevel.Debug()
             .WriteTo.Console()
             //.WriteTo.File("mongo.txt")
             .CreateLogger();

            var envMongoConnectionString = Environment.GetEnvironmentVariable("MongoConnectionString");
            if (string.IsNullOrEmpty(envMongoConnectionString))
            {
                Log.Error("Please provide the environment variable 'MongoConnectionString'");
                return 1;
            }

            var envIsDryrun = Environment.GetEnvironmentVariable("DryRun") ?? "true";
            if (!bool.TryParse(envIsDryrun, out var isDryRun))
            {
                Log.Error("Please provide the environment variable 'DryRun'");
                return 1;
            }

            var mongoSettings = MongoClientSettings.FromConnectionString(envMongoConnectionString);
            var mongoClient = new MongoClient(mongoSettings);
            var mongoDatabase = mongoClient.GetDatabase("dr-move-public-api");
            var chunks = mongoDatabase.GetCollection<BsonDocument>("packages.chunks");
            var files = mongoDatabase.GetCollection<BsonDocument>("packages.files");

            using var cts = new CancellationTokenSource();
            Console.CancelKeyPress += (sender, e) =>
            {
                e.Cancel = true;
                cts.Cancel();
            };

            Log.Information("Starting.. IsDryRun: {0}. Connection information: {@1}", isDryRun, mongoSettings);

            var sw = new Stopwatch();
            var filesDeleted = 0;
            var orphanedChunks = 0UL;
            var validFiles = new HashSet<ObjectId>();
            var filesToDelete = new HashSet<ObjectId>();

            // The projection and hint let us do this whole thing as a covered query
            // https://docs.mongodb.com/manual/core/query-optimization/#covered-query
            var findOptions = new FindOptions<BsonDocument>()
            {
                Projection = Builders<BsonDocument>.Projection.Include("files_id").Exclude("_id"),
                Hint = "files_id_1_n_1"
            };

            try
            {
                Log.Information("Creating cursor and starting search of orphaned chunks..");
                sw.Start();

                using var chunksCursor = await chunks.FindAsync(Builders<BsonDocument>.Filter.Empty, findOptions);
                while (await chunksCursor.MoveNextAsync())
                {
                    orphanedChunks += (ulong)chunksCursor.Current.Count();
                    var uniqueFileIds = chunksCursor.Current.Select(x => x["files_id"].AsObjectId).ToHashSet();

                    foreach (var fileId in uniqueFileIds)
                    {
                        // Don't check it if we already know it's valid
                        // Don't check if we already deleted
                        if (validFiles.Contains(fileId) || filesToDelete.Contains(fileId))
                        {
                            continue;
                        }

                        var fileCount = await files.CountDocumentsAsync(Builders<BsonDocument>.Filter.Eq("_id", fileId));
                        if (fileCount > 0)
                        {
                            // If the file exists, add it to our list and move on
                            validFiles.Add(fileId);
                            continue;
                        }

                        filesToDelete.Add(fileId);
                    }
                }

                Log.Information("Found '{0}' files and {1} orphaned chunks.", filesToDelete.Count, orphanedChunks);
                Log.Information("Elapsed time: {0}", sw.Elapsed.ToString());
                Log.Information("Starting deletion process..");

                // Do this outside the cursor loop to avoid cursor timeout
                foreach (var files_id in filesToDelete)
                {
                    if (isDryRun)
                    {
                        var deleteResult = await chunks.CountDocumentsAsync(Builders<BsonDocument>.Filter.Eq("files_id", files_id));
                        Log.Information("Dry-run: Could delete {0} chunks from orphaned file {@1}", deleteResult, new { Id = files_id.ToString(), files_id.CreationTime });
                        filesDeleted++;
                    }
                    else
                    {
                        // If we know it isn't valid, delete all possible chunks with a single command
                        var deleteResult = await chunks.DeleteManyAsync(Builders<BsonDocument>.Filter.Eq("files_id", files_id));
                        Log.Information("Deleted {0} chunks from orphaned file {@1}", deleteResult.DeletedCount, new { Id = files_id.ToString(), files_id.CreationTime });
                        filesDeleted++;
                    }
                }
            }
            catch (Exception ex)
            {
                Log.Error(ex, "Deletion process stopped. Something went wrong.");
            }
            finally
            {
                sw.Stop();
                Log.Information("Deletion process finished.");
                Log.Information("Elapsed time: {0}.", sw.Elapsed.ToString());
                Log.Information("Deleted '{0}' files", filesDeleted);
                Log.CloseAndFlush();
            }

            return 0;
        }
    }
}
