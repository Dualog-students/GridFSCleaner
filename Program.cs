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
             .WriteTo.File("mongo.txt")
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
            var validFiles = new HashSet<ObjectId>();
            var deletedFiles = new HashSet<ObjectId>();

            // The projection and hint let us do this whole thing as a covered query
            // https://docs.mongodb.com/manual/core/query-optimization/#covered-query
            var findOptions = new FindOptions<BsonDocument>()
            {
                Projection = Builders<BsonDocument>.Projection.Include("files_id").Exclude("_id"),
                Hint = "files_id_1_n_1"
            };

            // Just to see that the program is working as this can take a while.
            var timer = new Timer((s) => Log.Information("Valid files count: {0}", validFiles.Count), null, 0, 10_000);

            try
            {
                Log.Information("Creating cursor and starting search of orphaned chunks..");
                sw.Start();

                using var chunksCursor = await chunks.FindAsync(Builders<BsonDocument>.Filter.Empty, findOptions);
                while (await chunksCursor.MoveNextAsync())
                {
                    var uniqueFileIds = chunksCursor.Current.Select(x => x["files_id"].AsObjectId).ToHashSet();
                    foreach (var fileId in uniqueFileIds)
                    {
                        // Don't check it if we already know it's valid
                        // Don't check if we already deleted
                        if (validFiles.Contains(fileId) || deletedFiles.Contains(fileId))
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

                        var deleteResult = 0L;
                        // If we know it isn't valid, delete all possible chunks with a single command
                        if (isDryRun)
                        {
                            deleteResult = await chunks.CountDocumentsAsync(Builders<BsonDocument>.Filter.Eq("files_id", fileId));
                            Log.Information("Dry-run: Could delete {0} chunks from orphaned file {@1}", deleteResult, new { Id = fileId.ToString(), fileId.CreationTime });
                        }
                        else
                        {
                            var deleteResults = await chunks.DeleteManyAsync(Builders<BsonDocument>.Filter.Eq("files_id", fileId));
                            deleteResult = deleteResults.DeletedCount;
                            Log.Information("Deleted {0} chunks from orphaned file {@1}", deleteResult, new { Id = fileId.ToString(), fileId.CreationTime });
                        }

                        deletedFiles.Add(fileId);
                    }
                }
            }
            catch (Exception ex)
            {
                Log.Error(ex, "Something went wrong.");
            }
            finally
            {
                sw.Stop();
                Log.Information("Elapsed time: {0} minutes.", sw.Elapsed.TotalMinutes);
                Log.Information("Deleted '{0}' files: {1}", deletedFiles.Count, string.Join(",", deletedFiles.Select(x => x.ToString())));
                Log.CloseAndFlush();
            }

            return 0;
        }
    }
}
