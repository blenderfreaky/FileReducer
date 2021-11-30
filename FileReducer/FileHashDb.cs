namespace FileReducer;

using LiteDB;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Text;

public class FileHashDb : IDisposable
{
    private bool disposedValue;

    public LiteDatabase Database { get; }
    public ILiteCollection<DataHash> Hashes { get; }

    public ArrayPool<byte> Pool { get; } = ArrayPool<byte>.Shared;
    public SemaphoreSlim SemaphoreSlim { get; }

    public FileHashDb(int maxJobs = 32, string cacheFile = "Cache.db")
    {
        Database = new(cacheFile);
        SemaphoreSlim = new(maxJobs, maxJobs);
        Hashes = Database.GetCollection<DataHash>("hashes");
        //Hashes.EnsureIndex("Target", x => new { x.Path, x.SegmentLength }, true);
        BsonMapper.Global.RegisterType
        (
            serialize: obj => new BsonValue(obj.Data),
            deserialize: doc => new Hash(doc.AsBinary)
        );
    }

    private async Task<Action> Synchronization(FileSystemInfo info, CancellationToken cancellationToken = default)
    {
        await SemaphoreSlim.WaitAsync(cancellationToken);
        return () => SemaphoreSlim.Release();
    }

    private DataHash? Cache(FileSystemInfo info, int segmentLength)
    {
        try
        {
            using var _ = Profiler.MeasureStatic("Caching");
            var fileInfo = info as FileInfo;
            var isDirectory = fileInfo == null;
            var result = Hashes.Query()
                .Where(x => x.Path == info.FullName && x.SegmentLength == segmentLength && x.LastWriteUtc >= info.LastWriteTimeUtc)
                .OrderBy(x => x.LastWriteUtc)
                .FirstOrDefault();
            if (result == default) return null;
            if (result.IsDirectory != isDirectory) return null; // Putting this into LiteDB query crashes everytime
            if (!isDirectory && fileInfo!.Length != result.DataLength) return null;
            return result;
        }
        catch (LiteException ex)
        {
            Console.WriteLine(info);
            Console.WriteLine(ex.ToString());
            return null;
        }
    }

    private record struct HashPass(FileHashDb FileHashDb, int SegmentLength, bool CacheFiles, Func<FileSystemInfo, bool> ShouldHash, Action<DataHash, TimeSpan?> OnHashed) : IHasher
    {
        public ArrayPool<byte> ArrayPool { get; } = ArrayPool<byte>.Shared;

        public DataHash? Cache(FileSystemInfo info) => FileHashDb.Cache(info, SegmentLength);

        void IHasher.OnHashed(DataHash dataHash, TimeSpan? duration) => OnHashed(dataHash, duration);

        public Task<Action> Synchronization(FileSystemInfo info, CancellationToken cancellationToken = default) => FileHashDb.Synchronization(info, cancellationToken);

        bool IHasher.ShouldHash(FileSystemInfo info) => ShouldHash(info);
    }

    public async Task<DataHash> HashFileSystemInfo(FileSystemInfo info, int segmentLength = 8192, CancellationToken cancellationToken = default)
    {
        Ignore.Ignore ignore = new();
        var path = Path.Combine(info is DirectoryInfo directory ? directory.FullName : Path.GetDirectoryName(info.FullName)!, ".dupeignore");
        if (File.Exists(path)) ignore.Add(File.ReadAllLines(path));
        var hash = await DataHash.FromFileSystemInfoAsync(info, new HashPass(this, segmentLength, segmentLength == 0, info => !ignore.IsIgnored(info.FullName), (hash, time) =>
        {
            //if (!Hashes.Update(hash)) Hashes.Insert(hash); // Upsert no worky
            Hashes.Upsert(hash);
            Console.WriteLine($"Finished hashing ({time ?? TimeSpan.Zero}) {hash.Path}");
        }), cancellationToken);
        return hash;
    }

    public async Task ListDuplicates(CancellationToken cancellationToken = default)
    {
        var allDupes = Profiler.MeasureStatic("Duplicates.Query",
                () => Hashes.Query()
                .GroupBy("$.Hash").Select("ARRAY(*)").ToEnumerable()
                .Select<BsonDocument, ICollection<BsonValue>>(x => x.Values.First().AsArray)
                .Where(x => x.Count > 1)
                .Select(x => x.Select(x => BsonMapper.Global.ToObject<DataHash>(x.AsDocument)).ToList())
                .ToList()
            );

        var previousDupeCount = allDupes.Sum(x => x.Count);

        using var _ = Profiler.MeasureStatic("Duplicates.Verification");
        var steps = new int[] { 2, 4, 8, 16, 32, 0 };
        foreach (var step in steps)
        {
            using var __ = Profiler.MeasureStatic("Duplicates.Verification." + step);
            allDupes = (await VerifyDuplicates(allDupes, step * 8192, cancellationToken)).Select(x => x.ToList()).ToList();

            var falsePositives = allDupes.Sum(x => x.Count) - previousDupeCount;
            if (falsePositives > 0) Console.WriteLine("Removed " + falsePositives + " false positives");

            foreach (var dupes in allDupes)
            {
                Console.WriteLine($"Duplicates (Factor {step}): " + string.Join(", ", dupes.Select(x => x.Path)));
            }
        }
    }

    private async Task<IEnumerable<IEnumerable<DataHash>>> VerifyDuplicates(IEnumerable<IEnumerable<DataHash>> duplicateGroups, int segmentLength = 0, CancellationToken cancellationToken = default)
    {
        var result = duplicateGroups.Select(async duplicates =>
        {
            var hashTasks = duplicates.Select(async x =>
            {
                var info = DataHash.FolderOrFile(x.Path);
                if (info == null) return default;
                return await HashFileSystemInfo(info, 0, cancellationToken);
            })
                .Where(x => x != default).ToList();

            await Task.WhenAll(hashTasks.Select(x => x));
            var hashes = hashTasks.Select(x => x.Result).ToList();

            var collisions = hashes
                .GroupBy(x => (x.Hash, x.DataLength))
                .Select(x => x.ToList())
                .Where(x => x.Count > 1);

            return collisions;
        });

        await Task.WhenAll(result);
        return result.SelectMany(x => x.Result);
    }

    #region IDisposable
    protected virtual void Dispose(bool disposing)
    {
        if (!disposedValue)
        {
            if (disposing)
            {
                Database.Dispose();
                // TODO: dispose managed state (managed objects)
            }

            // TODO: free unmanaged resources (unmanaged objects) and override finalizer
            // TODO: set large fields to null
            disposedValue = true;
        }
    }

    // // TODO: override finalizer only if 'Dispose(bool disposing)' has code to free unmanaged resources
    // ~FileHashDb()
    // {
    //     // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
    //     Dispose(disposing: false);
    // }

    public void Dispose()
    {
        // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
        Dispose(disposing: true);
        GC.SuppressFinalize(this);
    }
    #endregion IDisposable
}