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
    public SemaphoreSlim SemaphoreSlim { get; } = new(32, 32);

    public FileHashDb()
    {
        Database = new("Cache.db");
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

    private record struct HashPass(FileHashDb FileHashDb, int SegmentLength, Func<FileSystemInfo, bool> ShouldHash, Action<DataHash, TimeSpan?> OnHashed) : IHasher
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
        var hash = await DataHash.FromFileSystemInfoAsync(info, new HashPass(this, segmentLength, info => !ignore.IsIgnored(info.FullName), (hash, time) =>
        {
            //if (!Hashes.Update(hash)) Hashes.Insert(hash); // Upsert no worky
            Hashes.Insert(hash);
            Console.WriteLine($"Finished hashing ({time ?? TimeSpan.Zero}) {hash.Path}");
        }), cancellationToken);
        return hash;
    }

    public async void ListDuplicates()
    {
        var allDupes = Hashes.Query()
            .GroupBy("$.Hash").Select("ARRAY(*)").ToEnumerable()
            .Select<BsonDocument, ICollection<BsonValue>>(x => x.Values.First().AsArray)
            .Where(x => x.Count > 1)
            .Select(x => x.Select(x => x["Path"].AsString));
        foreach (var dupes in allDupes)
        {
            var fullInfo = dupes.Select(x => Hashes.Query().Where(y => y.Path == x).ToList()).ToList();

            var bs = 8 * 1024 * 1024;
            var hashTasks = dupes.Select(x =>
            {
                try
                {
                    return (Path: x, HashTask: Hash.Blake2b(File.OpenRead(x), Pool.Rent(bs).AsMemory(0, bs)));
                }
                catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
                {
                    Console.WriteLine(ex.ToString());
                    return default;
                }
            }).Where(x => x != default).ToList();
            await Task.WhenAll(hashTasks.Select(x => x.HashTask));
            var hashes = hashTasks.Select(x => (x.Path, Hash: x.HashTask.Result)).ToList();

            var collisions = hashes
                .GroupBy(x => x.Hash)
                .Select(x => x.ToList())
                .Where(x => x.Count > 1)
                .ToList();

            var falsePositives = hashes.Count - collisions.Sum(x => x.Count);
            if (falsePositives > 0) Console.WriteLine("False positives " + falsePositives);

            foreach (var realDupes in collisions)
            {
                Console.WriteLine(string.Join(", ", realDupes.Select(x => x.Path)));
            }
        }
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