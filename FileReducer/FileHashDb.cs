namespace FileReducer;

using LiteDB;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
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

        BsonMapper.Global.RegisterType
        (
            serialize: obj => new BsonValue(obj.Data),
            deserialize: doc => new Hash(doc.AsBinary)
        );

        //BsonMapper.Global.Entity<DataHash>().Id(x => x.UUID);

        Hashes = Database.GetCollection<DataHash>("hashes");
        Hashes.EnsureIndex(x => x.UUID, true);
    }

    private async Task<Action> Synchronization(FileSystemInfo _, CancellationToken cancellationToken = default)
    {
        await SemaphoreSlim.WaitAsync(cancellationToken);
        return () => SemaphoreSlim.Release();
    }

    private Dictionary<int, Dictionary<string, DataHash>> InMemoryCache { get; } = new();
    private Dictionary<int, HashSet<string>> NotInFileCache { get; } = new();

    private bool MemCache(FileSystemInfo info, int segmentLength, out DataHash hash) =>
        InMemoryCache.GetOrCreate(segmentLength).TryGetValue(info.FullName, out hash) && IsValidCache(hash, info, segmentLength);

    private DataHash? Cache(FileSystemInfo info, int segmentLength, bool preCacheDirectories, bool restrictFilesToMemoryCache)
    {
        try
        {
            using var _ = Profiler.MeasureStatic("Caching");
            var isDirectory = info is not FileInfo fileInfo;

            if (MemCache(info, segmentLength, out var cached)) return cached;

            if (NotInFileCache.GetOrCreate(segmentLength).Contains(info.FullName)) return null;

            if (!isDirectory && restrictFilesToMemoryCache)
            {
                var directory = Path.GetDirectoryName(info.FullName);
                PreCache(directory ?? "");

                if (MemCache(info, segmentLength, out cached)) return cached;
                NotInFileCache.GetOrCreate(segmentLength).Add(info.FullName);
                return null;
            }

            var result = Hashes.Query()
                .Where(x => x.Path == info.FullName && x.LastWriteUtc >= info.LastWriteTimeUtc)
                .Where(SegmentLengthCheck(segmentLength))
                .FirstOrDefault();
            if (result == default || !IsValidCache(result, info, segmentLength))
            {
                NotInFileCache.GetOrCreate(segmentLength).Add(info.FullName);
                return null;
            }

            AddToMemCache(result);
            if (isDirectory && preCacheDirectories)
            {
                PreCache(info.FullName);
            }

            return result;
        }
        catch (LiteException ex)
        {
            Console.WriteLine(info);
            Console.WriteLine(ex.ToString());
            return null;
        }
    }

    private void PreCache(string directory)
    {
        // Assume preCacheDirectories is on
        if (InMemoryCache.Values.Any(x => x.ContainsKey(directory))) return;

        // TODO: Can this be more system-independent?
        //       Maybe force C# to always use / instead of \\
        foreach (var file in Hashes.Query().Where(x =>
            x.DirectoryPath == directory || x.DirectoryPath.StartsWith(directory + Path.DirectorySeparatorChar))
            .ToEnumerable())
        {
            AddToMemCache(file);
        }
    }

    private void AddToMemCache(DataHash hash) => InMemoryCache.GetOrCreate(hash.SegmentLength)[hash.Path] = hash;

    //private static DataHash Newer(DataHash a, DataHash b) => a.LastWriteUtc > b.LastWriteUtc ? a : b;

    private static bool IsValidCache(DataHash hash, FileSystemInfo info, int? segmentLength = null)
    {
        if (hash == default) return false;
        var fileInfo = info as FileInfo;
        var isDirectory = fileInfo == null;
        if (hash.IsDirectory != isDirectory) return false;
        if (hash.Path != info.FullName) return false;
        if (hash.LastWriteUtc < info.LastWriteTimeUtc) return false;
        if (segmentLength != null && hash.SegmentLength != segmentLength) return false;
        if (!isDirectory && fileInfo!.Length != hash.DataLength) return false;
        return true;
    }

    private record struct HashPass(FileHashDb FileHashDb, int SegmentLength, bool CacheFiles, bool PreCacheDirectories, bool RestrictFilesToMemoryCache, Func<FileSystemInfo, bool> ShouldHash, Action<DataHash, TimeSpan?> OnHashed) : IHasher
    {
        public ArrayPool<byte> ArrayPool { get; } = ArrayPool<byte>.Shared;

        public DataHash? Cache(FileSystemInfo info) => FileHashDb.Cache(info, SegmentLength, PreCacheDirectories, RestrictFilesToMemoryCache);

        void IHasher.OnHashed(DataHash dataHash, TimeSpan? duration) => OnHashed(dataHash, duration);

        public Task<Action> Synchronization(FileSystemInfo info, CancellationToken cancellationToken = default) => FileHashDb.Synchronization(info, cancellationToken);

        bool IHasher.ShouldHash(FileSystemInfo info) => ShouldHash(info);
    }

    public async Task<DataHash> HashFileSystemInfo(FileSystemInfo info, int segmentLength = 8192, IProgress<DataHash.ProgressReport>? progress = null, CancellationToken cancellationToken = default)
    {
        Ignore.Ignore ignore = new();
        var path = Path.Combine(info is DirectoryInfo directory ? directory.FullName : Path.GetDirectoryName(info.FullName)!, ".dupeignore");
        if (File.Exists(path)) ignore.Add(File.ReadAllLines(path));

        if (info is DirectoryInfo) PreCache(info.FullName);

        var hash = await DataHash.FromFileSystemInfoAsync(info, new HashPass(this, segmentLength, true, true, true, info => !ignore.IsIgnored(info.FullName), (hash, time) =>
        {
            try
            {
                Hashes.Upsert(hash);
                //if (hash.IsDirectory) Console.WriteLine($"Finished hashing ({time ?? TimeSpan.Zero}) {hash.Path}");
            }
            catch (LiteException) { }
        }), progress, cancellationToken);
        return hash;
    }

    public record struct ProgressReportDuplicates(int Step, int Steps, ProgressReportVerification SubReport)
    {
        public double ToPercentage() => 100 * (Step / (double)Steps);
    }

    public async Task<List<List<DataHash>>> GetDuplicates(string? directory = null, IProgress<ProgressReportDuplicates>? progress = null, CancellationToken cancellationToken = default)
    {
        var allDupes = Profiler.MeasureStaticF("Duplicates.Query", () => GetDuplicateCandidates(directory));

        var previousDupeCount = allDupes.Sum(x => x.Count);

        using var _ = Profiler.MeasureStatic("Duplicates.Verification");
        var steps = new int[] { 2, 4, 8, 16, 32, 64, 0 };
        int i = 0;
        foreach (var step in steps)
        {
            if (cancellationToken.IsCancellationRequested) return null!;

            var subProgress = new Progress<ProgressReportVerification>(x => progress?.Report(new (i, steps.Length, x)));

            //Console.WriteLine("Starting dedupe step " + step);
            //Profiler.Global.PrintTimings();
            using var __ = Profiler.MeasureStatic("Duplicates.Verification." + step);
            allDupes = (await VerifyDuplicates(allDupes, step * 8192, subProgress, cancellationToken)).Select(x => x.ToList()).ToList();

            var falsePositives = allDupes.Sum(x => x.Count) - previousDupeCount;
            //if (falsePositives > 0) Console.WriteLine("Removed " + falsePositives + " false positives in step " + step);
            //else Console.WriteLine("No false positives in step " + step);
            i++;
        }

        progress?.Report(new(steps.Length, steps.Length, default));

        return allDupes;
    }

    private List<List<DataHash>> GetDuplicateCandidates(string? directory = null, int segmentLength = 8192)
    {
        var allHashes = Hashes.Query().Where(SegmentLengthCheck(segmentLength));
        if (directory != null) allHashes = allHashes.Where(x => x.DirectoryPath.StartsWith(directory));
        var dupes = allHashes
            .GroupBy("$.Hash").Select("ARRAY(*)").ToEnumerable()
            .Select<BsonDocument, ICollection<BsonValue>>(x => x.Values.First().AsArray)
            .Where(x => x.Count > 1)
            .Select(x => x.Select(x => BsonMapper.Global.ToObject<DataHash>(x.AsDocument)).ToList())
            .ToList();
        foreach (var dupe in dupes.SelectMany(x => x)) AddToMemCache(dupe);
        return dupes;
    }

    // TODO: Add DB migration so that x.DataLength <= x.SegmentLength always gets replaced with x.SegmentLength == 0
    private static Expression<Func<DataHash, bool>> SegmentLengthCheck(int segmentLength) =>
        segmentLength == 0
        ? x => x.SegmentLength == 0 || x.DataLength <= x.SegmentLength * 2
        : x => x.SegmentLength == segmentLength
        || (x.DataLength <= segmentLength * 2 && (x.SegmentLength == 0 || x.DataLength <= x.SegmentLength * 2));

    public record struct ProgressReportVerification(long BytesRead, long BytesToRead, DataHash.ProgressReport SubReport)
    {
        public double ToPercentage() => 100 * (BytesRead / (double)BytesToRead);
    }

    // TODO: IMPORTANT: Exclude files in duplicate candidate folders until the folders get verified to either
    //                      be duplicates of each other  => only check for one and return for both
    //                      be different from each other => check both individually
    private async Task<IEnumerable<IEnumerable<DataHash>>> VerifyDuplicates(IEnumerable<IEnumerable<DataHash>> duplicateGroups, int segmentLength = 0, IProgress<ProgressReportVerification>? progress = null, CancellationToken cancellationToken = default)
    {
        long totalToRead = 0;
        long totalRead = 0;
        var subProgress = progress == null ? null : new Progress<DataHash.ProgressReport>(x => {
            var prevBytesToRead = totalToRead;
            var toReadDiff = x.BytesToRead - x.PrevBytesToRead;
            if (toReadDiff != 0 && x.BytesToRead != long.MaxValue)
            {
                Interlocked.Add(ref totalToRead, x.PrevBytesToRead == long.MaxValue ? x.BytesToRead : toReadDiff);
            }
            if (x.BytesRead != 0) Interlocked.Add(ref totalRead, x.BytesRead);
            progress.Report(new(totalRead, totalToRead, x));
        });

        var result = duplicateGroups.Select(async duplicates =>
        {
            var hashTasks = duplicates.Select(async x =>
                {
                    var info = DataHash.FolderOrFile(x.Path);
                    if (info == null) return default;
                    return await HashFileSystemInfo(info, segmentLength, subProgress, cancellationToken);
                })
                .Where(x => x != default).ToList();

            await Task.WhenAll(hashTasks.Select(x => x));
            var hashes = hashTasks.ConvertAll(x => x.Result);

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