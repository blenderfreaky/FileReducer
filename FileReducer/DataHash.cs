namespace FileReducer;

using LiteDB;
using System;
using System.Buffers;

public interface IHasher
{
    Task<Action> Synchronization(FileSystemInfo info, CancellationToken cancellationToken = default);
    DataHash? Cache(FileSystemInfo info);
    void OnHashed(DataHash dataHash, TimeSpan? duration);
    bool ShouldHash(FileSystemInfo info);

    int SegmentLength { get; }
    ArrayPool<byte> ArrayPool { get; }
    bool CacheFiles { get; }
}

public record struct TrivialHasher(int SegmentLength) : IHasher
{
    public ArrayPool<byte> ArrayPool { get; } = ArrayPool<byte>.Shared;
    public bool CacheFiles { get; } = false;

    public DataHash? Cache(FileSystemInfo info) => null;

    public void OnHashed(DataHash dataHash, TimeSpan? duration)
    {
        Console.WriteLine($"Finished hashing ({duration ?? TimeSpan.Zero}) {dataHash.Path}");
    }

    public bool ShouldHash(FileSystemInfo info) => true;

    public Task<Action> Synchronization(FileSystemInfo info, CancellationToken cancellationToken = default) => Task.FromResult(() => { });
}

public record struct DataHash(string Path, string DirectoryPath, bool IsDirectory, int SegmentLength, long DataLength, Hash Hash, DateTime LastWriteUtc, DateTime HashTimeUtc)
{
    [BsonId] public string UUID => SegmentLength.ToString() + ";" + Path;

    public delegate Task<Action> Synchronization(string path, int segmentLength, bool isDirectory, CancellationToken cancellationToken = default);
    public delegate DataHash? Cache(string path, int segmentLength, bool isDirectory);

    public static Task<DataHash> FromFileSystemInfoAsync(FileSystemInfo info, IHasher hasher, CancellationToken cancellationToken = default) =>
        info is DirectoryInfo directoryInfo ? FromFolderAsync(directoryInfo, hasher, cancellationToken)
        : info is FileInfo fileInfo ? FromFileAsync(fileInfo, hasher, cancellationToken)
        : throw new ArgumentException("Should be DirectoryInfo or FileInfo", nameof(info));

    public static FileSystemInfo? FolderOrFile(string path) => Directory.Exists(path) ? new DirectoryInfo(path) : File.Exists(path) ? new FileInfo(path) : null;

    public static async Task<DataHash> FromFolderAsync(DirectoryInfo info, IHasher hasher, CancellationToken cancellationToken = default)
    {
        var sync = hasher.Synchronization(info, cancellationToken);
        byte[]? buffer = null;
        try
        {
            if (cancellationToken.IsCancellationRequested) return default;
            if (sync != null) await sync;
            if (hasher.Cache(info) is DataHash cached) return cached;

            var allHashesTask =
                    info.EnumerateFileSystemInfos().Where(hasher.ShouldHash)
                    .Select(x => FromFileSystemInfoAsync(x, hasher, cancellationToken))
                .ToList();

            await Task.WhenAll(allHashesTask);

            using var timer = Profiler.MeasureStatic("Hashing.Directory");

            var allHashes = allHashesTask.Where(x => x.IsCompletedSuccessfully).Select(x => x.Result).Where(x => x != default).ToList();
            var length = allHashes.Sum(x => x.DataLength);
            var hash = Hash.Blake2b(allHashes.Select(x => x.Hash));

            DataHash fileHash = new(info.FullName, System.IO.Path.GetDirectoryName(info.FullName) ?? "", true, hasher.SegmentLength, length, hash, info.LastWriteTimeUtc, DateTime.UtcNow);
            hasher.OnHashed(fileHash, timer.Stop());
            return fileHash;
        }
        catch (IOException ex)
        {
            Console.WriteLine(ex.ToString());
            return default;
        }
        finally
        {
            if (buffer != null) hasher.ArrayPool.Return(buffer);
            if (sync?.IsCompletedSuccessfully == true) sync.Result?.Invoke();
        }
    }

    public static async Task<DataHash> FromFileAsync(FileInfo info, IHasher hasher, CancellationToken cancellationToken = default)
    {
        var sync = hasher.Synchronization(info, cancellationToken);
        byte[]? buffer = null;
        try
        {
            if (cancellationToken.IsCancellationRequested) return default;
            if (sync != null) await sync;
            using var timer = Profiler.MeasureStatic("Hashing.File");
            if (hasher.CacheFiles && hasher.Cache(info) is DataHash cached) return cached;

            using var stream = info.OpenRead();
            var length = stream.Length;

            Hash hash;

            if (hasher.SegmentLength > 0 && hasher.SegmentLength * 2 < length)
            {
                buffer = hasher.ArrayPool.Rent(hasher.SegmentLength * 2);

                if (length <= hasher.SegmentLength * 2)
                {
                    await stream.ReadAsync(buffer.AsMemory(0, (int)length), cancellationToken);
                }
                else
                {
                    await stream.ReadAsync(buffer.AsMemory(0, hasher.SegmentLength), cancellationToken);
                    if (cancellationToken.IsCancellationRequested) return default;

                    stream.Seek(-hasher.SegmentLength, SeekOrigin.End);
                    await stream.ReadAsync(buffer.AsMemory(hasher.SegmentLength, hasher.SegmentLength), cancellationToken);
                }
                if (cancellationToken.IsCancellationRequested) return default;
                hash = Hash.Blake2b(buffer.AsSpan(0, (int)Math.Min(length, hasher.SegmentLength * 2)));
            }
            else
            {
                const int bs = 4096;
                hash = await Hash.Blake2b(stream, hasher.ArrayPool.Rent(bs).AsMemory(0, bs), false, cancellationToken);
            }

            DataHash fileHash = new(info.FullName, System.IO.Path.GetDirectoryName(info.FullName) ?? "", false, hasher.SegmentLength * 2 < length ? hasher.SegmentLength : 0, length, hash, info.LastWriteTimeUtc, DateTime.UtcNow);
            hasher.OnHashed(fileHash, timer.Stop());
            return fileHash;
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            Console.WriteLine(ex.ToString());
            return default;
        }
        finally
        {
            if (buffer != null) hasher.ArrayPool.Return(buffer);
            if (sync?.IsCompletedSuccessfully == true) sync.Result?.Invoke();
        }
    }
}