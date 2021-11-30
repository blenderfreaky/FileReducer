﻿namespace FileReducer;

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
}

public record struct DataHash(string Path, bool IsDirectory, int SegmentLength, long DataLength, Hash Hash, DateTime LastWriteUtc, DateTime HashTimeUtc)
{
    public delegate Task<Action> Synchronization(string path, int segmentLength, bool isDirectory, CancellationToken cancellationToken = default);
    public delegate DataHash? Cache(string path, int segmentLength, bool isDirectory);

    public static Task<DataHash> FromFileSystemInfoAsync(FileSystemInfo info, IHasher hasher, CancellationToken cancellationToken = default) =>
        info is DirectoryInfo directoryInfo ? FromFolderAsync(directoryInfo, hasher, cancellationToken)
        : info is FileInfo fileInfo ? FromFileAsync(fileInfo, hasher, cancellationToken)
        : throw new ArgumentException("Should be DirectoryInfo or FileInfo", nameof(info));

    public static async Task<DataHash> FromFolderAsync(DirectoryInfo info, IHasher hasher, CancellationToken cancellationToken = default)
    {
        var sync = hasher.Synchronization(info, cancellationToken);
        byte[]? buffer = null;
        try
        {
            if (hasher.Cache(info) is DataHash cached) return cached;
            if (sync != null) await sync;

            var allHashesTask =
                    info.EnumerateFileSystemInfos().Where(hasher.ShouldHash)
                    .Select(x => FromFileSystemInfoAsync(x, hasher, cancellationToken))
                .ToList();

            await Task.WhenAll(allHashesTask);

            using var timer = Profiler.MeasureStatic("Hashing.Directory");

            var allHashes = allHashesTask.Where(x => x.IsCompletedSuccessfully).Select(x => x.Result).Where(x => x != default).ToList();
            var length = allHashes.Sum(x => x.DataLength);
            var hash = Hash.Blake2b(allHashes.Select(x => x.Hash));

            DataHash fileHash = new(info.FullName, true, hasher.SegmentLength, length, hash, info.LastWriteTimeUtc, DateTime.UtcNow);
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
            if (hasher.Cache(info) is DataHash cached) return cached;
            if (sync != null) await sync;

            using var timer = Profiler.MeasureStatic("Hashing.File");
            using var stream = info.OpenRead();
            var length = stream.Length;

            buffer = hasher.ArrayPool.Rent(hasher.SegmentLength * 2);

            if (length <= hasher.SegmentLength * 2)
            {
                await stream.ReadAsync(buffer.AsMemory(0, (int)length), cancellationToken);
            }
            else
            {
                await stream.ReadAsync(buffer.AsMemory(0, hasher.SegmentLength), cancellationToken);
                if (cancellationToken.IsCancellationRequested) return default;

                stream.Seek(hasher.SegmentLength, SeekOrigin.End);
                await stream.ReadAsync(buffer.AsMemory(hasher.SegmentLength, hasher.SegmentLength), cancellationToken);
            }
            if (cancellationToken.IsCancellationRequested) return default;

            var hash = Hash.Blake2b(buffer.AsSpan(0, (int)Math.Min(length, hasher.SegmentLength * 2)));

            DataHash fileHash = new(info.FullName, false, hasher.SegmentLength, length, hash, info.LastWriteTimeUtc, DateTime.UtcNow);
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