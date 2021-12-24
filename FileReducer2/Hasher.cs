namespace FileReducer2;

using System.Buffers;

public record Hasher(int HashLength, int BlockSize, SemaphoreSlim Semaphore, IProgress<(long ToRead, long TotalRead)>? Progress = null, CancellationToken CancellationToken = default)
{
    private Queue<FileSystemInfo> ToHash { get; } = new();

    private Dictionary<string, Task<FileHash>> Tasks { get; } = new();

    private ArrayPool<byte> Pool { get; } = ArrayPool<byte>.Shared;

    private long ToRead;
    private long TotalRead;

    public async Task<FileHash> Hash(FileSystemInfo info)
    {
        if (Tasks.TryGetValue(info.FullName, out var hashTask)) return await hashTask;
        if (info is FileInfo fileInfo)
        {
            byte[]? buffer = null;
            long length = 0;
            try
            {
                buffer = Pool.Rent(BlockSize);
                using var stream = fileInfo.OpenRead();
                Interlocked.Add(ref ToRead, length = FileHash.GetLength(stream.Length, HashLength));
                try
                {
                    await Semaphore.WaitAsync();
                    var task = FileHash.FromFileAsync(
                        fileInfo,
                        stream,
                        buffer.AsMemory()[..BlockSize],
                        HashLength,
                        new Progress<long>(x =>
                        {
                            Interlocked.Add(ref TotalRead, x);
                            Progress?.Report((TotalRead, ToRead));
                        }),
                        CancellationToken);
                    Tasks[info.FullName] = task;
                    return await task;
                }
                finally
                {
                    Semaphore.Release(1);
                }
            }
            catch (IOException)
            {
                Interlocked.Add(ref ToRead, -length);
            }
            finally // Memory-Leaks are not poggers
            {
                if (buffer != null) Pool.Return(buffer);
            }
        }
        else if (info is DirectoryInfo directoryInfo)
        {
            var files = directoryInfo.EnumerateFileSystemInfos().Select(Hash);
            var task = Tasks[info.FullName] = Task.WhenAll(files)
                .ContinueWith(_ => FileHash.FromDirectory(
                    directoryInfo,
                    files
                    .Where(x => x.IsCompletedSuccessfully)
                        .Select(x => x.Result)
                        .ToList()));
            Tasks[info.FullName] = task;
            return await task;
        }
        throw new ArgumentException("Invalid type", nameof(info));
    }
}