namespace FileReducer2;

public record FileHash(string File, bool IsDirectory, long Length, long HashLength, DateTime LastModifiedUTC, Hash Hash)
{
    public static long GetLength(long fileLength, long hashLength) =>
        hashLength * 3 >= fileLength ? fileLength : hashLength*3;

    public static async Task<FileHash> FromFileAsync(FileInfo fileInfo, FileStream stream, Memory<byte> buffer, long hashLength, IProgress<long>? progress = null, CancellationToken cancellationToken = default)
    {
        hashLength = hashLength * 3 >= stream.Length ? 0 : hashLength;
        var hash = await Hash.Blake2b(stream, buffer, hashLength, false, progress, cancellationToken);
        return new(fileInfo.FullName, false, stream.Length, hashLength, fileInfo.LastWriteTimeUtc, hash);
    }

    public static FileHash FromDirectory(DirectoryInfo directoryInfo, IReadOnlyCollection<FileHash> content)
    {
        var length = content.Sum(x => x.Length);
        var hash = Hash.Blake2b(content.Select(x => x.Hash));
        return new(directoryInfo.FullName, true, length, 0, directoryInfo.LastWriteTimeUtc, hash);
    }
}
