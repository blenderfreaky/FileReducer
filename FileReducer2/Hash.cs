namespace FileReducer2;

using System;
using System.Buffers;
using System.Collections.Generic;

public readonly struct Hash : IEquatable<Hash>, IComparable<Hash>
{
    public readonly byte[] Data;
    public readonly int HashCode;

    public Hash(byte[] hash)
    {
        Data = hash;

        HashCode hashCode = new();
        hashCode.AddBytes(hash);
        HashCode = hashCode.ToHashCode();
    }

    public static Hash Blake2b(ReadOnlySpan<byte> toHash) => new(Blake2Fast.Blake2b.ComputeHash(toHash));
    public static async Task<Hash> Blake2b(FileStream toHash, Memory<byte> buffer, bool dispose = true, IProgress<long>? progress = null, CancellationToken cancellationToken = default)
    {
        var hasher = Blake2Fast.Blake2b.CreateIncrementalHasher();
        int bytesRead;
        while ((bytesRead = await toHash.ReadAsync(buffer, cancellationToken)) > 0)
        {
            hasher.Update(buffer.Span[..bytesRead]);
            progress?.Report(bytesRead);
        }
        if (dispose) await toHash.DisposeAsync();
        return new(hasher.Finish());
    }
    public static async Task<Hash> Blake2b(FileStream toHash, Memory<byte> buffer, long hashLength, bool dispose = true, IProgress<long>? progress = null, CancellationToken cancellationToken = default)
    {
        if (hashLength == 0) return await Blake2b(toHash, buffer, dispose, progress, cancellationToken);

        var hasher = Blake2Fast.Blake2b.CreateIncrementalHasher();
        async Task Read()
        {
            long subTotalRead = 0;
            int bytesRead;
            while (subTotalRead < hashLength
                && (bytesRead = await toHash.ReadAsync(buffer.Slice(0, (int)Math.Min(hashLength - subTotalRead, buffer.Length)), cancellationToken)) > 0)
            {
                hasher.Update(buffer.Span[..bytesRead]);
                progress?.Report(bytesRead);
                subTotalRead += bytesRead;
            }
        }
        await Read();
        toHash.Seek((toHash.Length / 2) - (hashLength / 2), SeekOrigin.Begin);
        await Read();
        toHash.Seek(hashLength, SeekOrigin.End);
        await Read();
        if (dispose) await toHash.DisposeAsync();
        return new(hasher.Finish());
    }

    public static Hash Blake2b(IEnumerable<Hash> hashes) => Blake2b(hashes.OrderBy(x => x).SelectMany(x => x.Data).ToArray());

    public int CompareTo(Hash other)
    {
        var lenDiff = Data.Length.CompareTo(other.Data.Length);
        if (lenDiff != 0) return lenDiff;
        for (int i = 0; i < Data.Length; i++)
        {
            if (Data[i] != other.Data[i]) return Data[i].CompareTo(other.Data[i]);
        }
        return 0;
    }

    public readonly override bool Equals(object? obj) => obj is Hash hash && Equals(hash);
    public readonly bool Equals(Hash other) => HashCode == other.HashCode && (Data == other.Data || Data?.SequenceEqual(other.Data) == true);
    public readonly override int GetHashCode() => HashCode;
    public readonly override string? ToString() => Data == null ? "null" : string.Concat(Data.Select(x => x.ToString("X")));
    public static bool operator ==(Hash left, Hash right) => left.Equals(right);
    public static bool operator !=(Hash left, Hash right) => !(left == right);
}
