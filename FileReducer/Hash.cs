namespace FileReducer;

using LiteDB;
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
    public static async Task<Hash> Blake2b(FileStream toHash, Memory<byte> buffer, bool dispose = true, CancellationToken cancellationToken = default)
    {
        var hasher = Blake2Fast.Blake2b.CreateIncrementalHasher();
        var length = toHash.Length;
        for (long i = 0; i < length; i += buffer.Length)
        {
            var bufferSlice = buffer[..(int)Math.Min(length - i, buffer.Length)];
            await toHash.ReadAsync(bufferSlice, cancellationToken);
            hasher.Update(bufferSlice.Span);
        }
        if (dispose) await toHash.DisposeAsync();
        return new(hasher.Finish());
    }

    public static Hash Blake2b(IEnumerable<Hash> hashes) => Blake2b(hashes.OrderBy(x => x).SelectMany(x => x.Data).ToArray());

    public int CompareTo(Hash other)
    {
        for (int i = 0; i < Data.Length && i < other.Data.Length; i++)
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
