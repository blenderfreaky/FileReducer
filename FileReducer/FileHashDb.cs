using System;

namespace FileReducer
{
    using Blake2Fast;
    using LiteDB;
    using System;
    using System.Buffers;
    using System.Collections.Generic;
    using System.Text;

    public class FileHashDb : IDisposable
    {
        private bool disposedValue;

        public LiteDatabase Database { get; }
        public ILiteCollection<FileHash> Hashes { get; }

        public ArrayPool<byte> Pool { get; } = ArrayPool<byte>.Shared;

        public FileHashDb()
        {
            Database = new("Cache.db");
            Hashes = Database.GetCollection<FileHash>("hashes");
            Hashes.EnsureIndex(x => x.Path);
            BsonMapper.Global.RegisterType<Hash>
            (
                serialize: obj => new BsonValue(obj.Data),
                deserialize: doc => new Hash(doc.AsBinary)
            );
        }

        public async Task HashFolder(string path, int segmentLength = 8192, CancellationToken cancellationToken = default)
        {
            path = Path.GetFullPath(path);
            Console.WriteLine("Entering folder " + path);
            await Task.WhenAll(
                Task.WhenAll(Directory.GetFiles(path).Select(x => HashFile(x, segmentLength, cancellationToken))),
                Task.WhenAll(Directory.GetDirectories(path).Select(x => HashFolder(x, segmentLength, cancellationToken))));
        }

        public async Task<FileHash> HashFile(string path, int segmentLength = 8192, CancellationToken cancellationToken = default)
        {
            path = Path.GetFullPath(path);
            var cached = Hashes.Query().Where(x => x.Path == path).FirstOrDefault();
            if (cached != default)
            {
                Console.WriteLine("File cached " + path);
                return cached;
            }
            Console.WriteLine("Hashing file " + path);
            var buffer = Pool.Rent(segmentLength * 2);
            var stream = File.OpenRead(path);
            var length = stream.Length;

            if (length <= segmentLength * 2)
            {
                await stream.ReadAsync(buffer.AsMemory(0, (int)length), cancellationToken);
            }
            else
            {
                await stream.ReadAsync(buffer.AsMemory(0, segmentLength), cancellationToken);
                if (cancellationToken.IsCancellationRequested) return default;
                stream.Seek(segmentLength, SeekOrigin.End);
                await stream.ReadAsync(buffer.AsMemory(segmentLength, segmentLength), cancellationToken);
            }
            if (cancellationToken.IsCancellationRequested) return default;

            var hash = Hash.Blake2b(buffer.AsSpan(0, (int)Math.Min(length, segmentLength * 2)));

            FileHash fileHash = new(path, segmentLength, hash);
            Hashes.Insert(fileHash);
            return fileHash;
        }

        public void ListDuplicates()
        {
            var dupes = Hashes.Query()
                .GroupBy("$.Hash").Select("ARRAY(*)").ToEnumerable()
                .Select<BsonDocument, ICollection<BsonValue>>(x => x.Values.First().AsArray)
                .Where(x => x.Count > 1)
                .Select(x => x.Select(x => x["Path"].AsString));
            foreach (var dupe in dupes) Console.WriteLine(string.Join(", ", dupe));
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
    public record struct FileHash(string Path, int SegmentLength, Hash Hash);
    public readonly struct Hash : IEquatable<Hash>
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

        public readonly override bool Equals(object? obj) => obj is Hash hash && Equals(hash);
        public readonly bool Equals(Hash other) => HashCode == other.HashCode && (Data == other.Data || Data?.SequenceEqual(other.Data) == true);
        public readonly override int GetHashCode() => HashCode;
        public readonly override string? ToString() => Data == null ? "null" : string.Concat(Data.Select(x => x.ToString("X")));
        public static bool operator ==(Hash left, Hash right) => left.Equals(right);
        public static bool operator !=(Hash left, Hash right) => !(left == right);
    }
}