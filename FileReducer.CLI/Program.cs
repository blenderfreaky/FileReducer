using FileReducer;

//var folder = @"C:\Users\blend\Downloads";
var folder = @"E:\Stuff";

using (Profiler.MeasureStatic("Total"))
{
    FileHashDb db = new(128);

    //var hash1 = await db.HashFileSystemInfo(new FileInfo(folder + "\\LiteDB.Studio.exe"));
    //var hash2 = await db.HashFileSystemInfo(new FileInfo(folder + "\\LiteDB.Studio(1).exe"));

    //var hasher = new TrivialHasher(5);
    //var hash1 = await DataHash.FromFileAsync(new(folder + "\\LiteDB.Studio.exe"), hasher);
    //var hash2 = await DataHash.FromFileAsync(new(folder + "\\LiteDB.Studio(1).exe"), hasher);

    await db.HashFileSystemInfo(new DirectoryInfo(folder));

    Console.WriteLine("\nDone Hashing\n");

    await db.ListDuplicates();
}

Profiler.Global.PrintTimings();