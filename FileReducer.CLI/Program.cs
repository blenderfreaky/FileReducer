using FileReducer;

//var folder = @"C:\Users\blend\Downloads";
var folder = @"E:\Stuff";

using (Profiler.MeasureStatic("Total"))
{
    FileHashDb db = new();

    await db.HashFileSystemInfo(new DirectoryInfo(folder));

    db.ListDuplicates();
}

Profiler.Global.PrintTimings();