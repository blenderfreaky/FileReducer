using FileReducer;
using FileReducer.SpectreConsole;
using FileReducer2;
using Spectre.Console;

var folder = @"C:\Users\blend\Downloads";
//var folder = @"E:\Stuff";

using (Profiler.MeasureStatic("Total"))
{
    //FileHashDb db = new(32);

    //await AnsiConsole.Progress()
    //    .StartAsync(async ctx =>
    //    {
    //        // Define tasks
    //        var hashing = ctx.AddTask("[green]Hashing files[/]");

    //        await db.HashFileSystemInfo(new DirectoryInfo(folder), progress: new Progress<DataHash.ProgressReport>(x => hashing.Value = x.ToPercentage()));
    //    });

    //List<List<DataHash>>? dupes = null;

    //await AnsiConsole.Progress()
    //    .StartAsync(async ctx =>
    //    {
    //        // Define tasks
    //        var hashing1 = ctx.AddTask("[green]Checking duplicates[/]");
    //        var hashing2 = ctx.AddTask("[green]Hashing bigger segments for all duplicates[/]");
    //        var hashing3 = ctx.AddTask("[green]Current hashing[/]");

    //        dupes = await db.GetDuplicates(folder, new Progress<FileHashDb.ProgressReportDuplicates>(x => {
    //            hashing1.Value = x.ToPercentage();
    //            hashing2.Value = x.SubReport.ToPercentage();
    //            hashing3.Value = x.SubReport.ToPercentage();
    //        }));
    //    });

    //dupes!.PrintAsTree();

    //await File.WriteAllLinesAsync("dupes.csv", dupes!.Select(x => string.Join(',', x)));

    await AnsiConsole.Progress()
        .StartAsync(async ctx =>
        {
            // Define tasks
            var hashing = ctx.AddTask("[green]Hashing files[/]");

            Hasher hasher = new(1024, 4096, new(128, 128), new Progress<(long TotalRead, long ToRead)>(x => hashing.Value = 
            (double)x.TotalRead / (double)x.ToRead * 100d), default);

            await hasher.Hash(new DirectoryInfo(folder));
        });
}

Profiler.Global.PrintTimingsSpectre();