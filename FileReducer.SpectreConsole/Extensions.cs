namespace FileReducer.SpectreConsole;

using Spectre.Console;

public static class Extensions
{
    public static void PrintTimingsSpectre(this Profiler profiler)
    {
        Table table = new();
        table.AddColumns("Name", "Total Time", "Average Time", "Calls");
        foreach (var (name, timing) in profiler.Timings)
        {
            table.AddRow(name, timing.TotalTime.ToString(), timing.AverageTime.ToString(), timing.CallCount.ToString());
        }
        AnsiConsole.Write(table);
    }

    public static void PrintAsTree<T>(this IEnumerable<IEnumerable<T>> values, Func<T, string>? toString = null)
    {
        toString ??= x => x?.ToString() ?? "null";
        Tree tree = new("Duplicates");
        foreach (var list in values)
        {
            var node = tree.AddNode("");
            node.AddNodes(list.Select(toString));
        }
        AnsiConsole.Write(tree);
    }
}