namespace FileReducer;

using System.Collections.Concurrent;
using System.Diagnostics;

public class Profiler
{
    public static readonly Profiler Global = new();

    public static Timer MeasureStatic(string name) => Global.Measure(name);
    public static void MeasureStatic(string name, Action action) => Global.Measure(name, action);
    public static T MeasureStatic<T>(string name, Func<T> action) => Global.Measure(name, action);

    private ConcurrentDictionary<string, Timing> Timings { get; } = new();

    public record struct Timing(TimeSpan TotalTime, int CallCount)
    {
        public TimeSpan AverageTime => CallCount == 0 ? TimeSpan.Zero : TotalTime / CallCount;
        public override string ToString() => $"Total: {TotalTime}\t\tAverage: {AverageTime}\t\tCalls: {CallCount}";
    }

    public record struct Timer(Stopwatch Stopwatch, Profiler Profiler, string Name) : IDisposable
    {
        public void Dispose()
        {
            if (!Stopwatch.IsRunning) return;
            Profiler.StopTimer(this);
        }

        public TimeSpan Stop()
        {
            if (Stopwatch.IsRunning) Profiler.StopTimer(this);
            return Stopwatch.Elapsed;
        }
    }

    private void StopTimer(Timer timer)
    {
        timer.Stopwatch.Stop();
        Timings.AddOrUpdate(timer.Name, _ => new(timer.Stopwatch.Elapsed, 1), (_, prev) => new(prev.TotalTime + timer.Stopwatch.Elapsed, prev.CallCount + 1));
    }

    public Timer Measure(string name) => new(Stopwatch.StartNew(), this, name);

    public void Measure(string name, Action action)
    {
        using var _ = Measure(name);
        action();
    }
    public T Measure<T>(string name, Func<T> func)
    {
        using var _ = Measure(name);
        return func();
    }

    public Timing GetTiming(string name) => Timings[name];

    public void PrintTimings()
    {
        foreach (var timing in Timings) Console.WriteLine($"{timing.Key}\t\t|\t{timing.Value}");
    }
}