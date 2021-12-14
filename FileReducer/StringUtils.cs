namespace FileReducer;

public static class StringUtils
{
    public static string ShortJoin(string separator, IReadOnlyList<string> strings, int maxLength = 150, string empty = "None", string cont = "...")
    {
        if (strings.Count == 0) return empty;
        if (strings.Sum(x => x.Length + separator.Length) - separator.Length <= maxLength) return string.Join(separator, strings);
        var last = separator.Length + strings[strings.Count - 1];
        if (last.Length > maxLength) return "Too long (" + strings.Count + " elements)";
        int i = 0;
        int len = maxLength - last.Length - cont.Length;
        for (; i < strings.Count; i++) if ((len -= strings[i].Length + separator.Length) < 0) break;
        if (i == 0) return "Too long (" + strings.Count + " elements)";
        return string.Join(separator, strings.Take(i)) + separator + cont + last;
    }
}
