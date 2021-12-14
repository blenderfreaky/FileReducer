namespace FileReducer;

public static class DictionaryExtensions
{
    public static TValue GetOrCreate<TKey, TValue>(this IDictionary<TKey, TValue> dict, TKey key, Func<TValue> factory) =>
        dict.TryGetValue(key, out var value) ? value : dict[key] = factory();

    public static TValue GetOrCreate<TKey, TValue>(this IDictionary<TKey, TValue> dict, TKey key) where TValue : new() =>
        dict.GetOrCreate(key, () => new());
}
