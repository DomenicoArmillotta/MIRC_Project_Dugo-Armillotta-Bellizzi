package utility;


import queryProcessing.ScoreEntry;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * class Cache: extends LinkedHashMap and overrides the removeEldestEntry method to implement the LRU eviction policy.
 * The removeEldestEntry method returns true if the number of elements in the cache exceeds the maximum size, and removes
 * the eldest (least recently used) entry in that case. This way, the cache always keeps the most recently used elements and
 * removes the least recently used elements as needed to maintain the maximum size of the cache.
 */

public class Cache<String, ScoreEntry> extends LinkedHashMap<String, ScoreEntry> {
    private final int maxSize;

    public Cache(int maxSize) {
        super(100, 0.75f, true);
        this.maxSize = maxSize;
    }

    public ScoreEntry get(Object key) {
        return super.get(key);
    }

    public ScoreEntry put(String key, ScoreEntry value) {
        return super.put(key, value);
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<String, ScoreEntry> eldest) {
        return size() > maxSize;
    }
}
