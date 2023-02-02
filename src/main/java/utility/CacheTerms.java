package utility;

import java.util.LinkedHashMap;
import java.util.Map;

public class CacheTerms <String, LexiconStats> extends LinkedHashMap<String, LexiconStats> {
    private final int maxSize;

    public CacheTerms(int maxSize) {
        super(100, 0.75f, true);
        this.maxSize = maxSize;
    }

    public LexiconStats get(Object key) {
        return super.get(key);
    }

    public LexiconStats put(String key, LexiconStats value) {
        return super.put(key, value);
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<String, LexiconStats> eldest) {
        return size() > maxSize;
    }
}
