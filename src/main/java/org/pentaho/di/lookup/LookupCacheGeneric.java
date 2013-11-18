package org.pentaho.di.lookup;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Lookup Object cache with Generic for Key type
 * @param <K>
 */
public class LookupCacheGeneric<K> {
    private static int DEFAULT_MAX_SIZE = 5000;

    private static int DEFAULT_CLEANUP_PCT = 10;

    private static Comparator DEFAULT_COMPARATOR = LookupObject.LookupHitsComparator;

    private int maxSize;

    private int cleanupSize;

    private long cleanupCount;

    private Comparator comparator = null;

    private ConcurrentHashMap<K, LookupObject> lookupCache;

    public LookupCacheGeneric() {
        this(DEFAULT_MAX_SIZE);
    }

    public LookupCacheGeneric(int maxSize) {
        this(maxSize, DEFAULT_CLEANUP_PCT, DEFAULT_COMPARATOR);
    }

    public LookupCacheGeneric(int maxSize, Comparator comparator) {
        this(maxSize, DEFAULT_CLEANUP_PCT, comparator);
    }

    public LookupCacheGeneric(int maxSize, int cleanupSize, Comparator comparator) {
        this.maxSize = maxSize;
        this.cleanupSize = cleanupSize;
        this.comparator = comparator;

        lookupCache = new ConcurrentHashMap<K, LookupObject>(maxSize);
        cleanupCount = 0;
    }

    public LookupObject get(K key) {
        LookupObject v = lookupCache.get(key);

        if (v != null)
            v.touch();

        return v;
    }

    public void put(K key, Object value) {
        cacheCleanup();
        lookupCache.put(key, new LookupObject(key, value));
    }

    public int getCacheSize() {
        return lookupCache.size();
    }

    public long getCleanupCount() {
        return this.cleanupCount;
    }

    public void setComparator(Comparator comparator) {
        this.comparator = comparator;
    }

    private void cacheCleanup() {
        if (this.lookupCache.size() > maxSize) {
            cleanupCount++;

            Object[] valuesArray = this.lookupCache.values().toArray();

            // sample only cleanupSize%, default 10
            LookupObject[] valuesSample = Arrays.copyOfRange(valuesArray, 0, (int)Math.ceil((double)maxSize/cleanupSize),
                LookupObject[].class);

            Arrays.sort(valuesSample, comparator);

            int count = 0;
            LookupObject ref = valuesSample[0];
            for (int i = 0; i < valuesArray.length; i++) {
                int comp = comparator.compare(valuesArray[i],ref);

                //smaller items than the reference one get discarded from cache
                if (comp < 0) {
                    lookupCache.remove(((LookupObject) valuesArray[i]).getKey());
                    count++;
                }
            }

            // worse case scenario, only remove the ref value
            if(count == 0)
                lookupCache.remove(ref.getKey());
        }
    }
}
