package org.pentaho.di.lookup;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A Sampling cleanup Object lookup cache implementation
 */
public class SamplingLookupCache extends AbstractLookupCache {
    private static int DEFAULT_MAX_SIZE = 5000;

    private static double DEFAULT_CLEANUP_PCT = 0.1;

    private static Comparator DEFAULT_COMPARATOR = LookupObject.LookupLRUComparator;

    private Map lookupCache;

    public SamplingLookupCache() {
        this(DEFAULT_MAX_SIZE);
    }

    public SamplingLookupCache(int maxSize) {
        this(maxSize, DEFAULT_CLEANUP_PCT, DEFAULT_COMPARATOR);
    }

    public SamplingLookupCache(int maxSize, double cleanupSize) {
        this(maxSize, cleanupSize, DEFAULT_COMPARATOR);
    }

    public SamplingLookupCache(int maxSize, Comparator comparator) {
        this(maxSize, DEFAULT_CLEANUP_PCT, comparator);
    }

    public SamplingLookupCache(int maxSize, double cleanupSize, Comparator comparator) {
        super(maxSize, cleanupSize, comparator);

        lookupCache = Collections.synchronizedMap(new HashMap(maxSize));
    }

    /**
     * Add objects to the cache
     *
     * @param key the key
     * @param value the value
     */
    public void put(Object key, Object value) {
        if(this.maxSize > 0) {  //sanity check
            cacheCleanup();
            lookupCache.put(key, new LookupObject(key, value));
        }
    }

    /**
     * Get a LookupObject from the cache
     *
     * @param key the key
     * @return
     */
    public LookupObject get(Object key) {
        LookupObject lo = (LookupObject) lookupCache.get(key);

        if (lo != null)
            lo.touch();

        return lo;
    }

    public int getCacheSize() {
        return lookupCache.size();
    }

    /**
     * Cleanup method that uses a sampling of X% objects from the cache for cleanup
     */
    protected synchronized void cacheCleanup() {
        if (this.lookupCache.size() > maxSize) {
            cleanupCount++;

            Object[] valuesArray = this.lookupCache.values().toArray();

            // sample only cleanupSize%, default 10
            LookupObject[] valuesSample = Arrays.copyOfRange(valuesArray, 0, (int) Math.ceil((double) maxSize * cleanupSize),
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
