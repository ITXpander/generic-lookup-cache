package org.pentaho.di.lookup;

import java.util.*;

/**
 * A cache object with fixed cleanup ammounts
 */
public class FixedLookupCache extends AbstractLookupCache {
    private static int DEFAULT_MAX_SIZE = 5000;

    private static double DEFAULT_CLEANUP_PCT = 0.1; //10%

    private static Comparator DEFAULT_COMPARATOR = LookupObject.LookupLRUComparator;

    private SortedSet cleanupCache;

    private Map lookupCache;

    public FixedLookupCache() {
        this(DEFAULT_MAX_SIZE);
    }

    public FixedLookupCache(int maxSize) {
        this(maxSize, DEFAULT_CLEANUP_PCT, DEFAULT_COMPARATOR);
    }

    public FixedLookupCache(int maxSize, double cleanupSize) {
        this(maxSize, cleanupSize, DEFAULT_COMPARATOR);
    }

    public FixedLookupCache(int maxSize, Comparator comparator) {
        this(maxSize, DEFAULT_CLEANUP_PCT, comparator);
    }

    public FixedLookupCache(int maxSize, double cleanupSize, Comparator comparator) {
        super(maxSize, cleanupSize, comparator);

        cleanupCache = Collections.synchronizedSortedSet(new TreeSet(comparator));
        lookupCache = Collections.synchronizedMap(new HashMap(maxSize));
    }

    /**
     * Add objects to the cache
     *
     * @param key the key
     * @param value the value
     */
    public void put(Object key, Object value) {
        if (this.maxSize > 0) {
            cacheCleanup();

            LookupObject lo = new LookupObject(key, value);

            cleanupCache.add(lo); //for fast cleanup
            lookupCache.put(key, lo); //for fast lookup
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

        if (lo != null) {
            lo.touch();
        }

        return lo;
    }

    public int getCacheSize() {
        return lookupCache.size();
    }

    /**
     * Allow to reset the comparator, this method copies the old set to a new set
     *
     * @param comparator
     */
    @Override
    public void setComparator(Comparator comparator) {
        super.setComparator(comparator);

        //resort
        Set oldSet = this.cleanupCache;
        cleanupCache = Collections.synchronizedSortedSet(new TreeSet(comparator));
        cleanupCache.addAll(oldSet);
    }

    /**
     * cleanup uses the ordering of the TreeSet to remove the first elements
     */
    protected synchronized void cacheCleanup() {
        if (this.lookupCache.size() > maxSize) {
            cleanupCount++;

            int toRemove = (int) Math.ceil((double) maxSize * cleanupSize);

            Iterator it = cleanupCache.iterator();
            for (int i = 0; i < toRemove; i++) {
                if (it.hasNext()) {
                    LookupObject l = (LookupObject) it.next();
                    it.remove();
                    lookupCache.remove(l.getKey());
                } else {
                    break;
                }
            }
        }
    }
}
