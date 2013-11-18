package org.pentaho.di.lookup;

import java.util.Comparator;

/**
 * Abstract Class for Lookup Caches that contains the common ground
 */
public abstract class AbstractLookupCache implements LookupCache {
    protected int maxSize;

    protected double cleanupSize;

    protected long cleanupCount;

    protected Comparator comparator;

    protected AbstractLookupCache(int maxSize, double cleanupSize, Comparator comparator) {
        this.maxSize = maxSize;
        this.cleanupSize = cleanupSize;
        this.comparator = comparator;
        this.cleanupCount = 0;
    }

    public long getCleanupCount() {
        return this.cleanupCount;
    }

    public void setComparator(Comparator comparator) {
        this.comparator = comparator;
    }

    protected abstract void cacheCleanup();
}
