package org.pentaho.di.lookup;

import java.util.Comparator;

/**
 * Interface for LookupCache's
 */
public interface LookupCache {
    public int getCacheSize();

    public long getCleanupCount();

    public void setComparator(Comparator comparator);

    public void put(Object key, Object value);

    public LookupObject get(Object key);
}
