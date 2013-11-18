package org.pentaho.di.lookup;

import java.util.Comparator;
import java.util.Date;

/**
 * The base LookupObject for the cache, that stores lookup info
 */
public class LookupObject implements Comparable<LookupObject> {
    /**
     * Compare 2 Objects by the creation date
     */
    public static Comparator<LookupObject> LookupLRCComparator = new Comparator<LookupObject>() {
        @Override
        public int compare(LookupObject o1, LookupObject o2) {
            if (o1.getKey().equals(o2.getKey())) {
                return 0;
            }

            long comp = o1.getCreationDate() - o2.getCreationDate();
            return (int) comp;
        }
    };

    /**
     * Compare 2 Objects by the last usage date
     */
    public static Comparator<LookupObject> LookupLRUComparator = new Comparator<LookupObject>() {
        @Override
        public int compare(LookupObject o1, LookupObject o2) {
            if (o1.getKey().equals(o2.getKey())) {
                return 0;
            }

            long comp = o1.getLastAccessDate() - o2.getLastAccessDate();
            return (int) comp;
        }
    };

    /**
     * Compare 2 Objects by number of hits
     */
    public static Comparator<LookupObject> LookupHitsComparator = new Comparator<LookupObject>() {
        @Override
        public int compare(LookupObject o1, LookupObject o2) {
            if (o1.getKey().equals(o2.getKey())) {
                return 0;
            }

            long comp = o1.getHits() - o2.getHits();

            if (comp == 0) { // even if they have the same hits they're not the same
                comp = o1.getCreationDate() - o2.getCreationDate();
            }

            return (int) comp;
        }
    };

    private Object key;

    private Object object;

    private long creationDate;

    private long lastAccessDate;

    private long hits;

    LookupObject(Object k, Object o) {
        this.key = k;
        this.object = o;
        this.creationDate = System.currentTimeMillis();
        this.lastAccessDate = this.creationDate;
        this.hits = 0;
    }

    public long getCreationDate() {
        return this.creationDate;
    }

    public long getLastAccessDate() {
        return this.lastAccessDate;
    }

    public void touch() {
        updateLastAccessDate();
        increaseHits();
    }

    public Object getKey() {
        return this.key;
    }

    public Object getObject() {
        return this.object;
    }

    public synchronized long getHits() {
        return this.hits;
    }

    private synchronized void increaseHits() {
        this.hits++;
    }

    private synchronized void updateLastAccessDate() {
        long newDate = System.currentTimeMillis();

        if (newDate > this.lastAccessDate) {
            this.lastAccessDate = newDate;
        }
    }

    @Override
    public String toString() {
        return "LookupObject{" +
            "key=" + key +
            ", creationDate=" + creationDate +
            ", lastAccessDate=" + lastAccessDate +
            ", hits=" + hits +
            '}';
    }

    /**
     * Override for equals since key is the main source of equality
     *
     * @param o other object to compare
     * @return
     */
    @Override
    public boolean equals(Object o) {

        //if same instance
        if (this == o) {
            return true;
        }

        // sanity check for type
        if (!(o instanceof LookupObject)) {
            return false;
        }

        LookupObject lo = (LookupObject) o;

        return this.getKey().equals(lo);
    }

    /**
     * Override for hashCode since key is the main source of equality
     *
     * @return
     */
    @Override
    public int hashCode() {
        return this.getKey().hashCode();
    }

    /**
     * Default comparator by number of hits
     *
     * @param o2
     * @return
     */
    @Override
    public int compareTo(LookupObject o2) {
        if (this.getKey().equals(o2.getKey())) {
            return 0;
        }

        long comp = this.getHits() - o2.getHits();

        if (comp == 0) { // even if they have the same hits they're not the same
            comp = this.getCreationDate() - o2.getCreationDate();
        }

        return (int) comp;
    }
}
