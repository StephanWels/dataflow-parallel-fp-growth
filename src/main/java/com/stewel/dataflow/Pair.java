package com.stewel.dataflow;

import java.io.Serializable;

public class Pair<K,V> implements Serializable {

    private final K key;

    public K getKey() { return key; }

    private final V value;

    public V getValue() { return value; }

    public Pair(final K key, final V value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public String toString() {
        return key + "=" + value;
    }

    @Override
    public int hashCode() {
        return key.hashCode() * 13 + (value == null ? 0 : value.hashCode());
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o instanceof Pair) {
            final Pair pair = (Pair) o;
            if (key != null ? !key.equals(pair.key) : pair.key != null) return false;
            if (value != null ? !value.equals(pair.value) : pair.value != null) return false;
            return true;
        }
        return false;
    }
}