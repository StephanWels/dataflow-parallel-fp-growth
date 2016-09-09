package com.stewel.dataflow.assocrules;

import org.immutables.value.Value;

/**
 * This class represent an association rule, where itemsets are arrays of integers.
 */
@Value.Immutable
public abstract class AssociationRule {

    public abstract int[] getAntecedent();

    public abstract int[] getConsequent();

    public abstract long getCoverage();

    public abstract long getTransactionCount();

    public abstract double getConfidence();

    public abstract double getLift();

    public long getAbsoluteSupport() {
        return getTransactionCount();
    }

    public double getRelativeSupport(final long databaseSize) {
        return ((double) getTransactionCount()) / ((double) databaseSize);
    }
}