package com.stewel.dataflow.assocrules;

import org.immutables.value.Value;

/**
 * This class represent an association rule, where itemsets are arrays of integers.
 */
@Value.Immutable
public abstract class AssociationRule {

    public abstract int[] getAntecedent();

    public abstract int[] getConsequent();

    public abstract int getCoverage();

    public abstract int getTransactionCount();

    public abstract double getConfidence();

    public abstract double getLift();

    public int getAbsoluteSupport() {
        return getTransactionCount();
    }

    public double getRelativeSupport(final long databaseSize) {
        return ((double) getTransactionCount()) / ((double) databaseSize);
    }
}