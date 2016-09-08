package com.stewel.dataflow.assocrules;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

import javax.annotation.Nonnull;
import java.util.Arrays;

/**
 * This class represent an association rule, where itemsets are arrays of integers.
 */
public final class Rule {

    private final int[] antecedent;
    private final int[] consequent;
    private final int coverage; // support of the antecedent
    private final int transactionCount;
    private final double confidence;
    private final double lift;

    public Rule(@Nonnull final int[] antecedent,
                @Nonnull final int[] consequent,
                final int coverage,
                final int transactionCount,
                final double confidence,
                final double lift) {
        this.antecedent = Arrays.copyOf(antecedent, antecedent.length);
        this.consequent = Arrays.copyOf(consequent, consequent.length);
        this.coverage = coverage;
        this.transactionCount = transactionCount;
        this.confidence = confidence;
        this.lift = lift;
    }

    public int[] getAntecedent() {
        return antecedent;
    }

    public int[] getConsequent() {
        return consequent;
    }

    public int getCoverage() {
        return coverage;
    }

    public int getTransactionCount() {
        return transactionCount;
    }

    public double getConfidence() {
        return confidence;
    }

    public double getLift() {
        return lift;
    }

    public int getAbsoluteSupport() {
        return transactionCount;
    }

    public double getRelativeSupport(final long databaseSize) {
        return ((double) transactionCount) / ((double) databaseSize);
    }

    @Override
    public final boolean equals(final Object object) {
        return EqualsBuilder.reflectionEquals(this, object);
    }

    @Override
    public final int hashCode() {
        return HashCodeBuilder.reflectionHashCode(this);
    }

    @Override
    public final String toString() {
        return ReflectionToStringBuilder.toString(this, ToStringStyle.SHORT_PREFIX_STYLE);
    }
}