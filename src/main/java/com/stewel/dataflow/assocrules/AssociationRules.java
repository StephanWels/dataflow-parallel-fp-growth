package com.stewel.dataflow.assocrules;

import org.immutables.value.Value;

import java.util.List;

/**
 * This class represents a list of association rules.
 */
@Value.Immutable
public interface AssociationRules {

    String getName();

    List<AssociationRule> getRules();
}
