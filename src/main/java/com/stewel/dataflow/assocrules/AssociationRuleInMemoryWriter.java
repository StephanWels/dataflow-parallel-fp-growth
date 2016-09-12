package com.stewel.dataflow.assocrules;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class AssociationRuleInMemoryWriter implements AssociationRuleWriter {

    private final List<AssociationRule> associationRules = new ArrayList<>();

    public List<AssociationRule> getAll() {
        return Collections.unmodifiableList(associationRules);
    }

    @Override
    public void write(final AssociationRule associationRule) {
        associationRules.add(associationRule);
    }

}
