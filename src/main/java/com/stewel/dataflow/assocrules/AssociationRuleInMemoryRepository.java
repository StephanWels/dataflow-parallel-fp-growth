package com.stewel.dataflow.assocrules;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class AssociationRuleInMemoryRepository implements AssociationRuleRepository {

    private final List<AssociationRule> associationRules = new ArrayList<>();

    public List<AssociationRule> findAll() {
        return Collections.unmodifiableList(associationRules);
    }

    @Override
    public void save(final AssociationRule associationRule) {
        associationRules.add(associationRule);
    }

}
