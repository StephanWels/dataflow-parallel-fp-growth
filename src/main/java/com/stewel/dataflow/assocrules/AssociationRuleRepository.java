package com.stewel.dataflow.assocrules;

import java.util.List;

public interface AssociationRuleRepository {

    List<AssociationRule> findAll();

    void save(AssociationRule associationRule);

}
