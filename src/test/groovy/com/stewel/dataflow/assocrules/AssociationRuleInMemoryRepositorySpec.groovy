package com.stewel.dataflow.assocrules

import spock.lang.Specification
import spock.lang.Subject

class AssociationRuleInMemoryRepositorySpec extends Specification {

    @Subject
    def associationRuleInMemoryRepository = new AssociationRuleInMemoryRepository();

    AssociationRule rule1 = ImmutableAssociationRule.builder()
            .antecedent(1, 2)
            .consequent(3, 4)
            .coverage(30)
            .transactionCount(40)
            .confidence(0.35123541915812)
            .lift(4.129128401)
            .build()

    AssociationRule rule2 = ImmutableAssociationRule.builder()
            .antecedent(5, 6)
            .consequent(7, 8)
            .coverage(30)
            .transactionCount(40)
            .confidence(0.35123541915812)
            .lift(4.129128401)
            .build()

    def "save and retrieve association rules"() {
        when:
        associationRuleInMemoryRepository.save(rule1);
        def result1 = associationRuleInMemoryRepository.findAll();

        then:
        result1.size() == 1
        result1[0].antecedent == [1, 2]
        result1[0].consequent == [3, 4]

        and:
        associationRuleInMemoryRepository.save(rule2);
        def result2 = associationRuleInMemoryRepository.findAll();

        then:
        result2.size() == 2
        result2[0].antecedent == [1, 2]
        result2[0].consequent == [3, 4]
        result2[1].antecedent == [5, 6]
        result2[1].consequent == [7, 8]
    }

}
