package com.stewel.dataflow.assocrules

import spock.lang.Specification
import spock.lang.Subject

class AssociationRuleFormatterSpec extends Specification {

    @Subject
    AssociationRuleFormatter ruleFormatter = new AssociationRuleFormatter([1: 'Apfel', 3: 'Birne'])

    def "Rule is printed nicely"() {
        given:
        AssociationRule rule = ImmutableAssociationRule.builder()
                .antecedent(1, 2)
                .consequent(3, 4)
                .coverage(30)
                .transactionCount(40)
                .confidence(0.35123541915812)
                .lift(4.129128401)
                .build()

        expect:
        ruleFormatter.formatRule(rule) == "Apfel,2 => Birne,4 (support: 40, confidence: 0.35, lift: 4.13)"
    }
}
