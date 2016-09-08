package com.stewel.dataflow.assocrules

import spock.lang.Specification
import spock.lang.Subject

class RuleFormatterSpec extends Specification {

    @Subject
    RuleFormatter ruleFormatter = new RuleFormatter()

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
        ruleFormatter.formatRule(rule) == "1,2 => 3,4 (support: 40, confidence: 0.35, lift: 4.13)"
    }
}
