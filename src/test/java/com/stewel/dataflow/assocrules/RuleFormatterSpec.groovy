package com.stewel.dataflow.assocrules

import spock.lang.Specification
import spock.lang.Subject

class RuleFormatterSpec extends Specification {

    @Subject
    RuleFormatter ruleFormatter = new RuleFormatter()

    def "Rule is printed nicely"() {
        given:
        int[] antecedent = [1, 2]
        int[] consequent = [3, 4]
        def coverage = 30 // support of antecedent
        def transactionCount = 40 // support
        def confidence = 0.35123541915812
        def lift = 4.129128401
        Rule rule = new Rule(antecedent, consequent, coverage, transactionCount, confidence, lift)

        expect:
        ruleFormatter.formatRule(rule) == "1,2 => 3,4 (support: 40, confidence: 0.35, lift: 4.13)"
    }
}
