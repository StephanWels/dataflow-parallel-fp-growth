package com.stewel.dataflow.assocrules

import com.stewel.dataflow.fpgrowth.ImmutableItemset
import com.stewel.dataflow.fpgrowth.Itemsets
import spock.lang.Specification
import spock.lang.Subject

class AlgoAgrawalFaster94Spec extends Specification {

    def associationRuleRepository = new AssociationRuleInMemoryWriter();

    def supportRepository = Mock(SupportRepository) {
        getSupport(_) >> 1l
    }

    @Subject
    def algoAgrawalFaster94 = new AlgoAgrawalFaster94(associationRuleRepository, supportRepository);

    def "generate association rules"() {
        given:
        Itemsets itemsets = new Itemsets();
        itemsets.addItemset(ImmutableItemset.builder().items([1] as int[]).absoluteSupport(3l).build());
        itemsets.addItemset(ImmutableItemset.builder().items([2] as int[]).absoluteSupport(4l).build());
        itemsets.addItemset(ImmutableItemset.builder().items([3] as int[]).absoluteSupport(4l).build());
        itemsets.addItemset(ImmutableItemset.builder().items([5] as int[]).absoluteSupport(4l).build());
        itemsets.addItemset(ImmutableItemset.builder().items([1, 2] as int[]).absoluteSupport(2l).build());
        itemsets.addItemset(ImmutableItemset.builder().items([1, 3] as int[]).absoluteSupport(3l).build());
        itemsets.addItemset(ImmutableItemset.builder().items([1, 5] as int[]).absoluteSupport(2l).build());
        itemsets.addItemset(ImmutableItemset.builder().items([2, 3] as int[]).absoluteSupport(3l).build());
        itemsets.addItemset(ImmutableItemset.builder().items([2, 5] as int[]).absoluteSupport(4l).build());
        itemsets.addItemset(ImmutableItemset.builder().items([3, 5] as int[]).absoluteSupport(3l).build());
        itemsets.addItemset(ImmutableItemset.builder().items([1, 2, 3] as int[]).absoluteSupport(2l).build());
        itemsets.addItemset(ImmutableItemset.builder().items([1, 2, 5] as int[]).absoluteSupport(2l).build());
        itemsets.addItemset(ImmutableItemset.builder().items([1, 3, 5] as int[]).absoluteSupport(2l).build());
        itemsets.addItemset(ImmutableItemset.builder().items([2, 3, 5] as int[]).absoluteSupport(3l).build());
        itemsets.addItemset(ImmutableItemset.builder().items([1, 2, 3, 5] as int[]).absoluteSupport(2l).build());

        when:
        algoAgrawalFaster94.runAlgorithm(itemsets, 5);
        def rules = associationRuleRepository.getAll();

        then:
        rules.size() == 50
        with(rules[0]) {
            antecedent == [2] as int[]
            consequent == [1] as int[]
            coverage == 4
            transactionCount == 2l
            confidence == 0.5
            lift == 0.8333333333333334
        }
        with(rules[1]) {
            antecedent == [1] as int[]
            consequent == [2] as int[]
            coverage == 3
            transactionCount == 2l
            confidence == 0.6666666666666666
            lift == 0.8333333333333334
        }
        with(rules[2]) {
            antecedent == [3] as int[]
            consequent == [1] as int[]
            coverage == 4
            transactionCount == 3l
            confidence == 0.75
            lift == 1.25
        }
        with(rules[3]) {
            antecedent == [1] as int[]
            consequent == [3] as int[]
            coverage == 3
            transactionCount == 3l
            confidence == 1.0
            lift == 1.25
        }
        with(rules[4]) {
            antecedent == [5] as int[]
            consequent == [1] as int[]
            coverage == 4
            transactionCount == 2l
            confidence == 0.5
            lift == 0.8333333333333334
        }
    }

}
