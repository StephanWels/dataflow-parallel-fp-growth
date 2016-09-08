package com.stewel.dataflow.functions

import com.stewel.dataflow.ItemsListWithSupport
import spock.lang.Specification
import spock.lang.Subject

class FrequentItemSetToBigTablePutCommandFnSpec extends Specification {

    def family = "my-family"
    def qualifier = "my-qualifier"

    @Subject
    FrequentItemSetToBigTablePutCommandFn fn = new FrequentItemSetToBigTablePutCommandFn(family, qualifier)

    def 'pattern is used as row and support stored as value'() {
        given:
        def itemsListWithSupport = Mock(ItemsListWithSupport) {
            getKey() >> pattern
            getValue() >> support
        }

        when:
        def result = fn.apply(itemsListWithSupport)

        then:
        result.row == expectedRowKey
        result.getFamilyMap().get(family.bytes) != null

        where:
        pattern   | support | expectedRowKey
        [1, 2, 3] | 100     | [1, 2, 3].toString().bytes
        [3, 1, 2] | 100     | [1, 2, 3].toString().bytes
    }
}
