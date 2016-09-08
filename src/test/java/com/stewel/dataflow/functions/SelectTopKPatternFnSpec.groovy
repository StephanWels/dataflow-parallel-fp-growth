package com.stewel.dataflow.functions

import com.google.cloud.dataflow.sdk.values.KV
import com.stewel.dataflow.ItemsListWithSupport
import spock.lang.Specification
import spock.lang.Subject

class SelectTopKPatternFnSpec extends Specification {
    def k = 2;

    @Subject
    SelectTopKPatternFn fn = new SelectTopKPatternFn(k)

    def 'extracts product id and transaction id'() {
        given:
        def productId = 42
        def pattern1 = Mock(ItemsListWithSupport) {
            getKey() >> [1, 2, 3, 42]
            getValue() >> 100
        }
        def pattern2 = Mock(ItemsListWithSupport) {
            getKey() >> [3, 4, 42]
            getValue() >> 10
        }
        def pattern3 = Mock(ItemsListWithSupport) {
            getKey() >> [5, 42]
            getValue() >> 200
        }

        def inputPattern = KV.of(productId, [pattern1, pattern2, pattern3])

        when:
        def result = fn.apply(inputPattern)

        then:
        result.key == productId
        result.value.patterns[0].key == [5, 42]
        result.value.patterns[0].value == 200
        result.value.patterns[1].key == [1, 2, 3, 42]
        result.value.patterns[1].value == 100
    }
}
