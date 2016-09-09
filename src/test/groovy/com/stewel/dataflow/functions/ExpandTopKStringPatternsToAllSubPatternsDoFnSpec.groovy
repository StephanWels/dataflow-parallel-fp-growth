package com.stewel.dataflow.functions

import com.google.cloud.dataflow.sdk.transforms.DoFn
import com.google.cloud.dataflow.sdk.values.KV
import com.stewel.dataflow.ItemsListWithSupport
import com.stewel.dataflow.TopKStringPatterns
import spock.lang.Specification
import spock.lang.Subject

class ExpandTopKStringPatternsToAllSubPatternsDoFnSpec extends Specification {

    @Subject
    ExpandTopKStringPatternsToAllSubPatternsDoFn doFn = new ExpandTopKStringPatternsToAllSubPatternsDoFn();

    def "all subpatterns are output"() {
        given:
        def productId = 42
        DoFn.ProcessContext context = Mock(DoFn.ProcessContext) {
            element() >> KV.of(productId, new TopKStringPatterns([new ItemsListWithSupport([1, 2] as int[], 100),
                                                                  new ItemsListWithSupport([1] as int[], 300),
                                                                  new ItemsListWithSupport([2] as int[], 500)]))
        }

        when:
        doFn.processElement(context)

        then:
        1 * context.output(_) >> { args ->
            KV kv = (KV) args[0]
            assert kv.key == productId
            def pattern = ((ItemsListWithSupport) kv.value)
            assert pattern.key == [1, 2]
            assert pattern.value == 100
        }
        1 * context.output(_) >> { args ->
            KV kv = (KV) args[0]
            assert kv.key == productId
            def pattern = ((ItemsListWithSupport) kv.value)
            assert pattern.key == [1]
            assert pattern.value == 300
        }
        1 * context.output(_) >> { args ->
            KV kv = (KV) args[0]
            assert kv.key == productId
            def pattern = ((ItemsListWithSupport) kv.value)
            assert pattern.key == [2]
            assert pattern.value == 500
        }
    }
}
