package com.stewel.dataflow.functions

import com.google.cloud.dataflow.sdk.transforms.DoFn
import com.google.cloud.dataflow.sdk.values.KV
import com.stewel.dataflow.ItemsListWithSupport
import com.stewel.dataflow.TransactionTree
import spock.lang.Specification
import spock.lang.Subject

class OutputPatternForEachContainingProductIdDoFnSpec extends Specification {

    @Subject
    OutputPatternForEachContainingProductIdDoFn doFn = new OutputPatternForEachContainingProductIdDoFn();

    def "pattern is output for each product id contained in it"(){
        given:
        int[] transaction = [1, 3, 2, 4]
        int support = 100

        DoFn.ProcessContext context = Mock(DoFn.ProcessContext) {
            element() >> new ItemsListWithSupport(transaction, support)
        }

        when:
        doFn.processElement(context)

        then:
        1 * context.output(_) >> { args ->
            KV kv = (KV) args[0]
            assert kv.key == 1
            def outputTransaction = ((ItemsListWithSupport)kv.value).key
            assert outputTransaction == transaction
        }
        1 * context.output(_) >> { args ->
            KV kv = (KV) args[0]
            assert kv.key == 3
            def outputTransaction = ((ItemsListWithSupport)kv.value).key
            assert outputTransaction == transaction
        }
        1 * context.output(_) >> { args ->
            KV kv = (KV) args[0]
            assert kv.key == 2
            def outputTransaction = ((ItemsListWithSupport)kv.value).key
            assert outputTransaction == transaction
        }
        1 * context.output(_) >> { args ->
            KV kv = (KV) args[0]
            assert kv.key == 4
            def outputTransaction = ((ItemsListWithSupport)kv.value).key
            assert outputTransaction == transaction
        }

    }
}
