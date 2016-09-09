package com.stewel.dataflow.functions

import com.google.cloud.dataflow.sdk.transforms.DoFn
import com.google.cloud.dataflow.sdk.values.KV
import com.stewel.dataflow.TransactionTree
import spock.lang.Specification
import spock.lang.Subject

class GenerateGroupDependentTransactionsDoFnSpec extends Specification {

    def numberOfGroups = 3;

    @Subject
    GenerateGroupDependentTransactionsDoFn doFn = new GenerateGroupDependentTransactionsDoFn(numberOfGroups);

    def "transactions are generated for three groups"() {
        given:
        def transaction = [1, 3, 2, 4]

        DoFn.ProcessContext context = Mock(DoFn.ProcessContext) {
            element() >> transaction
        }

        when:
        doFn.processElement(context)

        then:
        1 * context.output(_) >> { args ->
            KV kv = (KV) args[0]
            assert kv.key == 1
            def groupDependentransaction = ((TransactionTree)kv.value).iterator().next().key
            assert groupDependentransaction == [1, 3, 2, 4]
        }
        1 * context.output(_) >> { args ->
            KV kv = (KV) args[0]
            assert kv.key == 2
            def groupDependentransaction = ((TransactionTree)kv.value).iterator().next().key
            assert groupDependentransaction == [1, 3, 2]
        }
        1 * context.output(_) >> { args ->
            KV kv = (KV) args[0]
            assert kv.key == 0
            def groupDependentransaction = ((TransactionTree)kv.value).iterator().next().key
            assert groupDependentransaction == [1, 3]
        }
    }
}
