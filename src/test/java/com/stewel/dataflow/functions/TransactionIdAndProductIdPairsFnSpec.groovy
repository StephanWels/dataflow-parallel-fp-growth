package com.stewel.dataflow.functions

import com.google.cloud.dataflow.sdk.values.KV
import spock.lang.Specification
import spock.lang.Subject

class TransactionIdAndProductIdPairsFnSpec extends Specification {

    @Subject
    TransactionIdAndProductIdPairsFn fn = new TransactionIdAndProductIdPairsFn()

    def 'extracts category id and name'() {
        expect:
        fn.apply(input) == output

        where:
        input                                | output
        '1002102233,Apfel(2074)'             | KV.of('1002102233', 2074)
        '1002102232,Apfel, Birne & Co(2073)' | KV.of('1002102232', 2073)
    }
}
