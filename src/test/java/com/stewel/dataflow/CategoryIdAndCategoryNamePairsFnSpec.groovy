package com.stewel.dataflow

import com.google.cloud.dataflow.sdk.values.KV
import spock.lang.Specification
import spock.lang.Subject

class CategoryIdAndCategoryNamePairsFnSpec extends Specification {

    @Subject
    CategoryIdAndCategoryNamePairsFn fn = new CategoryIdAndCategoryNamePairsFn()

    def 'extracts category id and name'() {
        expect:
        fn.apply(input) == output

        where:
        input                                | output
        '1002102233,Apfel(2074)'             | KV.of(2074, 'Apfel')
        '1002102233,Apfel, Birne & Co(2074)' | KV.of(2074, 'Apfel, Birne & Co')
    }
}
