package com.stewel.dataflow.functions;

import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.values.KV;
import com.stewel.dataflow.ItemsListWithSupport;
import com.stewel.dataflow.TopKStringPatterns;

public class ExpandTopKStringPatternsToAllSubPatternsDoFn extends DoFn<KV<Integer, TopKStringPatterns>, KV<Integer, ItemsListWithSupport>> {
    @Override
    public void processElement(ProcessContext c) throws Exception {
        final Integer productId = c.element().getKey();
        TopKStringPatterns patterns = c.element().getValue();
        patterns.getPatterns().forEach(pattern -> c.output(KV.of(productId, pattern)));
    }
}
