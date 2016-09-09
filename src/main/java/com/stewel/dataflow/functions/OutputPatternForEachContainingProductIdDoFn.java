package com.stewel.dataflow.functions;

import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.values.KV;
import com.stewel.dataflow.ItemsListWithSupport;

public class OutputPatternForEachContainingProductIdDoFn extends DoFn<ItemsListWithSupport, KV<Integer, ItemsListWithSupport>> {
    @Override
    public void processElement(ProcessContext c) throws Exception {
        ItemsListWithSupport patterns = c.element();
        for (Integer item : patterns.getKey()) {
            c.output(KV.of(item, patterns));
        }
    }
}
