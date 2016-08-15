package com.stewel.dataflow;

import com.google.cloud.dataflow.sdk.transforms.SimpleFunction;
import com.google.cloud.dataflow.sdk.values.KV;

public class TransactionIdAndProductIdPairsFn extends SimpleFunction<String, KV<String, Integer>> {
    @Override
    public KV<String, Integer> apply(String string) {
        final String transactionId = string.split(",")[0];
        final String productId = string.replaceAll(".*\\(", "").replaceAll("\\).*", "");
        return KV.of(transactionId, Integer.parseInt(productId));
    }
}
