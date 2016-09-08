package com.stewel.dataflow.functions;

import com.google.cloud.dataflow.sdk.transforms.SimpleFunction;
import com.google.cloud.dataflow.sdk.values.KV;

public class ProductIdAndTransactionIdPairsFn extends SimpleFunction<String, KV<Integer, String>> {
    @Override
    public KV<Integer, String> apply(final String string) {
        final String transactionId = string.split(",")[0];
        final String productId = string.replaceAll(".*\\(", "").replaceAll("\\).*", "");
        return KV.of(Integer.parseInt(productId), transactionId);
    }
}
