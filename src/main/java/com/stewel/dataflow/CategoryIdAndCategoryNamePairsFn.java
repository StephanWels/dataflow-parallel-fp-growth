package com.stewel.dataflow;

import com.google.cloud.dataflow.sdk.transforms.SimpleFunction;
import com.google.cloud.dataflow.sdk.values.KV;

public class CategoryIdAndCategoryNamePairsFn extends SimpleFunction<String, KV<Integer, String>> {
    @Override
    public KV<Integer, String> apply(final String string) {
        final String categoryId = string.replaceAll(".*\\(", "").replaceAll("\\).*", "");
        final String categoryName = string.replaceFirst("\\d*\\,", "").replaceAll("\\(.*", "");
        return KV.of(Integer.parseInt(categoryId), categoryName);
    }
}
