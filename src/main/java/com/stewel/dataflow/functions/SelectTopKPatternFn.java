package com.stewel.dataflow.functions;

import com.google.cloud.dataflow.sdk.transforms.SimpleFunction;
import com.google.cloud.dataflow.sdk.values.KV;
import com.stewel.dataflow.ItemsListWithSupport;
import com.stewel.dataflow.TopKStringPatterns;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;

public class SelectTopKPatternFn extends SimpleFunction<KV<Integer, Iterable<ItemsListWithSupport>>, KV<Integer, TopKStringPatterns>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(SelectTopKPatternFn.class);
    private final int k;

    public SelectTopKPatternFn(final int k){
        this.k = k;
    }

    @Override
    public KV<Integer, TopKStringPatterns> apply(KV<Integer, Iterable<ItemsListWithSupport>> input) {
        LOGGER.info("input=" + input);
        TopKStringPatterns topKStringPatterns = new TopKStringPatterns();
        final Iterable<ItemsListWithSupport> frequentItemsets = input.getValue();
        for (final ItemsListWithSupport pattern : frequentItemsets) {
            topKStringPatterns = topKStringPatterns.merge(new TopKStringPatterns(Collections.singletonList(new ItemsListWithSupport(pattern.getKey(), pattern.getValue()))), k);
        }
        LOGGER.info("merged=" + topKStringPatterns);
        return KV.of(input.getKey(), topKStringPatterns);
    }
}
