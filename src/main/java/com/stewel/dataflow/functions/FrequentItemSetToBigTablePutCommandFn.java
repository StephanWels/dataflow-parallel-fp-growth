package com.stewel.dataflow.functions;

import com.google.cloud.dataflow.sdk.transforms.SimpleFunction;
import com.stewel.dataflow.ItemsListWithSupport;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Arrays;

public class FrequentItemSetToBigTablePutCommandFn extends SimpleFunction<ItemsListWithSupport, Mutation> {
    private final byte[] bigTableFamily;
    private final byte[] bigTableQualifier;

    public FrequentItemSetToBigTablePutCommandFn(final String family, final String qualifier) {
        bigTableFamily = family.getBytes();
        bigTableQualifier = qualifier.getBytes();
    }

    @Override
    public Mutation apply(ItemsListWithSupport itemsListWithSupport) {
        final Long support = itemsListWithSupport.getValue();
        final int[] itemset = itemsListWithSupport.getKey();
        Arrays.sort(itemset);
        final byte[] rowId = Arrays.toString(itemset).getBytes();
        final byte[] supportValue = Bytes.toBytes(support);
        return new Put(rowId, System.currentTimeMillis()).addColumn(bigTableFamily, bigTableQualifier, supportValue);
    }
}
