package com.stewel.dataflow.functions;

import com.google.cloud.dataflow.sdk.transforms.SimpleFunction;
import com.stewel.dataflow.ItemsListWithSupport;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.Collections;

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
        final ArrayList<Integer> itemset = itemsListWithSupport.getKey();
        Collections.sort(itemset);
        final byte[] rowId = itemset.toString().getBytes();
        final byte[] supportValue = Bytes.toBytes(support);
        return new Put(rowId, System.currentTimeMillis()).addColumn(bigTableFamily, bigTableQualifier, supportValue);
    }
}
