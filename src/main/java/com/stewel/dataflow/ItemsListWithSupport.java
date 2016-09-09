package com.stewel.dataflow;

import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.coders.DefaultCoder;

import java.io.Serializable;

@DefaultCoder(AvroCoder.class)
public class ItemsListWithSupport extends Pair<int[], Long> implements Serializable {

    public ItemsListWithSupport(final int[] frequentItemsList, final long support) {
        super(frequentItemsList, support);
    }
}
