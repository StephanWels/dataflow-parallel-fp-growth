package com.stewel.dataflow;

import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.coders.DefaultCoder;

import java.io.Serializable;
import java.util.ArrayList;

@DefaultCoder(AvroCoder.class)
public class ItemsListWithSupport extends Pair<ArrayList<Integer>, Long> implements Serializable {

    public ItemsListWithSupport(final ArrayList<Integer> frequentItemsList, final Long support) {
        super(frequentItemsList, support);
    }
}
