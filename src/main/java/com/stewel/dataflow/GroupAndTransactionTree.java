package com.stewel.dataflow;

import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.coders.DefaultCoder;

import java.io.Serializable;

@DefaultCoder(AvroCoder.class)
public final class GroupAndTransactionTree extends Pair<Integer, TransactionTree> implements Serializable {

    public GroupAndTransactionTree(final Integer group, final TransactionTree transactionTree) {
        super(group, transactionTree);
    }
}
