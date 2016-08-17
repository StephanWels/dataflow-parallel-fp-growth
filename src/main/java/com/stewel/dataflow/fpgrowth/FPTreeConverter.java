package com.stewel.dataflow.fpgrowth;

import com.stewel.dataflow.ItemsListWithSupport;
import com.stewel.dataflow.TransactionTree;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class FPTreeConverter {

    public static FPTree convertToSPMFModel(TransactionTree transactionTree, Map<Integer, Integer> supportMap) {
        final FPTree fpTree = new FPTree();
        final Iterator<ItemsListWithSupport> iterator = transactionTree.getCompressedTree().iterator();
        iterator.forEachRemaining(itemsListWithSupport -> fpTree.addTransaction(itemsListWithSupport.getKey()));
        fpTree.createHeaderList(supportMap);
        return fpTree;
    }
}
