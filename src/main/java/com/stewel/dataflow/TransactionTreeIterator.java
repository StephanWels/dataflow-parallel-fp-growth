package com.stewel.dataflow;

import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.coders.DefaultCoder;
import com.google.common.collect.AbstractIterator;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Stack;

@DefaultCoder(AvroCoder.class)
final class TransactionTreeIterator extends AbstractIterator<ItemsListWithSupport> implements Serializable {

    private final Stack<int[]> depth = new Stack<int[]>();
    private final TransactionTree transactionTree;

    TransactionTreeIterator(TransactionTree transactionTree) {
        this.transactionTree = transactionTree;
        depth.push(new int[] {0, -1});
    }

    @Override
    protected ItemsListWithSupport computeNext() {
        if (depth.isEmpty()) {
            return endOfData();
        }
        long sum;
        int childId;
        do {
            int[] top = depth.peek();
            while (top[1] + 1 == transactionTree.childCount(top[0])) {
                depth.pop();
                top = depth.peek();
            }
            if (depth.isEmpty()) {
                return endOfData();
            }
            top[1]++;
            childId = transactionTree.childAtIndex(top[0], top[1]);
            depth.push(new int[] {childId, -1});

            sum = 0;
            for (int i = transactionTree.childCount(childId) - 1; i >= 0; i--) {
                sum += transactionTree.count(transactionTree.childAtIndex(childId, i));
            }
        } while (sum == transactionTree.count(childId));
        ArrayList<Integer> data = new ArrayList<>();
        Iterator<int[]> it = depth.iterator();
        it.next();
        while (it.hasNext()) {
            data.add(transactionTree.attribute(it.next()[0]));
        }
        ItemsListWithSupport returnable = new ItemsListWithSupport(toIntArray(data), transactionTree.count(childId) - sum);
        int[] top = depth.peek();
        while (top[1] + 1 == transactionTree.childCount(top[0])) {
            depth.pop();
            if (depth.isEmpty()) {
                break;
            }
            top = depth.peek();
        }
        return returnable;
    }

    private static int[] toIntArray(List<Integer> list){
        int[] ret = new int[list.size()];
        for(int i = 0;i < ret.length;i++)
            ret[i] = list.get(i);
        return ret;
    }

}
