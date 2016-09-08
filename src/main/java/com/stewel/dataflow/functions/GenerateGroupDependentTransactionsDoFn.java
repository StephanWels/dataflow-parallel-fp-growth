package com.stewel.dataflow.functions;

import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.values.KV;
import com.stewel.dataflow.TransactionTree;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class GenerateGroupDependentTransactionsDoFn extends DoFn<List<Integer>, KV<Integer, TransactionTree>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(GenerateGroupDependentTransactionsDoFn.class);
    private final int numberOfGroups;

    public GenerateGroupDependentTransactionsDoFn(final int numberOfGroups) {
        this.numberOfGroups = numberOfGroups;
    }

    @Override
    public void processElement(ProcessContext c) throws Exception {
        List<Integer> transaction = c.element();
        Set<Integer> groups = new HashSet<>();
        for (int j = transaction.size() - 1; j >= 0; j--) {
            int productId = transaction.get(j);
            int groupId = getGroupId(productId);

            if (!groups.contains(groupId)) {
                ArrayList<Integer> groupDependentTransaction = new ArrayList<>(transaction.subList(0, j + 1));
                final KV<Integer, TransactionTree> output = KV.of(groupId, new TransactionTree(groupDependentTransaction, 1L));
                c.output(output);
            }
            groups.add(groupId);
        }
    }

    //TODO: distribute groupIds according to F-List
    private int getGroupId(Integer productId) {
        return productId % numberOfGroups;
    }
}
