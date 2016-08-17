package com.stewel.dataflow;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.BlockingDataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.*;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.stewel.dataflow.fpgrowth.CountDescendingPairComparator;
import org.apache.commons.lang.mutable.MutableLong;

import java.util.*;


public class ParallelFPGrowth {

    public static final int NUMBER_OF_GROUPS = 2;

    public static final String PROJECT_ID = "rewe-148055";
    public static final String STAGING_BUCKET_LOCATION = "gs://stephan-dataflow-bucket/staging/";
    public static final String INPUT_BUCKET_LOCATION = "gs://stephan-dataflow-bucket/input/*";

    public static void main(final String... args) {
        // Read options from the command-line, check for required command-line arguments and validate argument values.
        final PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().create();
        // Create the Pipeline with the specified options.
        final Pipeline pipeline = Pipeline.create(options);

        final PCollectionView<Map<Integer, Long>> frequentItemsWithFrequency = pipeline
                .apply("readTransactionsInputFile", TextIO.Read.from(INPUT_BUCKET_LOCATION))
                .apply("extractProductId_TransactionIdPairs", MapElements.via(new ProductIdAndTransactionIdPairsFn()))
                .apply("countFrequencies", Count.<Integer, String>perKey())
                .apply("filterForMinimumSupport", filterForMinimumSupport())
                .apply("supplyAsView", View.<Integer, Long>asMap());

        pipeline.apply("readTransactionsInputFile", TextIO.Read.from(INPUT_BUCKET_LOCATION))
                .apply("extractTransactionId_ProductIdPairs", MapElements.via(new TransactionIdAndProductIdPairsFn()))
                .apply("assembleTransactions", GroupByKey.create())
                .apply("sortTransactionsBySupport", sortTransactionsBySupport(frequentItemsWithFrequency))
                .apply("generateGroupDependentTransactions", generateGroupDependentTransactions())
                .apply("groupByGroupId", GroupByKey.create())
                .apply("generateFPTrees", ParDo.of(new DoFn<KV<Integer, Iterable<TransactionTree>>, String>() {
                    @Override
                    public void processElement(ProcessContext c) throws Exception {
                        final int groupId = c.element().getKey();
                        final Iterable<TransactionTree> transactionTrees = c.element().getValue();

                        TransactionTree cTree = new TransactionTree();
                        for (TransactionTree tr : transactionTrees) {
                            for (ItemsListWithSupport p : tr) {
                                cTree.addPattern(p.getKey(), p.getValue());
                            }
                        }
                        System.out.println(cTree.toString());


                        List<Pair<Integer, Long>> localFList = new ArrayList<>();
                        for (Map.Entry<Integer, MutableLong> fItem : cTree.generateFList().entrySet()) {
                            localFList.add(new Pair<>(fItem.getKey(), fItem.getValue().toLong()));
                        }

                        Collections.sort(localFList, new CountDescendingPairComparator<>());

                        System.out.println(localFList);
                    }
                }));


        // for each transaction and each group output a KV <groupId, filteredTransaction>
        // combine PER KEY all transactions into local fp-trees
        // aggregate local fp-trees

        pipeline.run();

    }

    private static ParDo.Bound<List<Integer>, KV<Integer, TransactionTree>> generateGroupDependentTransactions() {
        return ParDo.of(new DoFn<List<Integer>, KV<Integer, TransactionTree>>() {
            @Override
            public void processElement(ProcessContext c) throws Exception {
                List<Integer> transaction = c.element();
                Set<Integer> groups = new HashSet<>();
                for (int j = transaction.size() - 1; j >= 0; j--) {
                    int productId = transaction.get(j);
                    int groupId = getGroupId(productId);

                    if (!groups.contains(groupId)) {
                        ArrayList<Integer> groupDependentTransaction = new ArrayList<Integer>(transaction.subList(0, j + 1));
                        final KV<Integer, TransactionTree> output = KV.of(groupId, new TransactionTree(groupDependentTransaction, 1L));
//                        final GroupAndTransactionTree output = new GroupAndTransactionTree(groupId, new TransactionTree(groupDependentTransaction, 1L));
                        System.out.println(output);
                        c.output(output);
                    }
                    groups.add(groupId);
                }
            }
        });
    }

    private static void runOnDataflowService(DataflowPipelineOptions options) {
        options.setRunner(BlockingDataflowPipelineRunner.class);
        options.setStagingLocation(STAGING_BUCKET_LOCATION);
        options.setProject(PROJECT_ID);
    }

    private static ParDo.Bound<KV<String, Iterable<Integer>>, List<Integer>> sortTransactionsBySupport(final PCollectionView<Map<Integer, Long>> frequentItemsWithFrequency) {
        return ParDo.withSideInputs(frequentItemsWithFrequency).of(new DoFn<KV<String, Iterable<Integer>>, List<Integer>>() {
            @Override
            public void processElement(ProcessContext c) {
                final Map<Integer, Long> productFrequencies = c.sideInput(frequentItemsWithFrequency);
                final Comparator<Integer> productFrequencyComparator = (o1, o2) -> Long.compare(productFrequencies.get(o1), productFrequencies.get(o2));
                final List<Integer> productsInTransaction = new ArrayList<>();
                c.element().getValue().iterator().forEachRemaining(productsInTransaction::add);
                productsInTransaction.sort(productFrequencyComparator.reversed());
                System.out.println(productsInTransaction);
                c.output(productsInTransaction);
            }
        });
    }

    private static ParDo.Bound<KV<Integer, Long>, KV<Integer, Long>> filterForMinimumSupport() {
        return ParDo.of(new DoFn<KV<Integer, Long>, KV<Integer, Long>>() {
            @Override
            public void processElement(ProcessContext c) throws Exception {
                System.out.println("Item " + c.element().getKey() + " | Support " + c.element().getValue());
                if (c.element().getValue() > 0) {
                    c.output(c.element());
                } else {
                }

            }
        });
    }


    private static int getGroupId(Integer productId) {
        return productId % NUMBER_OF_GROUPS;
    }
}
