package com.stewel.dataflow;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.BlockingDataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.Count;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.GroupByKey;
import com.google.cloud.dataflow.sdk.transforms.MapElements;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.View;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.stewel.dataflow.fpgrowth.AlgoFPGrowth;
import com.stewel.dataflow.fpgrowth.FPTreeConverter;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;


public class ParallelFPGrowth {

    public static final int NUMBER_OF_GROUPS = 1;

    public static final String PROJECT_ID = "rewe-148055";
    public static final String STAGING_BUCKET_LOCATION = "gs://stephan-dataflow-bucket/staging/";
    public static final String INPUT_BUCKET_LOCATION = "gs://stephan-dataflow-bucket/input/*";
    public static final int MINIMUM_SUPPORT = 3;
    public static final int DEFAULT_HEAP_SIZE = 50;

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
                .apply("generateFPTrees", ParDo.withSideInputs(frequentItemsWithFrequency).of(new DoFn<KV<Integer, Iterable<TransactionTree>>, ItemsListWithSupport>() {
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

                        final AlgoFPGrowth algoFPGrowth = new AlgoFPGrowth();
                        AtomicInteger transactionCount = new AtomicInteger();

                        final Map<Integer, Integer> productFrequencies = c.sideInput(frequentItemsWithFrequency).entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, k -> k.getValue().intValue()));

                        cTree.iterator()
                                .forEachRemaining(itemsListWithSupport -> {
                                    transactionCount.incrementAndGet();
                                });


                        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(System.out));
                        algoFPGrowth.fpgrowth(FPTreeConverter.convertToSPMFModel(cTree, productFrequencies), new int[0], transactionCount.get(), productFrequencies, writer, c);
                        writer.flush();
                    }
                }))
                .apply("mapForAggregating", ParDo.of(new DoFn<ItemsListWithSupport, KV<Integer, ItemsListWithSupport>>() {

                    @Override
                    public void processElement(ProcessContext c) throws Exception {
                        ItemsListWithSupport patterns = c.element();
                        for (Integer item : patterns.getKey()) {
                            c.output(KV.of(item, patterns));
                        }
                    }
                }))
                .apply("groupByGroupId", GroupByKey.create())
                .apply("reduceForAggregating", ParDo.of(new DoFn<KV<Integer, Iterable<ItemsListWithSupport>>, KV<Integer, TopKStringPatterns>>() {

                    @Override
                    public void processElement(ProcessContext c) throws Exception {
                        System.out.println("input=" + c.element());
                        TopKStringPatterns topKStringPatterns = new TopKStringPatterns();
                        for(final ItemsListWithSupport pattern : c.element().getValue()) {
                            topKStringPatterns = topKStringPatterns.merge(new TopKStringPatterns(Collections.singletonList(new ItemsListWithSupport(pattern.getKey(), pattern.getValue()))), DEFAULT_HEAP_SIZE);
                        }
                        System.out.println("merged=" + topKStringPatterns);
                        c.output(KV.of(c.element().getKey(), topKStringPatterns));
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
                final Comparator<Integer> productFrequencyComparator = (o1, o2) -> Long.compare(productFrequencies.getOrDefault(o1, 0L), productFrequencies.getOrDefault(o2, 0L));
                List<Integer> productsInTransaction = new ArrayList<>();
                c.element().getValue().iterator().forEachRemaining(productsInTransaction::add);
                productsInTransaction.sort(productFrequencyComparator.reversed());
                productsInTransaction = productsInTransaction.stream().filter(productFrequencies::containsKey).collect(Collectors.toList());
                System.out.println(productsInTransaction);
                c.output(productsInTransaction);
            }
        });
    }

    private static ParDo.Bound<KV<Integer, Long>, KV<Integer, Long>> filterForMinimumSupport() {
        return ParDo.of(new DoFn<KV<Integer, Long>, KV<Integer, Long>>() {
            @Override
            public void processElement(ProcessContext c) throws Exception {
                if (c.element().getValue() >= MINIMUM_SUPPORT) {
                    System.out.println("Item " + c.element().getKey() + " | Support " + c.element().getValue());
                    c.output(c.element());
                } else {
                    System.out.println("Item " + c.element().getKey() + " | Support " + c.element().getValue() + " NOT FREQUENT.");
                }

            }
        });
    }


    private static int getGroupId(Integer productId) {
        return productId % NUMBER_OF_GROUPS;
    }
}
