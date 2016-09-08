package com.stewel.dataflow;

import com.google.cloud.bigtable.dataflow.CloudBigtableIO;
import com.google.cloud.bigtable.dataflow.CloudBigtableScanConfiguration;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.BlockingDataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.*;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.stewel.dataflow.assocrules.*;
import com.stewel.dataflow.fpgrowth.AlgoFPGrowth;
import com.stewel.dataflow.fpgrowth.FPTreeConverter;
import com.stewel.dataflow.fpgrowth.Itemset;
import com.stewel.dataflow.fpgrowth.Itemsets;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;


public class ParallelFPGrowth {

    private static final Logger LOGGER = LoggerFactory.getLogger(ParallelFPGrowth.class);

    public static final int NUMBER_OF_GROUPS = 10;

    protected static final String PROJECT_ID = "rewe-148055";
    protected static final String DATASET_SIZE = "-1000000";
    protected static final double MINIMUM_SUPPORT = 0.01;
    protected static final double MINIMUM_CONFIDENCE = 0.05;
    protected static final double MINIMUM_LIFT = 1.5;
    protected static final int DEFAULT_HEAP_SIZE = 50;
    protected static final String STAGING_BUCKET_LOCATION = "gs://stephan-dataflow-bucket/staging/";
    protected static final String INPUT_BUCKET_LOCATION = "gs://stephan-dataflow-bucket/input" + DATASET_SIZE + "/*";
    protected static final String OUTPUT_BUCKET_LOCATION = "gs://stephan-dataflow-bucket/output" + DATASET_SIZE + "_groups-" + NUMBER_OF_GROUPS + "_minSupport-" + MINIMUM_SUPPORT + "/*";
    protected static final String BIGTABLE_INSTANCE_ID = "parallel-fpgrowth-itemsets";
    protected static final String BIGTABLE_TABLE_ID = "itemsets";
    protected static final byte[] BIG_TABLE_FAMILY = "support".getBytes();
    protected static final byte[] BIG_TABLE_QUALIFIER_PATTERN = "pattern".getBytes();

    public static void main(final String... args) {

        CloudBigtableScanConfiguration bigtableScanConfiguration = new CloudBigtableScanConfiguration.Builder()
                .withProjectId(PROJECT_ID)
                .withInstanceId(BIGTABLE_INSTANCE_ID)
                .withTableId(BIGTABLE_TABLE_ID)
                .build();

        // Read options from the command-line, check for required command-line arguments and validate argument values.
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().create();
        options = runOnDataflowService(options);
        // Create the Pipeline with the specified options.
        final Pipeline pipeline = Pipeline.create(options);

        CloudBigtableIO.initializeForWrite(pipeline);

        final PCollection<KV<String, Iterable<Integer>>> assembledTransactions = pipeline.apply("readTransactionsInputFile", TextIO.Read.from(INPUT_BUCKET_LOCATION))
                .apply("extractTransactionId_ProductIdPairs", MapElements.via(new TransactionIdAndProductIdPairsFn()))
                .apply("assembleTransactions", GroupByKey.create());

        final PCollectionView<Long> transactionCount = assembledTransactions.apply("countTransactions", Count.globally()).apply("supplyAsView", View.asSingleton());

        final PCollection<KV<Integer, Long>> frequentItems = pipeline
                .apply("readTransactionsInputFile", TextIO.Read.from(INPUT_BUCKET_LOCATION))
                .apply("extractProductId_TransactionIdPairs", MapElements.via(new ProductIdAndTransactionIdPairsFn()))
                .apply("countFrequencies", Count.<Integer, String>perKey())
                .apply("filterForMinimumSupport", filterForMinimumSupport(transactionCount));

        final PCollectionView<Map<Integer, Long>> frequentItemsWithFrequency = frequentItems.apply("supplyAsView", View.<Integer, Long>asMap());


        final PCollection<ItemsListWithSupport> frequentItemSetsWithSupport = assembledTransactions
                .apply("sortTransactionsBySupport", sortTransactionsBySupport(frequentItemsWithFrequency))
                .apply("generateGroupDependentTransactions", generateGroupDependentTransactions())
                .apply("groupByGroupId", GroupByKey.create())
                .apply("generateFPTrees", generateFPTrees(frequentItemsWithFrequency, transactionCount));

        frequentItemSetsWithSupport.apply("transformToBigTablePutCommands", transformToBigTablePutCommands())
                .apply(CloudBigtableIO.writeToTable(bigtableScanConfiguration));


        frequentItemSetsWithSupport.apply("output_<productId,Pattern>_ForEachContainingProduct", outputThePatternForEachContainingProduct())
                .apply("groupByProductId", GroupByKey.create())
                .apply("selectTopKPattern", selectTopKPattern())
                .apply("expandToAllSubPatterns", expandTopKStringPatternsToAllSubPatterns())
                .apply("groupByProductId", GroupByKey.create())
                .apply("extractAssociationRules", extractAssociationRules(transactionCount))
                .apply("writeToFile", TextIO.Write.to(OUTPUT_BUCKET_LOCATION));

        pipeline.run();
    }

    private static ParDo.Bound<KV<Integer, TopKStringPatterns>, KV<Integer, ItemsListWithSupport>> expandTopKStringPatternsToAllSubPatterns() {
        return ParDo.of(new DoFn<KV<Integer, TopKStringPatterns>, KV<Integer, ItemsListWithSupport>>() {
            @Override
            public void processElement(ProcessContext c) throws Exception {
                final Integer productId = c.element().getKey();
                TopKStringPatterns patterns = c.element().getValue();
                patterns.getPatterns().forEach(pattern -> c.output(KV.of(productId, pattern)));
            }
        });
    }

    private static ParDo.Bound<ItemsListWithSupport, Mutation> transformToBigTablePutCommands() {
        return ParDo.of(new DoFn<ItemsListWithSupport, Mutation>() {
            @Override
            public void processElement(ProcessContext c) throws Exception {
                final Long support = c.element().getValue();
                final ItemsListWithSupport pattern = c.element();
                final ArrayList<Integer> itemset = pattern.getKey();
                Collections.sort(itemset);
                final byte[] rowId = itemset.toString().getBytes();
                final byte[] supportValue = Bytes.toBytes(support);
                Mutation putFrequentItem = new Put(rowId, System.currentTimeMillis()).addColumn(BIG_TABLE_FAMILY, BIG_TABLE_QUALIFIER_PATTERN, supportValue);
                c.output(putFrequentItem);
            }
        });
    }


    private static ParDo.Bound<KV<Integer, Iterable<ItemsListWithSupport>>, String> extractAssociationRules(final PCollectionView<Long> transactionCount) {
        return ParDo.withSideInputs(transactionCount).of(new DoFn<KV<Integer, Iterable<ItemsListWithSupport>>, String>() {

            @Override
            public void processElement(ProcessContext c) throws Exception {
                final Itemsets patterns = new Itemsets("TOP K PATTERNS");

                c.element().getValue().forEach(itemsListWithSupport -> {
                    patterns.addItemset(new Itemset(itemsListWithSupport.getKey(), itemsListWithSupport.getValue()));
                });
                final AlgoAgrawalFaster94 associationRulesExtractionAlgorithm = new AlgoAgrawalFaster94(SupportRepository.getInstance());

                long numberTransactions = c.sideInput(transactionCount);
                // an dieser stelle fehlen (sub-)patterns, um die confidence zu berechnen
                final AssociationRules associationRules = associationRulesExtractionAlgorithm.runAlgorithm(patterns, null, numberTransactions, MINIMUM_CONFIDENCE, MINIMUM_LIFT);

                final int productId = c.element().getKey();

                final StringBuilder productAssociationRulesResult = new StringBuilder("Item " + c.element().getKey() + "\n");

                RuleFormatter ruleFormatter = new RuleFormatter();
                associationRules.getRules().stream()
                        .filter(rule -> Arrays.binarySearch(rule.getAntecedent(), productId) >= 0)
                        .sorted(new Comparator<AssociationRule>() {
                            @Override
                            public int compare(AssociationRule o1, AssociationRule o2) {
                                return Double.compare(o1.getLift(), o2.getLift());
                            }
                        }.reversed())
                        .map(ruleFormatter::formatRule)
                        .forEach(rule -> productAssociationRulesResult.append(rule).append("\n"));
                c.output(productAssociationRulesResult.toString());
            }
        });
    }

    private static ParDo.Bound<KV<Integer, Iterable<ItemsListWithSupport>>, KV<Integer, TopKStringPatterns>> selectTopKPattern() {
        return ParDo.of(new DoFn<KV<Integer, Iterable<ItemsListWithSupport>>, KV<Integer, TopKStringPatterns>>() {

            @Override
            public void processElement(ProcessContext c) throws Exception {
                LOGGER.info("input=" + c.element());
                TopKStringPatterns topKStringPatterns = new TopKStringPatterns();
                for (final ItemsListWithSupport pattern : c.element().getValue()) {
                    topKStringPatterns = topKStringPatterns.merge(new TopKStringPatterns(Collections.singletonList(new ItemsListWithSupport(pattern.getKey(), pattern.getValue()))), DEFAULT_HEAP_SIZE);
                }
                LOGGER.info("merged=" + topKStringPatterns);
                c.output(KV.of(c.element().getKey(), topKStringPatterns));
            }
        });
    }

    private static ParDo.Bound<ItemsListWithSupport, KV<Integer, ItemsListWithSupport>> outputThePatternForEachContainingProduct() {
        return ParDo.of(new DoFn<ItemsListWithSupport, KV<Integer, ItemsListWithSupport>>() {

            @Override
            public void processElement(ProcessContext c) throws Exception {
                ItemsListWithSupport patterns = c.element();
                for (Integer item : patterns.getKey()) {
                    c.output(KV.of(item, patterns));
                }
            }
        });
    }

    private static ParDo.Bound<KV<Integer, Iterable<TransactionTree>>, ItemsListWithSupport> generateFPTrees(final PCollectionView<Map<Integer, Long>> frequentItemsWithFrequency, PCollectionView<Long> transactionCount) {
        return ParDo.withSideInputs(frequentItemsWithFrequency, transactionCount).of(new DoFn<KV<Integer, Iterable<TransactionTree>>, ItemsListWithSupport>() {
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
                LOGGER.info(cTree.toString());
                final long databaseSize = c.sideInput(transactionCount);
                final AlgoFPGrowth algoFPGrowth = new AlgoFPGrowth(databaseSize, MINIMUM_SUPPORT);
                AtomicInteger localFpTreeTransactionCount = new AtomicInteger();

                final Map<Integer, Integer> productFrequencies = c.sideInput(frequentItemsWithFrequency).entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, k -> k.getValue().intValue()));

                cTree.iterator()
                        .forEachRemaining(itemsListWithSupport -> {
                            localFpTreeTransactionCount.incrementAndGet();
                        });

                algoFPGrowth.fpgrowth(FPTreeConverter.convertToSPMFModel(cTree, productFrequencies), new int[0], localFpTreeTransactionCount.get(), productFrequencies, c);
                algoFPGrowth.printStats();
            }
        });
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
                        ArrayList<Integer> groupDependentTransaction = new ArrayList<>(transaction.subList(0, j + 1));
                        final KV<Integer, TransactionTree> output = KV.of(groupId, new TransactionTree(groupDependentTransaction, 1L));
                        c.output(output);
                    }
                    groups.add(groupId);
                }
            }
        });
    }

    private static DataflowPipelineOptions runOnDataflowService(PipelineOptions options) {
        DataflowPipelineOptions dataflowOptions = options.as(DataflowPipelineOptions.class);
        dataflowOptions.setRunner(BlockingDataflowPipelineRunner.class);
        dataflowOptions.setStagingLocation(STAGING_BUCKET_LOCATION);
        dataflowOptions.setProject(PROJECT_ID);
        return dataflowOptions;
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
                c.output(productsInTransaction);
            }
        });
    }

    private static ParDo.Bound<KV<Integer, Long>, KV<Integer, Long>> filterForMinimumSupport(PCollectionView<Long> transactionCount) {
        return ParDo.withSideInputs(transactionCount).of(new DoFn<KV<Integer, Long>, KV<Integer, Long>>() {
            @Override
            public void processElement(ProcessContext c) throws Exception {
                long databaseSize = c.sideInput(transactionCount);
                long minimumAbsoluteSupport = (long) Math.ceil(databaseSize * MINIMUM_SUPPORT);
                if (c.element().getValue() >= minimumAbsoluteSupport) {
                    LOGGER.trace("Item '{}' | Support '{}' FREQUENT.", c.element().getKey(), c.element().getValue());
                    c.output(c.element());
                } else {
                    LOGGER.trace("Item '{}' | Support '{}' NOT FREQUENT.", c.element().getKey(), c.element().getValue());
                }

            }
        });
    }

    //TODO: distribute groupIds according to F-List
    private static int getGroupId(Integer productId) {
        return productId % NUMBER_OF_GROUPS;
    }
}
