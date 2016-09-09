package com.stewel.dataflow;

import com.google.cloud.bigtable.dataflow.CloudBigtableIO;
import com.google.cloud.bigtable.dataflow.CloudBigtableScanConfiguration;
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
import com.google.cloud.dataflow.sdk.transforms.RemoveDuplicates;
import com.google.cloud.dataflow.sdk.transforms.View;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.stewel.dataflow.assocrules.AlgoAgrawalFaster94;
import com.stewel.dataflow.assocrules.AssociationRule;
import com.stewel.dataflow.assocrules.AssociationRuleFormatter;
import com.stewel.dataflow.assocrules.AssociationRules;
import com.stewel.dataflow.assocrules.SupportRepository;
import com.stewel.dataflow.fpgrowth.AlgoFPGrowth;
import com.stewel.dataflow.fpgrowth.FPTreeConverter;
import com.stewel.dataflow.fpgrowth.ImmutableItemset;
import com.stewel.dataflow.fpgrowth.Itemsets;
import com.stewel.dataflow.functions.CategoryIdAndCategoryNamePairsFn;
import com.stewel.dataflow.functions.FrequentItemSetToBigTablePutCommandFn;
import com.stewel.dataflow.functions.GenerateGroupDependentTransactionsDoFn;
import com.stewel.dataflow.functions.ProductIdAndTransactionIdPairsFn;
import com.stewel.dataflow.functions.SelectTopKPatternFn;
import com.stewel.dataflow.functions.TransactionIdAndProductIdPairsFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;


public class ParallelFPGrowth {

    private static final Logger LOGGER = LoggerFactory.getLogger(ParallelFPGrowth.class);

    public static final int NUMBER_OF_GROUPS = 1;

    protected static final String PROJECT_ID = "rewe-148055";
    protected static final String DATASET_SIZE = "";
    protected static final double MINIMUM_SUPPORT = 0.1;
    protected static final double MINIMUM_CONFIDENCE = 0.05;
    protected static final double MINIMUM_LIFT = 0.0;
    protected static final int DEFAULT_HEAP_SIZE = 50;
    protected static final String STAGING_BUCKET_LOCATION = "gs://stephan-dataflow-bucket/staging/";
    protected static final String INPUT_BUCKET_LOCATION = "gs://stephan-dataflow-bucket/input" + DATASET_SIZE + "/*";
    protected static final String OUTPUT_BUCKET_LOCATION = "gs://stephan-dataflow-bucket/output" + DATASET_SIZE + "_groups-" + NUMBER_OF_GROUPS + "_minSupport-" + MINIMUM_SUPPORT + "/*";
    protected static final String BIGTABLE_INSTANCE_ID = "parallel-fpgrowth-itemsets";
    protected static final String BIGTABLE_TABLE_ID = "itemsets";
    protected static final String BIG_TABLE_FAMILY = "support";
    protected static final String BIG_TABLE_QUALIFIER_PATTERN = "pattern";

    public static void main(final String... args) {
        CloudBigtableScanConfiguration bigtableScanConfiguration = new CloudBigtableScanConfiguration.Builder()
                .withProjectId(PROJECT_ID)
                .withInstanceId(BIGTABLE_INSTANCE_ID)
                .withTableId(BIGTABLE_TABLE_ID)
                .build();

        // Read options from the command-line, check for required command-line arguments and validate argument values.
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().create();
//        options = runOnDataflowService(options);
        // Create the Pipeline with the specified options.
        final Pipeline pipeline = Pipeline.create(options);

        CloudBigtableIO.initializeForWrite(pipeline);

        final PCollection<KV<String, Iterable<Integer>>> assembledTransactions = pipeline.apply("readTransactionsInputFile", TextIO.Read.from(INPUT_BUCKET_LOCATION))
                .apply("extractTransactionId_ProductIdPairs", MapElements.via(new TransactionIdAndProductIdPairsFn()))
                .apply("assembleTransactions", GroupByKey.create());

        final PCollectionView<Map<Integer, String>> categoryNames = pipeline.apply("readTransactionsInputFile", TextIO.Read.from(INPUT_BUCKET_LOCATION))
                .apply("extractTransactionId_ProductIdPairs", MapElements.via(new CategoryIdAndCategoryNamePairsFn()))
                .apply("unique", RemoveDuplicates.create())
                .apply("supplyAsView", View.asMap());

        final PCollectionView<Long> transactionCount = assembledTransactions.apply("countTransactions", Count.globally()).apply("supplyAsView", View.asSingleton());

        final PCollection<KV<Integer, Long>> frequentItems = pipeline
                .apply("readTransactionsInputFile", TextIO.Read.from(INPUT_BUCKET_LOCATION))
                .apply("extractProductId_TransactionIdPairs", MapElements.via(new ProductIdAndTransactionIdPairsFn()))
                .apply("countFrequencies", Count.<Integer, String>perKey())
                .apply("filterForMinimumSupport", filterForMinimumSupport(transactionCount));

        final PCollectionView<Map<Integer, Long>> frequentItemsWithFrequency = frequentItems.apply("supplyAsView", View.<Integer, Long>asMap());

//        frequentItems.apply(ApproximateQuantiles.ApproximateQuantilesCombineFn.create())

        final PCollection<ItemsListWithSupport> frequentItemSetsWithSupport = assembledTransactions
                .apply("sortTransactionsBySupport", sortTransactionsBySupport(frequentItemsWithFrequency))
                .apply("generateGroupDependentTransactions", ParDo.of(new GenerateGroupDependentTransactionsDoFn(NUMBER_OF_GROUPS)))
                .apply("groupByGroupId", GroupByKey.create())
                .apply("generateFPTrees", generateFPTrees(frequentItemsWithFrequency, transactionCount));

        frequentItemSetsWithSupport.apply("transformToBigTablePutCommands", MapElements.via(new FrequentItemSetToBigTablePutCommandFn(BIG_TABLE_FAMILY, BIG_TABLE_QUALIFIER_PATTERN)))
                .apply(CloudBigtableIO.writeToTable(bigtableScanConfiguration));


        frequentItemSetsWithSupport.apply("output_<productId,Pattern>_ForEachContainingProduct", outputThePatternForEachContainingProduct())
                .apply("groupByProductId", GroupByKey.create())
                .apply("selectTopKPattern", MapElements.via(new SelectTopKPatternFn(DEFAULT_HEAP_SIZE)))
                .apply("expandToAllSubPatterns", expandTopKStringPatternsToAllSubPatterns())
                .apply("groupByProductId", GroupByKey.create())
                .apply("extractAssociationRules", extractAssociationRules(transactionCount, categoryNames))
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

    private static ParDo.Bound<KV<Integer, Iterable<ItemsListWithSupport>>, String> extractAssociationRules(final PCollectionView<Long> transactionCount, PCollectionView<Map<Integer, String>> categoryNames) {
        return ParDo.withSideInputs(transactionCount, categoryNames).of(new DoFn<KV<Integer, Iterable<ItemsListWithSupport>>, String>() {

            @Override
            public void processElement(ProcessContext c) throws Exception {
                final Itemsets patterns = new Itemsets("TOP K PATTERNS");

                c.element().getValue().forEach(itemsListWithSupport -> {
                    patterns.addItemset(ImmutableItemset.builder()
                            .items(itemsListWithSupport.getKey())
                            .absoluteSupport(itemsListWithSupport.getValue())
                            .build());
                });
                final AlgoAgrawalFaster94 associationRulesExtractionAlgorithm = new AlgoAgrawalFaster94(SupportRepository.getInstance());

                long numberTransactions = c.sideInput(transactionCount);
                // an dieser stelle fehlen (sub-)patterns, um die confidence zu berechnen
                final AssociationRules associationRules = associationRulesExtractionAlgorithm.runAlgorithm(patterns, null, numberTransactions, MINIMUM_CONFIDENCE, MINIMUM_LIFT);

                final int productId = c.element().getKey();

                final StringBuilder productAssociationRulesResult = new StringBuilder("Item " + c.element().getKey() + "\n");

                final Map<Integer, String> categoryTranslations = c.sideInput(categoryNames);
                AssociationRuleFormatter ruleFormatter = new AssociationRuleFormatter(categoryTranslations);
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

    private static DataflowPipelineOptions runOnDataflowService(PipelineOptions options) {
        DataflowPipelineOptions dataflowOptions = options.as(DataflowPipelineOptions.class);
        dataflowOptions.setRunner(BlockingDataflowPipelineRunner.class);
        dataflowOptions.setStagingLocation(STAGING_BUCKET_LOCATION);
        dataflowOptions.setProject(PROJECT_ID);
        return dataflowOptions;
    }
}
