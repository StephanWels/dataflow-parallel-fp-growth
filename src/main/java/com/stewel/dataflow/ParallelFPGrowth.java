package com.stewel.dataflow;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.BlockingDataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.*;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollectionView;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;


public class ParallelFPGrowth {

    public static final String PROJECT_ID = "rewe-148055";
    public static final String STAGING_BUCKET_LOCATION = "gs://stephan-dataflow-bucket/staging/";
    public static final String INPUT_BUCKET_LOCATION = "gs://stephan-dataflow-bucket/input/*";

    public static void main(String... args) {

        DataflowPipelineOptions options = PipelineOptionsFactory.create().as(DataflowPipelineOptions.class);
        runOnDataflowService(options);
        Pipeline pipeline = Pipeline.create(options);

        final PCollectionView<Map<Integer, Long>> frequentItemsWithFrequency = pipeline
                .apply("readTransactionsInputFile", TextIO.Read.from(INPUT_BUCKET_LOCATION))
                .apply("extractProductId_TransactionIdPairs", transformToProductIdAndTransactionIdPairs())
                .apply("countFrequencies", Count.<Integer, String>perKey())
                .apply("filterForMinimumSupport", filterForMinimumSupport())
                .apply("supplyAsView", View.<Integer, Long>asMap());

        pipeline.apply("readTransactionsInputFile", TextIO.Read.from(INPUT_BUCKET_LOCATION))
                .apply("extractTransactionId_ProductIdPairs", transformToTransactionIdAndProductIdPairs())
                .apply("assembleTransactions", GroupByKey.<String, Integer>create())
                .apply("sortTransactionsBySupport", sortTransactionsBySupport(frequentItemsWithFrequency));

                // for each transaction and each group output a KV <groupId, filteredTransaction>
                // combine PER KEY all transactions into local fp-trees
                // aggregate local fp-trees

        pipeline.run();

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

    private static ParDo.Bound<String, KV<Integer, String>> transformToProductIdAndTransactionIdPairs() {
        return ParDo.of(new DoFn<String, KV<Integer, String>>() {
            @Override
            public void processElement(ProcessContext c) throws Exception {
                System.out.println(c.element());
                final String transactionId = c.element().split(",")[0];
                final String productId = c.element().replaceAll(".*\\(", "").replaceAll("\\).*", "");
                System.out.println(transactionId + " | " + productId);
                c.output(KV.of(Integer.parseInt(productId), transactionId));
            }
        });
    }

    private static ParDo.Bound<String, KV<String, Integer>> transformToTransactionIdAndProductIdPairs() {
        return ParDo.of(new DoFn<String, KV<String, Integer>>() {
            @Override
            public void processElement(ProcessContext c) throws Exception {
                final String transactionId = c.element().split(",")[0];
                final String productId = c.element().replaceAll(".*\\(", "").replaceAll("\\).*", "");
                c.output(KV.of(transactionId, Integer.parseInt(productId)));
            }
        });
    }
}
