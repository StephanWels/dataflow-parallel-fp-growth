package com.stewel.dataflow.assocrules;

import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class SupportRepository implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(SupportRepository.class);

    protected static final String PROJECT_ID = "rewe-148055";
    protected static final String BIGTABLE_INSTANCE_ID = "parallel-fpgrowth-itemsets";
    protected static final String BIGTABLE_TABLE_ID = "itemsets";
    protected static final byte[] BIG_TABLE_FAMILY = "support".getBytes();
    protected static final byte[] BIG_TABLE_QUALIFIER = "item".getBytes();
    protected static final byte[] BIG_TABLE_QUALIFIER_PATTERN = "pattern".getBytes();

    private final Connection connection;
    private Map<List<Integer>, Long> superPerformantCache = new HashMap<List<Integer>, Long>();

    private static SupportRepository instance = new SupportRepository();

    public SupportRepository() {
        connection = BigtableConfiguration.connect(PROJECT_ID, BIGTABLE_INSTANCE_ID);
    }

    public static SupportRepository getInstance() {
        return instance;
    }

    public long getSupport(int[] pattern) {
        return getSupport(Arrays.stream(pattern).mapToObj(Integer::valueOf).collect(Collectors.toList()));
    }

    public long getSupport(List<Integer> pattern) {
        Collections.sort(pattern);
        if (superPerformantCache.containsKey(pattern)) {
            return superPerformantCache.get(pattern);
        }
        try (Table table = connection.getTable(TableName.valueOf(BIGTABLE_TABLE_ID))) {
            byte[] rowKey = pattern.toString().getBytes();

            final long support = Bytes.toLong(table.get(new Get(rowKey)).getValue(BIG_TABLE_FAMILY, BIG_TABLE_QUALIFIER_PATTERN));
            superPerformantCache.put(pattern, support);
            LOGGER.info("Support " + support + ", " + pattern);
            return support;
        } catch (IOException exception) {
            LOGGER.error("ERROR WITH BIGTABLE", exception);
        }
        return 0;
    }

    @Override
    public void close() throws Exception {
        if (connection != null) {
            connection.close();
        }
    }
}
