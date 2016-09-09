package com.stewel.dataflow.fpgrowth;
 
 /* This file is copyright (c) 2008-2013 Philippe Fournier-Viger
 * 
 * This file is part of the SPMF DATA MINING SOFTWARE
 * (http://www.philippe-fournier-viger.com/spmf).
 * 
 * SPMF is free software: you can redistribute it and/or modify it under the
 * terms of the GNU General Public License as published by the Free Software
 * Foundation, either version 3 of the License, or (at your option) any later
 * version.
 * 
 * SPMF is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
 * A PARTICULAR PURPOSE. See the GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License along with
 * SPMF. If not, see <http://www.gnu.org/licenses/>.
 */


import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.stewel.dataflow.ItemsListWithSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This is an implementation of the FPGROWTH algorithm (Han et al., 2004).
 * FPGrowth is described here:
 * <br/><br/>
 * <p>
 * Han, J., Pei, J., & Yin, Y. (2000, May). Mining frequent patterns without candidate generation. In ACM SIGMOD Record (Vol. 29, No. 2, pp. 1-12). ACM
 * <br/><br/>
 * <p>
 * This is an optimized version that saves the result to a file
 * or keep it into memory if no output path is provided
 * by the user to the runAlgorithm method().
 *
 * @author Philippe Fournier-Viger
 * @see FPTree
 */
public class AlgoFPGrowth {

    private static final Logger LOGGER = LoggerFactory.getLogger(AlgoFPGrowth.class);

    // for statistics
    private long startTimestamp; // start time of the latest execution
    private long endTime; // end time of the latest execution
    private int transactionCount = 0; // transaction count in the database
    private int itemsetCount; // number of freq. itemsets found

    // parameter
    public final long minimumSupport;// the relative minimum support

    DoFn.ProcessContext c;

    // The  patterns that are found
    // (if the user want to keep them into memory)
    protected Itemsets patterns = null;


    /**
     * Constructor
     */
    public AlgoFPGrowth(final long databaseSize, final double minimumRelativeSupport) {
        this.minimumSupport = (long) Math.ceil(databaseSize * minimumRelativeSupport);
    }


    /**
     * This method mines pattern from a Prefix-Tree recursively
     *
     * @param tree       The Prefix Tree
     * @param mapSupport The frequency of each item in the prefix tree.
     * @throws IOException exception if error writing the output file
     */
    public void fpgrowth(FPTree tree, int[] prefixAlpha, int prefixSupport, Map<Integer, Integer> mapSupport, DoFn.ProcessContext c) throws IOException {
        this.c = c;
        // We need to check if there is a single path in the prefix tree or not.
        if (!tree.hasMoreThanOnePath) {
            // That means that there is a single path, so we
            // add all combinations of this path, concatenated with the prefix "alpha", to the set of patterns found.
            if (tree.root.childs.isEmpty()) {
                LOGGER.debug("Test");
            }
            addAllCombinationsForPathAndPrefix(tree.root.childs.get(0), prefixAlpha); // CORRECT?

        } else { // There is more than one path
            fpgrowthMoreThanOnePath(tree, prefixAlpha, prefixSupport, mapSupport);
        }
    }

    /**
     * Mine an FP-Tree having more than one path.
     *
     * @param tree       the FP-tree
     * @param mapSupport the frequency of items in the FP-Tree
     * @throws IOException exception if error writing the output file
     */
    private void fpgrowthMoreThanOnePath(FPTree tree, int[] prefixAlpha, int prefixSupport, Map<Integer, Integer> mapSupport) throws IOException {
        // For each frequent item in the header table list of the tree in reverse order.
        for (int i = tree.headerList.size() - 1; i >= 0; i--) {
            // get the item
            Integer item = tree.headerList.get(i);

            // get the support of the item
            int support = mapSupport.get(item);
            // if the item is not frequent, we skip it
            if (support < minimumSupport) {
                continue;
            }
            // Create Beta by concatening Alpha with the current item
            // and add it to the list of frequent patterns
            int[] beta = new int[prefixAlpha.length + 1];
            System.arraycopy(prefixAlpha, 0, beta, 0, prefixAlpha.length);
            beta[prefixAlpha.length] = item;

            // calculate the support of beta
            int betaSupport = (prefixSupport < support) ? prefixSupport : support;
            // save beta to the output file
            saveItemset(beta, betaSupport);

            // === Construct beta's conditional pattern base ===
            // It is a subdatabase which consists of the set of prefix paths
            // in the FP-tree co-occuring with the suffix pattern.
            List<List<FPNode>> prefixPaths = new ArrayList<List<FPNode>>();
            FPNode path = tree.mapItemNodes.get(item);
            while (path != null) {
                // if the path is not just the root node
                if (path.parent.itemID != -1) {
                    // create the prefixpath
                    List<FPNode> prefixPath = new ArrayList<FPNode>();
                    // add this node.
                    prefixPath.add(path);   // NOTE: we add it just to keep its support,
                    // actually it should not be part of the prefixPath

                    //Recursively add all the parents of this node.
                    FPNode parent = path.parent;
                    while (parent.itemID != -1) {
                        prefixPath.add(parent);
                        parent = parent.parent;
                    }
                    // add the path to the list of prefixpaths
                    prefixPaths.add(prefixPath);
                }
                // We will look for the next prefixpath
                path = path.nodeLink;
            }

            // (A) Calculate the frequency of each item in the prefixpath
            // The frequency is stored in a map such that:
            // key:  item   value: support
            Map<Integer, Integer> mapSupportBeta = new HashMap<Integer, Integer>();
            // for each prefixpath
            for (List<FPNode> prefixPath : prefixPaths) {
                // the support of the prefixpath is the support of its first node.
                int pathCount = prefixPath.get(0).counter;
                // for each node in the prefixpath,
                // except the first one, we count the frequency
                for (int j = 1; j < prefixPath.size(); j++) {
                    FPNode node = prefixPath.get(j);
                    // if the first time we see that node id
                    if (mapSupportBeta.get(node.itemID) == null) {
                        // just add the path count
                        mapSupportBeta.put(node.itemID, pathCount);
                    } else {
                        // otherwise, make the sum with the value already stored
                        mapSupportBeta.put(node.itemID, mapSupportBeta.get(node.itemID) + pathCount);
                    }
                }
            }

            // (B) Construct beta's conditional FP-Tree
            // Create the tree.
            FPTree treeBeta = new FPTree();
            // Add each prefixpath in the FP-tree.
            for (List<FPNode> prefixPath : prefixPaths) {
                treeBeta.addPrefixPath(prefixPath, mapSupportBeta, minimumSupport);
            }
            // Create the header list.
            treeBeta.createHeaderList(mapSupportBeta);

            // Mine recursively the Beta tree if the root as child(s)
            if (treeBeta.root.childs.size() > 0) {
                // recursive call
                fpgrowth(treeBeta, beta, betaSupport, mapSupportBeta, c);
            }
        }

    }

    /**
     * This method is for adding recursively all combinations of nodes in a path, concatenated with a given prefix,
     * to the set of patterns found.
     *
     * @param prefix the prefix
     * @throws IOException exception if error while writing the output file
     */
    private void addAllCombinationsForPathAndPrefix(FPNode node, int[] prefix) throws IOException {
        // Concatenate the node item to the current prefix
        int[] itemset = new int[prefix.length + 1];
        System.arraycopy(prefix, 0, itemset, 0, prefix.length);
        itemset[prefix.length] = node.itemID;

        // save the resulting itemset to the file with its support
        saveItemset(itemset, node.counter);

        // recursive call if there is a node link
        if (!node.childs.isEmpty()) {
            addAllCombinationsForPathAndPrefix(node.childs.get(0), itemset);
            addAllCombinationsForPathAndPrefix(node.childs.get(0), prefix);
        }
    }

    /**
     * Write a frequent itemset that is found to the output file or
     * keep into memory if the user prefer that the result be saved into memory.
     */
    private void saveItemset(int[] itemset, int support) throws IOException {
        // increase the number of itemsets found for statistics purpose
        itemsetCount++;

        // We sort the itemset before showing it to the user so that it is
        // in lexical order.
        Arrays.sort(itemset);

        StringBuilder stringBuilder = new StringBuilder();
        // write the items of the itemset
        for (int i = 0; i < itemset.length; i++) {
            stringBuilder.append(itemset[i]);
            if (i != itemset.length - 1) {
                stringBuilder.append(' ');
            }
        }
        // Then, write the support
        stringBuilder.append(" #SUP: ");
        stringBuilder.append(support);
        // write to file and create a new line
        LOGGER.debug(stringBuilder.toString());
        c.output(new ItemsListWithSupport(itemset, Long.valueOf(support)));

    }

    /**
     * Print statistics about the algorithm execution to Logger.
     */
    public void printStats() {
        LOGGER.info("=============  FP-GROWTH - STATS =============");
        long temps = endTime - startTimestamp;
        LOGGER.info(" Transactions count from database : " + transactionCount);
        LOGGER.info(" Frequent itemsets count : " + itemsetCount);
        LOGGER.info(" Total time ~ " + temps + " ms");
        LOGGER.info("===================================================");
    }

}