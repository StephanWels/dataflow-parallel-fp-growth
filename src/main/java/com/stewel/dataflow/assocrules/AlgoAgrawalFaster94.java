package com.stewel.dataflow.assocrules;

/* This file is copyright (c) 2008-2012 Philippe Fournier-Viger
*
* This file is part of the SPMF DATA MINING SOFTWARE
* (http://www.philippe-fournier-viger.com/spmf).
*
* SPMF is free software: you can redistribute it and/or modify it under the
* terms of the GNU General Public License as published by the Free Software
* Foundation, either version 3 of the License, or (at your option) any later
* version.
* SPMF is distributed in the hope that it will be useful, but WITHOUT ANY
* WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
* A PARTICULAR PURPOSE. See the GNU General Public License for more details.
* You should have received a copy of the GNU General Public License along with
* SPMF. If not, see <http://www.gnu.org/licenses/>.
*/

import com.stewel.dataflow.fpgrowth.Itemset;
import com.stewel.dataflow.fpgrowth.ItemsetComparator;
import com.stewel.dataflow.fpgrowth.Itemsets;
import org.apache.commons.digester.Rules;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 * This is an implementation of the "faster algorithm" for generating association rules,
 * described in Agrawal &
 * al. 1994, IBM Research Report RJ9839, June 1994.
 * <br/><br/>
 * <p>
 * This implementation saves the result to a file
 * or can alternatively keep it into memory if no output
 * path is provided by the user when the runAlgorithm()
 * method is called.
 *
 * @author Philippe Fournier-Viger
 * @see Rules
 **/

public class AlgoAgrawalFaster94 {

    private final static ItemsetComparator ITEMSET_COMPARATOR = new ItemsetComparator();

    private final AssociationRuleRepository associationRuleRepository;
    private final SupportRepository supportRepository;

    // the frequent itemsets that will be used to generate the rules
    private Itemsets patterns;

    protected long databaseSize = 0; // number of transactions in database

    // parameters
    protected double minconf;
    protected double minlift;
    protected boolean usingLift = true;

    /**
     * Default constructor
     */
    public AlgoAgrawalFaster94(@Nonnull final AssociationRuleRepository associationRuleRepository,
                               @Nonnull final SupportRepository supportRepository) {
        this.associationRuleRepository = Objects.requireNonNull(associationRuleRepository);
        this.supportRepository = Objects.requireNonNull(supportRepository);
    }

    /**
     * Run the algorithm
     *
     * @param patterns     a set of frequent itemsets
     * @param databaseSize the number of transactions in the database
     * @param minconf      the minconf threshold
     * @param minlift      the minlift threshold
     * @return the set of association rules if the user wished to save them into memory
     * @throws IOException exception if error writing to the output file
     */
    public void runAlgorithm(Itemsets patterns, long databaseSize, double minconf,
                                   double minlift) throws IOException {
        // save the parameters
        this.minconf = minconf;
        this.minlift = minlift;
        usingLift = true;

        // start the algorithm
        runAlgorithm(patterns, databaseSize);
    }

    /**
     * Run the algorithm for generating association rules from a set of itemsets.
     *
     * @param patterns     the set of itemsets.
     * @param databaseSize the number of transactions in the original database
     * @return the set of rules found if the user chose to save the result to memory
     * @throws IOException exception if error while writting to file
     */
    private void runAlgorithm(Itemsets patterns, long databaseSize)
            throws IOException {
        this.databaseSize = databaseSize;
        // save itemsets in a member variable
        this.patterns = patterns;

        // SORTING
        // First, we sort all itemsets having the same size by lexical order
        // We do this for optimization purposes. If the itemsets are sorted, it allows to
        // perform two optimizations:
        // 1) When we need to calculate the support of an itemset (in the method
        // "calculateSupport()") we can use a binary search instead of browsing the whole list.
        // 2) When combining itemsets to generate candidate, we can use the
        //    lexical order to avoid comparisons (in the method "generateCandidates()").

        // For itemsets of the same size
        for (List<Itemset> itemsetsSameSize : patterns.getLevels()) {
            // Sort by lexicographical order using a Comparator
            Collections.sort(itemsetsSameSize, new Comparator<Itemset>() {
                @Override
                public int compare(Itemset o1, Itemset o2) {
                    // The following code assume that itemsets are the same size
                    return ITEMSET_COMPARATOR.compare(o1.getItems(), o2.getItems());
                }
            });
        }
        // END OF SORTING

        // Now we will generate the rules.

        // For each frequent itemset of size >=2 that we will name "lk"
        for (int k = 2; k < patterns.getLevels().size(); k++) {
            for (Itemset lk : patterns.getLevels().get(k)) {

                // create a variable H1 for recursion
                List<int[]> H1_for_recursion = new ArrayList<int[]>();

                // For each itemset "itemsetSize1" of size 1 that is member of lk
                for (int item : lk.getItems()) {
                    int itemsetHm_P_1[] = new int[]{item};

                    // make a copy of  lk without items from  hm_P_1
                    int[] itemset_Lk_minus_hm_P_1 = Itemset.cloneItemSetMinusOneItem(lk.getItems(), item);

                    // Now we will calculate the support and confidence
                    // of the rule: itemset_Lk_minus_hm_P_1 ==>  hm_P_1
                    long support = calculateSupport(itemset_Lk_minus_hm_P_1); // THIS COULD BE
                    // OPTIMIZED ?
                    double supportAsDouble = (double) support;

                    // calculate the confidence of the rule : itemset_Lk_minus_hm_P_1 ==>  hm_P_1
                    double conf = lk.getAbsoluteSupport() / supportAsDouble;

                    // if the confidence is lower than minconf
                    if (conf < minconf || Double.isInfinite(conf)) {
                        continue;
                    }

                    double lift = 0;
                    long supportHm_P_1 = 0;
                    // if the user is using the minlift threshold, we will need
                    // to also calculate the lift of the rule:  itemset_Lk_minus_hm_P_1 ==>  hm_P_1
                    if (usingLift) {
                        // if we want to calculate the lift, we need the support of hm_P_1
                        supportHm_P_1 = calculateSupport(itemsetHm_P_1);  // if we want to calculate the lift, we need to add this.
                        // calculate the lift
                        double term1 = ((double) lk.getAbsoluteSupport()) / databaseSize;
                        double term2 = supportAsDouble / databaseSize;
                        double term3 = ((double) supportHm_P_1 / databaseSize);
                        lift = term1 / (term2 * term3);

                        // if the lift is not enough
                        if (lift < minlift) {
                            continue;
                        }
                    }

                    // If we are here, it means that the rule respect the minconf and minlift parameters.
                    // Therefore, we output the rule.
                    associationRuleRepository.save(ImmutableAssociationRule.builder()
                            .antecedent(itemset_Lk_minus_hm_P_1)
                            .consequent(itemsetHm_P_1)
                            .coverage(support)
                            .transactionCount(lk.getAbsoluteSupport())
                            .confidence(conf)
                            .lift(lift)
                            .build());

                    // Then we keep the itemset  hm_P_1 to find more rules using this itemset and lk.
                    H1_for_recursion.add(itemsetHm_P_1);
                    // ================ END OF WHAT I HAVE ADDED
                }
                // Finally, we make a recursive call to continue explores rules that can be made with "lk"
                apGenrules(k, 1, lk, H1_for_recursion);
            }

        }
    }

    /**
     * The ApGenRules as described in p.14 of the paper by Agrawal.
     * (see the Agrawal paper for more details).
     *
     * @param k  the size of the first itemset used to generate rules
     * @param m  the recursive depth of the call to this method (first time 1, then 2...)
     * @param lk the itemset that is used to generate rules
     * @param Hm a set of itemsets that can be used with lk to generate rules
     * @throws IOException exception if error while writing output file
     */
    private void apGenrules(int k, int m, Itemset lk, List<int[]> Hm)
            throws IOException {

        // if the itemset "lk" that is used to generate rules is larger than the size of itemsets in "Hm"
        if (k > m + 1) {
            // Create a list that we will be used to store itemsets for the recursive call
            List<int[]> Hm_plus_1_for_recursion = new ArrayList<int[]>();

            // generate candidates using Hm
            List<int[]> Hm_plus_1 = generateCandidateSizeK(Hm);

            // for each such candidates
            for (int[] hm_P_1 : Hm_plus_1) {

                // We subtract the candidate from the itemset "lk"
                int[] itemset_Lk_minus_hm_P_1 = Itemset.cloneItemSetMinusAnItemset(lk.getItems(), hm_P_1);

                // We will now calculate the support of the rule  Lk/(hm_P_1) ==> hm_P_1
                // we need it to calculate the confidence
                long support = calculateSupport(itemset_Lk_minus_hm_P_1);

                double supportAsDouble = (double) support;

                // calculate the confidence of the rule Lk/(hm_P_1) ==> hm_P_1
                double conf = lk.getAbsoluteSupport() / supportAsDouble;

                // if the confidence is not enough than we don't need to consider
                // the rule  Lk/(hm_P_1) ==> hm_P_1 anymore so we continue
                if (conf < minconf || Double.isInfinite(conf)) {
                    continue;
                }

                double lift = 0;
                long supportHm_P_1 = 0;
                // if the user is using the minlift threshold, then we will need to calculate the lift of the
                // rule as well and check if the lift is higher or equal to minlift.
                if (usingLift) {
                    // if we want to calculate the lift, we need the support of Hm+1
                    supportHm_P_1 = calculateSupport(hm_P_1);
                    // calculate the lift of the rule:  Lk/(hm_P_1) ==> hm_P_1
                    double term1 = ((double) lk.getAbsoluteSupport()) / databaseSize;
                    double term2 = (supportAsDouble) / databaseSize;

                    lift = term1 / (term2 * ((double) supportHm_P_1 / databaseSize));

                    // if the lift is not enough
                    if (lift < minlift) {
                        continue;
                    }
                }

                // The rule has passed the confidence and lift threshold requirements,
                // so we can output it
                associationRuleRepository.save(ImmutableAssociationRule.builder()
                        .antecedent(itemset_Lk_minus_hm_P_1)
                        .consequent(hm_P_1)
                        .coverage(support)
                        .transactionCount(lk.getAbsoluteSupport())
                        .confidence(conf)
                        .lift(lift)
                        .build());

                // if k == m+1, then we cannot explore further rules using Lk since Lk will be too small.
                if (k != m + 1) {
                    Hm_plus_1_for_recursion.add(hm_P_1);
                }
            }
            // recursive call to apGenRules to find more rules using "lk"
            apGenrules(k, m + 1, lk, Hm_plus_1_for_recursion);
        }
    }

    /**
     * Calculate the support of an itemset by looking at the frequent patterns
     * of the same size.
     * Because patterns are sorted by lexical order, we use a binary search.
     * This is MUCH MORE efficient than just browsing the full list of patterns.
     *
     * @param itemset the itemset.
     * @return the support of the itemset
     */
    private long calculateSupport(int[] itemset) {
        // We first get the list of patterns having the same size as "itemset"
        List<Itemset> patternsSameSize = patterns.getLevels().get(itemset.length);

        // We perform a binary search to find the position of itemset in this list
        int first = 0;
        int last = patternsSameSize.size() - 1;

        while (first <= last) {
            int middle = (first + last) >> 1; // >>1 means to divide by 2
            int[] itemsetMiddle = patternsSameSize.get(middle).getItems();

            int comparison = ITEMSET_COMPARATOR.compare(itemset, itemsetMiddle);
            if (comparison > 0) {
                first = middle + 1;  //  the itemset compared is larger than the subset according to the lexical order
            } else if (comparison < 0) {
                last = middle - 1; //  the itemset compared is smaller than the subset  is smaller according to the lexical order
            } else {
                // we have found the itemset, so we return its support.
                return patternsSameSize.get(middle).getAbsoluteSupport();
            }
        }
        return (int) supportRepository.getSupport(itemset);
    }

    /**
     * Generating candidate itemsets of size k from frequent itemsets of size
     * k-1. This is called "apriori-gen" in the paper by agrawal. This method is
     * also used by the Apriori algorithm for generating candidates.
     * Note that this method is very optimized. It assumed that the list of
     * itemsets received as parameter are lexically ordered.
     *
     * @param levelK_1 a set of itemsets of size k-1
     * @return a set of candidates
     */
    protected List<int[]> generateCandidateSizeK(List<int[]> levelK_1) {
        // create a variable to store candidates
        List<int[]> candidates = new ArrayList<int[]>();

        // For each itemset I1 and I2 of level k-1
        loop1:
        for (int i = 0; i < levelK_1.size(); i++) {
            int[] itemset1 = levelK_1.get(i);
            loop2:
            for (int j = i + 1; j < levelK_1.size(); j++) {
                int[] itemset2 = levelK_1.get(j);

                // we compare items of itemset1 and itemset2.
                // If they have all the same k-1 items and the last item of
                // itemset1 is smaller than
                // the last item of itemset2, we will combine them to generate a
                // candidate
                for (int k = 0; k < itemset1.length; k++) {
                    // if they are the last items
                    if (k == itemset1.length - 1) {
                        // the one from itemset1 should be smaller (lexical
                        // order)
                        // and different from the one of itemset2
                        if (itemset1[k] >= itemset2[k]) {
                            continue loop1;
                        }
                    }
                    // if they are not the last items, and
                    else if (itemset1[k] < itemset2[k]) {
                        continue loop2; // we continue searching
                    } else if (itemset1[k] > itemset2[k]) {
                        continue loop1; // we stop searching: because of lexical
                        // order
                    }
                }

                // Create a new candidate by combining itemset1 and itemset2
                int lastItem1 = itemset1[itemset1.length - 1];
                int lastItem2 = itemset2[itemset2.length - 1];
                int newItemset[];
                if (lastItem1 < lastItem2) {
                    // Create a new candidate by combining itemset1 and itemset2
                    newItemset = new int[itemset1.length + 1];
                    System.arraycopy(itemset1, 0, newItemset, 0, itemset1.length);
                    newItemset[itemset1.length] = lastItem2;
                    candidates.add(newItemset);
                } else {
                    // Create a new candidate by combining itemset1 and itemset2
                    newItemset = new int[itemset1.length + 1];
                    System.arraycopy(itemset2, 0, newItemset, 0, itemset2.length);
                    newItemset[itemset2.length] = lastItem1;
                    candidates.add(newItemset);
                }

            }
        }
        // return the set of candidates
        return candidates;
    }
}