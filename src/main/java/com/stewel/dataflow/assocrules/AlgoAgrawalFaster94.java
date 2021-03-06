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
import com.stewel.dataflow.fpgrowth.Itemsets;
import org.apache.commons.digester.Rules;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
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

    private final ItemsetCandidateGenerator itemsetsCandidateGenerator;
    private final ItemsetSupportCalculator itemsetSupportCalculator;
    private final AssociationRuleWriter associationRuleWriter;

    private final Itemsets patterns;
    private final long databaseSize; // number of transactions in database
    private final double minimumConfidence;
    private final double minimumLift;
    private final boolean usingLift = true;

    public AlgoAgrawalFaster94(@Nonnull final ItemsetCandidateGenerator itemsetsCandidateGenerator,
                               @Nonnull final ItemsetSupportCalculator itemsetSupportCalculator,
                               @Nonnull final AssociationRuleWriter associationRuleWriter,
                               @Nonnull final Itemsets patterns,
                               final long databaseSize,
                               final double minimumConfidence,
                               final double minimumLift) {
        this.itemsetsCandidateGenerator = Objects.requireNonNull(itemsetsCandidateGenerator, "itemsetsCandidateGenerator");
        this.itemsetSupportCalculator = Objects.requireNonNull(itemsetSupportCalculator, "itemsetSupportCalculator");
        this.associationRuleWriter = Objects.requireNonNull(associationRuleWriter, "associationRuleWriter");
        this.patterns = Objects.requireNonNull(patterns, "patterns");
        this.databaseSize = databaseSize;
        this.minimumConfidence = minimumConfidence;
        this.minimumLift = minimumLift;
        itemsetSupportCalculator.init(patterns);
    }

    /**
     * Run the algorithm for generating association rules from a set of itemsets.
     *
     * @return the set of rules found if the user chose to save the result to memory
     * @throws IOException exception if error while writting to file
     */
    public void runAlgorithm()
            throws IOException {
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
                    long support = itemsetSupportCalculator.calculateSupport(itemset_Lk_minus_hm_P_1); // THIS COULD BE
                    // OPTIMIZED ?
                    double supportAsDouble = (double) support;

                    // calculate the confidence of the rule : itemset_Lk_minus_hm_P_1 ==>  hm_P_1
                    double conf = lk.getAbsoluteSupport() / supportAsDouble;

                    // if the confidence is lower than minimumConfidence
                    if (conf < minimumConfidence || Double.isInfinite(conf)) {
                        continue;
                    }

                    double lift = 0;
                    long supportHm_P_1 = 0;
                    // if the user is using the minimumLift threshold, we will need
                    // to also calculate the lift of the rule:  itemset_Lk_minus_hm_P_1 ==>  hm_P_1
                    if (usingLift) {
                        // if we want to calculate the lift, we need the support of hm_P_1
                        supportHm_P_1 = itemsetSupportCalculator.calculateSupport(itemsetHm_P_1);  // if we want to calculate the lift, we need to add this.
                        // calculate the lift
                        double term1 = ((double) lk.getAbsoluteSupport()) / databaseSize;
                        double term2 = supportAsDouble / databaseSize;
                        double term3 = ((double) supportHm_P_1 / databaseSize);
                        lift = term1 / (term2 * term3);

                        // if the lift is not enough
                        if (lift < minimumLift) {
                            continue;
                        }
                    }

                    // If we are here, it means that the rule respect the minimumConfidence and minimumLift parameters.
                    // Therefore, we output the rule.
                    associationRuleWriter.write(ImmutableAssociationRule.builder()
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
            List<int[]> Hm_plus_1 = itemsetsCandidateGenerator.generateCandidateSizeK(Hm);

            // for each such candidates
            for (int[] hm_P_1 : Hm_plus_1) {

                // We subtract the candidate from the itemset "lk"
                int[] itemset_Lk_minus_hm_P_1 = Itemset.cloneItemSetMinusAnItemset(lk.getItems(), hm_P_1);

                // We will now calculate the support of the rule  Lk/(hm_P_1) ==> hm_P_1
                // we need it to calculate the confidence
                long support = itemsetSupportCalculator.calculateSupport(itemset_Lk_minus_hm_P_1);

                double supportAsDouble = (double) support;

                // calculate the confidence of the rule Lk/(hm_P_1) ==> hm_P_1
                double conf = lk.getAbsoluteSupport() / supportAsDouble;

                // if the confidence is not enough than we don't need to consider
                // the rule  Lk/(hm_P_1) ==> hm_P_1 anymore so we continue
                if (conf < minimumConfidence || Double.isInfinite(conf)) {
                    continue;
                }

                double lift = 0;
                long supportHm_P_1 = 0;
                // if the user is using the minimumLift threshold, then we will need to calculate the lift of the
                // rule as well and check if the lift is higher or equal to minimumLift.
                if (usingLift) {
                    // if we want to calculate the lift, we need the support of Hm+1
                    supportHm_P_1 = itemsetSupportCalculator.calculateSupport(hm_P_1);
                    // calculate the lift of the rule:  Lk/(hm_P_1) ==> hm_P_1
                    double term1 = ((double) lk.getAbsoluteSupport()) / databaseSize;
                    double term2 = (supportAsDouble) / databaseSize;

                    lift = term1 / (term2 * ((double) supportHm_P_1 / databaseSize));

                    // if the lift is not enough
                    if (lift < minimumLift) {
                        continue;
                    }
                }

                // The rule has passed the confidence and lift threshold requirements,
                // so we can output it
                associationRuleWriter.write(ImmutableAssociationRule.builder()
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
}