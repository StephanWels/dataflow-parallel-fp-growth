package com.stewel.dataflow.assocrules;

import com.stewel.dataflow.fpgrowth.Itemset;
import com.stewel.dataflow.fpgrowth.ItemsetComparator;
import com.stewel.dataflow.fpgrowth.Itemsets;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

public class ItemsetSupportCalculator {

    private final static ItemsetComparator ITEMSET_COMPARATOR = new ItemsetComparator();

    private final SupportRepository supportRepository;

    private Itemsets patterns;

    public ItemsetSupportCalculator(final SupportRepository supportRepository) {
        this.supportRepository = Objects.requireNonNull(supportRepository, "supportRepository");
    }

    public void init(final Itemsets patterns) {
        this.patterns = Objects.requireNonNull(patterns, "patterns");
        // First, we sort all itemsets having the same size by lexical order
        // We do this for optimization purposes. If the itemsets are sorted, it allows to
        // perform two optimizations:
        // 1) When we need to calculate the support of an itemset (in the method
        // "calculateSupport()") we can use a binary search instead of browsing the whole list.
        // 2) When combining itemsets to generate candidate, we can use the
        //    lexical order to avoid comparisons (in the method "generateCandidates()").

        // For itemsets of the same size
        for (final List<Itemset> itemsetsSameSize : patterns.getLevels()) {
            // Sort by lexicographical order using a Comparator
            Collections.sort(itemsetsSameSize, new Comparator<Itemset>() {
                @Override
                public int compare(final Itemset o1, final Itemset o2) {
                    // The following code assume that itemsets are the same size
                    return ITEMSET_COMPARATOR.compare(o1.getItems(), o2.getItems());
                }
            });
        }
    }

    /**
     * Calculate the support of an itemset by looking at the frequent patterns of the same size.
     *
     * Because patterns are sorted by lexical order, we use a binary search.
     * This is MUCH MORE efficient than just browsing the full list of patterns.
     *
     * @param itemset the itemset.
     * @return the support of the itemset
     */
    public long calculateSupport(final int[] itemset) {
        // We first get the list of patterns having the same size as "itemset".
        final List<Itemset> patternsSameSize = patterns.getLevels().get(itemset.length);
        // We perform a binary search to find the position of itemset in this list.
        int first = 0;
        int last = patternsSameSize.size() - 1;

        while (first <= last) {
            final int middle = (first + last) >> 1; // >>1 means to divide by 2
            final int[] itemsetMiddle = patternsSameSize.get(middle).getItems();
            final int comparison = ITEMSET_COMPARATOR.compare(itemset, itemsetMiddle);
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

}
