package com.stewel.dataflow.assocrules;

import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

public class AssociationRuleFormatter {

    private final Map<Integer, String> categoryTranslations;
    private final static String FORMAT = "%s => %s (support: %d, confidence: %.2f, lift: %.2f)";

    public AssociationRuleFormatter(final Map<Integer, String> categoryTranslations) {
        this.categoryTranslations = categoryTranslations;
    }

    public String formatRule(final AssociationRule rule) {
        final String antecedentList = Arrays.stream(rule.getAntecedent()).mapToObj(id -> categoryTranslations.getOrDefault(id, String.valueOf(id))).collect(Collectors.joining(","));
        final String consequentList = Arrays.stream(rule.getConsequent()).mapToObj(id -> categoryTranslations.getOrDefault(id, String.valueOf(id))).collect(Collectors.joining(","));
        return String.format(Locale.ENGLISH, FORMAT, antecedentList, consequentList, rule.getAbsoluteSupport(), rule.getConfidence(), rule.getLift());
    }

}
