package com.stewel.dataflow.assocrules;

import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

public class RuleFormatter {

    private final Map<Integer, String> categoryTranslations;
    DecimalFormat decimalFormat = new DecimalFormat("#.##");
    String format = "%s => %s (support: %d, confidence: %.2f, lift: %.2f)";

    public RuleFormatter(final Map<Integer, String> categoryTranslations) {
        this.categoryTranslations = categoryTranslations;
    }

    public String formatRule(final AssociationRule rule) {
        String antecedentList = Arrays.stream(rule.getAntecedent()).mapToObj(id -> categoryTranslations.getOrDefault(id, String.valueOf(id))).collect(Collectors.joining(","));
        String consequentList = Arrays.stream(rule.getConsequent()).mapToObj(id -> categoryTranslations.getOrDefault(id, String.valueOf(id))).collect(Collectors.joining(","));
        return String.format(Locale.ENGLISH, format, antecedentList, consequentList, rule.getAbsoluteSupport(), rule.getConfidence(), rule.getLift());
    }

}
