package com.stewel.dataflow.assocrules;

import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.Locale;
import java.util.stream.Collectors;

public class RuleFormatter {

    DecimalFormat decimalFormat = new DecimalFormat("#.##");
    String format = "%s => %s (support: %d, confidence: %.2f, lift: %.2f)";

    public String formatRule(final Rule rule) {
        String antecedentList = Arrays.stream(rule.getAntecedent()).mapToObj(String::valueOf).collect(Collectors.joining(","));
        String consequentList = Arrays.stream(rule.getConsequent()).mapToObj(String::valueOf).collect(Collectors.joining(","));
        return String.format(Locale.ENGLISH, format, antecedentList, consequentList, rule.getAbsoluteSupport(), rule.getConfidence(), rule.getLift());
    }

}
