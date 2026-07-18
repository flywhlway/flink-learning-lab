package com.flywhl.flinklab.p02.score;

/**
 * 规则打分中间结果（Wave 0 {@code RuleScorerTest} 契约）。
 */
public record ScoredItem(String itemId, double score, String reason) {
}
