/**
 * Keyword definitions for tournament classification
 */
export interface KeywordPattern {
    patterns: RegExp[];
    weight: number;
    description: string;
}
export interface ClassificationKeywords {
    PKO: KeywordPattern[];
    mystery: KeywordPattern[];
    nonKO: KeywordPattern[];
}
/**
 * Tournament classification keywords
 */
export declare const KEYWORDS: ClassificationKeywords;
/**
 * Exclusion patterns that negate classifications
 */
export declare const EXCLUSION_PATTERNS: Record<string, RegExp[]>;
/**
 * Calculate keyword match score
 */
export declare function calculateMatchScore(text: string, patterns: KeywordPattern[], exclusions?: RegExp[]): {
    score: number;
    matches: string[];
};
//# sourceMappingURL=keywords.d.ts.map