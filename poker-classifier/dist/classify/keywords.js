"use strict";
/**
 * Keyword definitions for tournament classification
 */
Object.defineProperty(exports, "__esModule", { value: true });
exports.EXCLUSION_PATTERNS = exports.KEYWORDS = void 0;
exports.calculateMatchScore = calculateMatchScore;
/**
 * Create case-insensitive word boundary patterns
 */
function createWordPatterns(words) {
    return words.map(word => new RegExp(`\\b${word}\\b`, 'gi'));
}
/**
 * Tournament classification keywords
 */
exports.KEYWORDS = {
    PKO: [
        {
            patterns: createWordPatterns([
                'progressive', 'pko', 'knockout', 'knock-out', 'ko',
                'bounty', 'bounties', 'progressive knockout',
                'progressive ko', 'prog ko', 'prog knockout'
            ]),
            weight: 1.0,
            description: 'Progressive Knockout indicators'
        },
        {
            patterns: [
                /progressive\s+knock[\s-]?out/gi,
                /prog\.?\s*ko/gi,
                /\bpko\b/gi
            ],
            weight: 1.2,
            description: 'Strong PKO patterns'
        }
    ],
    mystery: [
        {
            patterns: createWordPatterns([
                'mystery', 'mysteries', 'mystery bounty', 'mystery bounties',
                'mystery knockout', 'mystery ko'
            ]),
            weight: 1.0,
            description: 'Mystery tournament indicators'
        },
        {
            patterns: [
                /mystery\s+bounty/gi,
                /mystery\s+knock[\s-]?out/gi,
                /\bmystery\b/gi
            ],
            weight: 1.2,
            description: 'Strong mystery patterns'
        }
    ],
    nonKO: [
        {
            patterns: createWordPatterns([
                'freezeout', 'freeze-out', 'regular', 'standard',
                'turbo', 'hyper', 'deep', 'deepstack', 'non-knockout',
                'non-ko', 'no knockout', 'no ko'
            ]),
            weight: 1.0,
            description: 'Non-knockout tournament types'
        },
        {
            patterns: [
                /\bno[\s-]?knockout\b/gi,
                /\bnon[\s-]?ko\b/gi,
                /\bregular\s+tournament\b/gi
            ],
            weight: 1.0,
            description: 'Explicit non-KO indicators'
        }
    ]
};
/**
 * Exclusion patterns that negate classifications
 */
exports.EXCLUSION_PATTERNS = {
    PKO: [
        /\bno[\s-]?pko\b/gi,
        /\bnot\s+progressive/gi,
        /\bwithout\s+knockout/gi
    ],
    mystery: [
        /\bno[\s-]?mystery/gi,
        /\bnot\s+mystery/gi,
        /\bwithout\s+mystery/gi
    ]
};
/**
 * Calculate keyword match score
 */
function calculateMatchScore(text, patterns, exclusions) {
    let totalScore = 0;
    const matches = [];
    // Check exclusions first
    if (exclusions) {
        for (const exclusion of exclusions) {
            if (exclusion.test(text)) {
                return { score: -1, matches: ['excluded'] };
            }
        }
    }
    // Calculate positive matches
    for (const pattern of patterns) {
        for (const regex of pattern.patterns) {
            const matchArray = text.match(regex);
            if (matchArray) {
                totalScore += pattern.weight * matchArray.length;
                matches.push(...matchArray);
            }
        }
    }
    return {
        score: totalScore,
        matches: [...new Set(matches)] // Unique matches
    };
}
//# sourceMappingURL=keywords.js.map