"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.detectTournamentType = detectTournamentType;
exports.detectTournamentTypeEnhanced = detectTournamentTypeEnhanced;
/**
 * Tournament type detection logic
 */
const keywords_1 = require("./keywords");
/**
 * Detect tournament type from text content
 */
function detectTournamentType(content, filename = '') {
    // Combine filename and content for analysis
    const fullText = `${filename}\n${content}`;
    // First check for freezeout - strong indicator of non-KO
    if (/\bfreeze[\s-]?out\b/gi.test(fullText)) {
        return {
            type: 'non-KO',
            confidence: 0.9,
            reason: 'Freezeout tournament format',
            matches: fullText.match(/\bfreeze[\s-]?out\b/gi) || []
        };
    }
    // Calculate scores for each type
    const pkoScore = (0, keywords_1.calculateMatchScore)(fullText, keywords_1.KEYWORDS.PKO, keywords_1.EXCLUSION_PATTERNS.PKO);
    const mysteryScore = (0, keywords_1.calculateMatchScore)(fullText, keywords_1.KEYWORDS.mystery, keywords_1.EXCLUSION_PATTERNS.mystery);
    const nonKOScore = (0, keywords_1.calculateMatchScore)(fullText, keywords_1.KEYWORDS.nonKO);
    // Priority: Mystery > Non-KO (if explicit) > PKO
    // Mystery takes precedence due to specific tournament format
    if (mysteryScore.score > 0) {
        return {
            type: 'mystery',
            confidence: Math.min(mysteryScore.score / 3, 1.0), // Normalize confidence
            reason: `Matched mystery patterns: ${mysteryScore.matches.join(', ')}`,
            matches: mysteryScore.matches
        };
    }
    // Check for non-KO with higher threshold
    if (nonKOScore.score > 0) {
        return {
            type: 'non-KO',
            confidence: Math.min(nonKOScore.score / 2, 1.0),
            reason: `Matched non-KO patterns: ${nonKOScore.matches.join(', ')}`,
            matches: nonKOScore.matches
        };
    }
    if (pkoScore.score > 0) {
        return {
            type: 'PKO',
            confidence: Math.min(pkoScore.score / 3, 1.0),
            reason: `Matched PKO patterns: ${pkoScore.matches.join(', ')}`,
            matches: pkoScore.matches
        };
    }
    // Heuristic: Check for general knockout indicators
    const generalKOPattern = /\b(knockout|ko|bounty)\b/gi;
    const negativeContext = /\b(no|not|without|non)[\s\-]*(knockout|ko|bounty|bounties)/gi;
    // Check if knockout terms appear in negative context
    if (negativeContext.test(fullText)) {
        return {
            type: 'non-KO',
            confidence: 0.8,
            reason: 'Explicit non-knockout indicators',
            matches: fullText.match(negativeContext) || []
        };
    }
    const hasGeneralKO = generalKOPattern.test(fullText);
    if (hasGeneralKO) {
        // Has knockout but not specific type
        return {
            type: 'PKO', // Default to PKO for generic knockout
            confidence: 0.5,
            reason: 'Generic knockout indicators found',
            matches: fullText.match(generalKOPattern) || []
        };
    }
    // No clear classification
    return {
        type: 'unknown',
        confidence: 0,
        reason: 'No matching patterns found',
        matches: []
    };
}
/**
 * Enhanced detection with multi-pass analysis
 */
function detectTournamentTypeEnhanced(content, filename = '') {
    // First pass: Standard detection
    const firstPass = detectTournamentType(content, filename);
    // If high confidence, return immediately
    if (firstPass.confidence >= 0.8) {
        return firstPass;
    }
    // Second pass: Look for tournament structure indicators
    const structureIndicators = analyzeStructure(content);
    // Combine results
    if (structureIndicators.type !== 'unknown') {
        return {
            ...structureIndicators,
            confidence: (firstPass.confidence + structureIndicators.confidence) / 2,
            matches: [...firstPass.matches, ...structureIndicators.matches]
        };
    }
    return firstPass;
}
/**
 * Analyze tournament structure for type indicators
 */
function analyzeStructure(content) {
    const lines = content.split('\n').slice(0, 100); // Check first 100 lines
    // Look for bounty indicators in hand history
    const bountyPattern = /\bbounty\s*(awarded|won|collected|earned)/gi;
    const knockoutPattern = /\bknocked out\b|\beliminated\b.*\bbounty\b/gi;
    for (const line of lines) {
        if (bountyPattern.test(line)) {
            const isMystery = /mystery/i.test(line);
            return {
                type: isMystery ? 'mystery' : 'PKO',
                confidence: 0.7,
                reason: 'Found bounty transaction in hand history',
                matches: line.match(bountyPattern) || []
            };
        }
        if (knockoutPattern.test(line)) {
            return {
                type: 'PKO',
                confidence: 0.6,
                reason: 'Found knockout with bounty reference',
                matches: line.match(knockoutPattern) || []
            };
        }
    }
    // Look for tournament title/header
    const titlePattern = /^(tournament|torneio|event)[\s#:]*(.+)/gmi;
    const titleMatches = content.match(titlePattern);
    if (titleMatches) {
        for (const title of titleMatches.slice(0, 5)) { // Check first 5 matches
            const result = detectTournamentType(title, '');
            if (result.type !== 'unknown') {
                return {
                    ...result,
                    reason: `Tournament title: ${result.reason}`,
                    confidence: result.confidence * 0.8
                };
            }
        }
    }
    return {
        type: 'unknown',
        confidence: 0,
        reason: 'No structural indicators found',
        matches: []
    };
}
//# sourceMappingURL=detect.js.map