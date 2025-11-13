export type TournamentType = 'PKO' | 'mystery' | 'non-KO' | 'unknown';
export interface DetectionResult {
    type: TournamentType;
    confidence: number;
    reason: string;
    matches: string[];
}
/**
 * Detect tournament type from text content
 */
export declare function detectTournamentType(content: string, filename?: string): DetectionResult;
/**
 * Enhanced detection with multi-pass analysis
 */
export declare function detectTournamentTypeEnhanced(content: string, filename?: string): DetectionResult;
//# sourceMappingURL=detect.d.ts.map