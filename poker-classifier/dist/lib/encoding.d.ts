export interface EncodingResult {
    encoding: string;
    confidence: number;
    content: string;
    originalBytes: Buffer;
}
/**
 * Detect encoding and read file content with proper conversion
 */
export declare function detectAndRead(filePath: string): Promise<EncodingResult>;
/**
 * Write content preserving original encoding
 */
export declare function encodeContent(content: string, encoding: string): Buffer;
//# sourceMappingURL=encoding.d.ts.map