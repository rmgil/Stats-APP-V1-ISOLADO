import * as glob from 'fast-glob';
/**
 * Ensure directory exists (recursive)
 */
export declare function ensureDir(dirPath: string): Promise<void>;
/**
 * Copy file preserving metadata
 */
export declare function copyFile(src: string, dest: string): Promise<void>;
/**
 * Write file with directory creation
 */
export declare function writeFile(filePath: string, content: Buffer | string): Promise<void>;
/**
 * Find all files matching pattern
 */
export declare function findFiles(pattern: string | string[], options?: glob.Options): Promise<string[]>;
/**
 * Get file stats safely
 */
export declare function getFileStats(filePath: string): Promise<{
    exists: boolean;
    size: number;
    modified: Date;
    created: Date;
} | {
    exists: boolean;
    size: number;
    modified: null;
    created: null;
}>;
/**
 * Clean directory (remove and recreate)
 */
export declare function cleanDir(dirPath: string): Promise<void>;
/**
 * Read JSON file safely
 */
export declare function readJson<T = any>(filePath: string): Promise<T | null>;
/**
 * Write JSON file with pretty formatting
 */
export declare function writeJson(filePath: string, data: any): Promise<void>;
//# sourceMappingURL=fsx.d.ts.map