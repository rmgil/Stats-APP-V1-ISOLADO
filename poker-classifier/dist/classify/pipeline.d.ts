import { z } from 'zod';
declare const FileEntrySchema: z.ZodObject<{
    input: z.ZodString;
    output_class: z.ZodEnum<{
        mystery: "mystery";
        PKO: "PKO";
        "non-KO": "non-KO";
        unknown: "unknown";
    }>;
    detector: z.ZodObject<{
        reason: z.ZodString;
        score: z.ZodNumber;
    }, z.core.$strip>;
    encoding: z.ZodString;
    bytes: z.ZodNumber;
}, z.core.$strip>;
declare const ManifestSchema: z.ZodObject<{
    run_id: z.ZodString;
    started_at: z.ZodString;
    finished_at: z.ZodString;
    totals: z.ZodObject<{
        PKO: z.ZodNumber;
        mystery: z.ZodNumber;
        'non-KO': z.ZodNumber;
        unknown: z.ZodNumber;
    }, z.core.$strip>;
    files: z.ZodArray<z.ZodObject<{
        input: z.ZodString;
        output_class: z.ZodEnum<{
            mystery: "mystery";
            PKO: "PKO";
            "non-KO": "non-KO";
            unknown: "unknown";
        }>;
        detector: z.ZodObject<{
            reason: z.ZodString;
            score: z.ZodNumber;
        }, z.core.$strip>;
        encoding: z.ZodString;
        bytes: z.ZodNumber;
    }, z.core.$strip>>;
}, z.core.$strip>;
export type ClassificationManifest = z.infer<typeof ManifestSchema>;
export type FileEntry = z.infer<typeof FileEntrySchema>;
export interface ClassificationOptions {
    inputDir: string;
    outputDir: string;
    pattern?: string;
    preserveStructure?: boolean;
    verbose?: boolean;
}
/**
 * Main classification pipeline
 */
export declare function classifyTournaments(options: ClassificationOptions): Promise<ClassificationManifest>;
/**
 * Create ZIP archive from classification results
 */
export declare function createArchive(sourceDir: string, outputPath: string): Promise<void>;
/**
 * Full pipeline: classify and create archive
 */
export declare function runClassificationPipeline(inputDir: string, outputZipPath: string, options?: Partial<ClassificationOptions>): Promise<ClassificationManifest>;
export {};
//# sourceMappingURL=pipeline.d.ts.map