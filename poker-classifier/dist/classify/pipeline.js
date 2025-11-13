"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function(o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
Object.defineProperty(exports, "__esModule", { value: true });
exports.classifyTournaments = classifyTournaments;
exports.createArchive = createArchive;
exports.runClassificationPipeline = runClassificationPipeline;
/**
 * Classification pipeline orchestration
 */
const path = __importStar(require("path"));
const uuid_1 = require("uuid");
const archiver = __importStar(require("archiver"));
const fs_1 = require("fs");
const encoding_1 = require("../lib/encoding");
const fsx_1 = require("../lib/fsx");
const detect_1 = require("./detect");
const zod_1 = require("zod");
// Manifest schemas
const FileEntrySchema = zod_1.z.object({
    input: zod_1.z.string(),
    output_class: zod_1.z.enum(['PKO', 'mystery', 'non-KO', 'unknown']),
    detector: zod_1.z.object({
        reason: zod_1.z.string(),
        score: zod_1.z.number()
    }),
    encoding: zod_1.z.string(),
    bytes: zod_1.z.number()
});
const ManifestSchema = zod_1.z.object({
    run_id: zod_1.z.string(),
    started_at: zod_1.z.string(),
    finished_at: zod_1.z.string(),
    totals: zod_1.z.object({
        PKO: zod_1.z.number(),
        mystery: zod_1.z.number(),
        'non-KO': zod_1.z.number(),
        unknown: zod_1.z.number()
    }),
    files: zod_1.z.array(FileEntrySchema)
});
/**
 * Main classification pipeline
 */
async function classifyTournaments(options) {
    const { inputDir, outputDir, pattern = '**/*.txt', preserveStructure = false, verbose = false } = options;
    const runId = (0, uuid_1.v4)();
    const startedAt = new Date().toISOString();
    if (verbose)
        console.log(`Starting classification run: ${runId}`);
    // Find all text files
    const files = await (0, fsx_1.findFiles)(pattern, { cwd: inputDir });
    if (verbose)
        console.log(`Found ${files.length} files to process`);
    // Prepare output directories
    const classificationDirs = {
        'PKO': path.join(outputDir, 'PKO'),
        'mystery': path.join(outputDir, 'mystery'),
        'non-KO': path.join(outputDir, 'non-KO'),
        'unknown': path.join(outputDir, 'unknown')
    };
    for (const dir of Object.values(classificationDirs)) {
        await (0, fsx_1.ensureDir)(dir);
    }
    // Process files
    const fileEntries = [];
    const totals = {
        'PKO': 0,
        'mystery': 0,
        'non-KO': 0,
        'unknown': 0
    };
    for (const file of files) {
        const inputPath = path.join(inputDir, file);
        try {
            // Read and detect encoding
            const { content, encoding, originalBytes } = await (0, encoding_1.detectAndRead)(inputPath);
            const stats = await (0, fsx_1.getFileStats)(inputPath);
            // Detect tournament type
            const detection = (0, detect_1.detectTournamentTypeEnhanced)(content, path.basename(file));
            // Determine output path
            const outputClass = detection.type;
            const outputFilename = preserveStructure
                ? file
                : path.basename(file);
            const outputPath = path.join(classificationDirs[outputClass], outputFilename);
            // Write file preserving encoding
            await (0, fsx_1.writeFile)(outputPath, originalBytes);
            // Update manifest
            const entry = {
                input: file,
                output_class: outputClass,
                detector: {
                    reason: detection.reason,
                    score: detection.confidence
                },
                encoding,
                bytes: stats.size
            };
            fileEntries.push(entry);
            totals[outputClass]++;
            if (verbose) {
                console.log(`Classified ${file} as ${outputClass} (${detection.reason})`);
            }
        }
        catch (error) {
            console.error(`Error processing ${file}:`, error);
            // Add error entry
            fileEntries.push({
                input: file,
                output_class: 'unknown',
                detector: {
                    reason: `Processing error: ${error}`,
                    score: 0
                },
                encoding: 'unknown',
                bytes: 0
            });
            totals['unknown']++;
        }
    }
    // Create manifest
    const manifest = {
        run_id: runId,
        started_at: startedAt,
        finished_at: new Date().toISOString(),
        totals,
        files: fileEntries
    };
    // Validate manifest
    ManifestSchema.parse(manifest);
    // Write manifest
    await (0, fsx_1.writeJson)(path.join(outputDir, 'classification_manifest.json'), manifest);
    if (verbose) {
        console.log('Classification complete:');
        console.log(`  PKO: ${totals['PKO']}`);
        console.log(`  Mystery: ${totals['mystery']}`);
        console.log(`  Non-KO: ${totals['non-KO']}`);
        console.log(`  Unknown: ${totals['unknown']}`);
    }
    return manifest;
}
/**
 * Create ZIP archive from classification results
 */
async function createArchive(sourceDir, outputPath) {
    return new Promise((resolve, reject) => {
        const output = (0, fs_1.createWriteStream)(outputPath);
        const archive = archiver.default('zip', {
            zlib: { level: 9 } // Maximum compression
        });
        output.on('close', () => resolve());
        archive.on('error', (err) => reject(err));
        archive.pipe(output);
        // Add directories
        archive.directory(path.join(sourceDir, 'PKO'), 'PKO');
        archive.directory(path.join(sourceDir, 'mystery'), 'mystery');
        archive.directory(path.join(sourceDir, 'non-KO'), 'non-KO');
        // Add manifest
        archive.file(path.join(sourceDir, 'classification_manifest.json'), {
            name: 'classification_manifest.json'
        });
        // Don't include unknown directory if empty
        const unknownDir = path.join(sourceDir, 'unknown');
        (0, fsx_1.getFileStats)(unknownDir).then(stats => {
            if (stats.exists) {
                archive.directory(unknownDir, 'unknown');
            }
            archive.finalize();
        });
    });
}
/**
 * Full pipeline: classify and create archive
 */
async function runClassificationPipeline(inputDir, outputZipPath, options) {
    const tempDir = path.join(path.dirname(outputZipPath), `temp_${Date.now()}`);
    try {
        // Clean temp directory
        await (0, fsx_1.cleanDir)(tempDir);
        // Run classification
        const manifest = await classifyTournaments({
            inputDir,
            outputDir: tempDir,
            ...options
        });
        // Create archive
        await createArchive(tempDir, outputZipPath);
        // Clean up temp directory
        await (0, fsx_1.cleanDir)(tempDir);
        return manifest;
    }
    catch (error) {
        // Clean up on error
        try {
            await (0, fsx_1.cleanDir)(tempDir);
        }
        catch { }
        throw error;
    }
}
//# sourceMappingURL=pipeline.js.map