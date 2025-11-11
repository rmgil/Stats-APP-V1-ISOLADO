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
exports.ensureDir = ensureDir;
exports.copyFile = copyFile;
exports.writeFile = writeFile;
exports.findFiles = findFiles;
exports.getFileStats = getFileStats;
exports.cleanDir = cleanDir;
exports.readJson = readJson;
exports.writeJson = writeJson;
/**
 * Extended filesystem utilities
 */
const fs_1 = require("fs");
const path = __importStar(require("path"));
const glob = __importStar(require("fast-glob"));
/**
 * Ensure directory exists (recursive)
 */
async function ensureDir(dirPath) {
    try {
        await fs_1.promises.mkdir(dirPath, { recursive: true });
    }
    catch (error) {
        if (error.code !== 'EEXIST') {
            throw error;
        }
    }
}
/**
 * Copy file preserving metadata
 */
async function copyFile(src, dest) {
    await ensureDir(path.dirname(dest));
    await fs_1.promises.copyFile(src, dest);
    // Preserve timestamps
    const stats = await fs_1.promises.stat(src);
    await fs_1.promises.utimes(dest, stats.atime, stats.mtime);
}
/**
 * Write file with directory creation
 */
async function writeFile(filePath, content) {
    await ensureDir(path.dirname(filePath));
    await fs_1.promises.writeFile(filePath, content);
}
/**
 * Find all files matching pattern
 */
async function findFiles(pattern, options) {
    const patterns = Array.isArray(pattern) ? pattern : [pattern];
    return glob.default(patterns, {
        onlyFiles: true,
        followSymbolicLinks: false,
        ...options
    });
}
/**
 * Get file stats safely
 */
async function getFileStats(filePath) {
    try {
        const stats = await fs_1.promises.stat(filePath);
        return {
            exists: true,
            size: stats.size,
            modified: stats.mtime,
            created: stats.birthtime
        };
    }
    catch {
        return {
            exists: false,
            size: 0,
            modified: null,
            created: null
        };
    }
}
/**
 * Clean directory (remove and recreate)
 */
async function cleanDir(dirPath) {
    try {
        await fs_1.promises.rm(dirPath, { recursive: true, force: true });
    }
    catch {
        // Ignore if doesn't exist
    }
    await ensureDir(dirPath);
}
/**
 * Read JSON file safely
 */
async function readJson(filePath) {
    try {
        const content = await fs_1.promises.readFile(filePath, 'utf-8');
        return JSON.parse(content);
    }
    catch {
        return null;
    }
}
/**
 * Write JSON file with pretty formatting
 */
async function writeJson(filePath, data) {
    const content = JSON.stringify(data, null, 2);
    await writeFile(filePath, content);
}
//# sourceMappingURL=fsx.js.map