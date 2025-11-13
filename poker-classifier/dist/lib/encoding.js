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
exports.detectAndRead = detectAndRead;
exports.encodeContent = encodeContent;
/**
 * Encoding detection and conversion utilities
 */
const chardet = __importStar(require("chardet"));
const iconv = __importStar(require("iconv-lite"));
const promises_1 = require("fs/promises");
/**
 * Detect encoding and read file content with proper conversion
 */
async function detectAndRead(filePath) {
    const buffer = await (0, promises_1.readFile)(filePath);
    // Detect encoding with confidence
    const detectionResult = chardet.analyse(buffer);
    let encoding = 'utf-8';
    let confidence = 1.0;
    if (detectionResult && detectionResult.length > 0) {
        encoding = detectionResult[0].name.toLowerCase();
        confidence = detectionResult[0].confidence / 100;
    }
    else {
        // Fallback detection
        const simpleDetection = chardet.detect(buffer);
        if (simpleDetection) {
            encoding = simpleDetection.toLowerCase();
            confidence = 0.8;
        }
    }
    // Common encoding aliases normalization
    const encodingMap = {
        'windows-1252': 'win1252',
        'cp1252': 'win1252',
        'iso-8859-1': 'latin1',
        'iso-8859-15': 'latin1',
        'utf8': 'utf-8',
        'ascii': 'utf-8'
    };
    const normalizedEncoding = encodingMap[encoding] || encoding;
    // Convert to UTF-8 string for processing
    let content;
    try {
        if (iconv.encodingExists(normalizedEncoding)) {
            content = iconv.decode(buffer, normalizedEncoding);
        }
        else {
            // Fallback to UTF-8 with replacement chars
            content = buffer.toString('utf-8');
            confidence = 0.5;
        }
    }
    catch (error) {
        // Last resort: force UTF-8 and mark low confidence
        content = buffer.toString('utf-8');
        confidence = 0.1;
        console.warn(`Encoding conversion failed for ${filePath}: ${error}`);
    }
    return {
        encoding: normalizedEncoding,
        confidence,
        content,
        originalBytes: buffer
    };
}
/**
 * Write content preserving original encoding
 */
function encodeContent(content, encoding) {
    const normalizedEncoding = encoding === 'utf-8' ? 'utf8' : encoding;
    if (iconv.encodingExists(normalizedEncoding)) {
        return iconv.encode(content, normalizedEncoding);
    }
    // Fallback to UTF-8
    return Buffer.from(content, 'utf-8');
}
//# sourceMappingURL=encoding.js.map