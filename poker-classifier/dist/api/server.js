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
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
/**
 * REST API server for tournament classification
 */
const express_1 = __importDefault(require("express"));
const multer_1 = __importDefault(require("multer"));
const path = __importStar(require("path"));
const fs = __importStar(require("fs/promises"));
const uuid_1 = require("uuid");
const pipeline_1 = require("../classify/pipeline");
const fsx_1 = require("../lib/fsx");
const app = (0, express_1.default)();
const PORT = process.env.PORT || 3000;
// Configure multer for file uploads
const upload = (0, multer_1.default)({
    dest: path.join(__dirname, '../../temp/uploads'),
    limits: {
        fileSize: 500 * 1024 * 1024, // 500MB max
        files: 1
    }
});
// Ensure temp directories exist
async function initDirectories() {
    const dirs = [
        path.join(__dirname, '../../temp/uploads'),
        path.join(__dirname, '../../temp/processing'),
        path.join(__dirname, '../../temp/outputs')
    ];
    for (const dir of dirs) {
        await fs.mkdir(dir, { recursive: true });
    }
}
// Middleware
app.use(express_1.default.json());
app.use(express_1.default.static('public'));
// CORS for development
app.use((req, res, next) => {
    res.header('Access-Control-Allow-Origin', '*');
    res.header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
    res.header('Access-Control-Allow-Headers', 'Content-Type');
    next();
});
/**
 * Health check endpoint
 */
app.get('/health', (req, res) => {
    res.json({
        status: 'healthy',
        service: 'poker-tournament-classifier',
        version: '1.0.0',
        timestamp: new Date().toISOString()
    });
});
/**
 * Classification endpoint
 * Accepts a ZIP file containing .txt tournament files
 */
app.post('/classify', upload.single('archive'), async (req, res) => {
    const sessionId = (0, uuid_1.v4)();
    const processingDir = path.join(__dirname, '../../temp/processing', sessionId);
    const outputZipPath = path.join(__dirname, '../../temp/outputs', `${sessionId}.zip`);
    try {
        if (!req.file) {
            return res.status(400).json({
                error: 'No file uploaded',
                message: 'Please upload a ZIP file containing tournament text files'
            });
        }
        console.log(`Processing upload: ${req.file.originalname} (${sessionId})`);
        // Extract uploaded archive
        const uploadedPath = req.file.path;
        await (0, fsx_1.cleanDir)(processingDir);
        // Use Node.js built-in unzip or call system unzip
        const { exec } = require('child_process');
        await new Promise((resolve, reject) => {
            exec(`unzip -q "${uploadedPath}" -d "${processingDir}"`, (error) => {
                if (error) {
                    // Try with tar if it's not a zip
                    exec(`tar -xf "${uploadedPath}" -C "${processingDir}"`, (error2) => {
                        if (error2)
                            reject(new Error('Failed to extract archive'));
                        else
                            resolve(null);
                    });
                }
                else {
                    resolve(null);
                }
            });
        });
        // Run classification pipeline
        const manifest = await (0, pipeline_1.runClassificationPipeline)(processingDir, outputZipPath, { verbose: true });
        // Clean up uploaded file and processing directory
        await fs.unlink(uploadedPath);
        await (0, fsx_1.cleanDir)(processingDir);
        // Send result
        res.json({
            success: true,
            sessionId,
            manifest,
            downloadUrl: `/download/${sessionId}`
        });
    }
    catch (error) {
        console.error(`Error processing ${sessionId}:`, error);
        // Clean up on error
        try {
            if (req.file)
                await fs.unlink(req.file.path);
            await (0, fsx_1.cleanDir)(processingDir);
        }
        catch { }
        res.status(500).json({
            error: 'Processing failed',
            message: error.message || 'An error occurred during classification',
            sessionId
        });
    }
});
/**
 * Download classified results
 */
app.get('/download/:sessionId', async (req, res) => {
    const { sessionId } = req.params;
    const outputPath = path.join(__dirname, '../../temp/outputs', `${sessionId}.zip`);
    try {
        await fs.access(outputPath);
        res.download(outputPath, `classified_${sessionId}.zip`, async (err) => {
            if (!err) {
                // Clean up after successful download
                setTimeout(async () => {
                    try {
                        await fs.unlink(outputPath);
                    }
                    catch { }
                }, 60000); // Delete after 1 minute
            }
        });
    }
    catch {
        res.status(404).json({
            error: 'File not found',
            message: 'The requested classification result was not found or has expired'
        });
    }
});
/**
 * Get classification manifest
 */
app.get('/manifest/:sessionId', async (req, res) => {
    const { sessionId } = req.params;
    const outputPath = path.join(__dirname, '../../temp/outputs', `${sessionId}.zip`);
    try {
        // Check if result exists
        await fs.access(outputPath);
        // Extract and read manifest from ZIP
        const { exec } = require('child_process');
        const manifestContent = await new Promise((resolve, reject) => {
            exec(`unzip -p "${outputPath}" classification_manifest.json`, { encoding: 'utf8' }, (error, stdout) => {
                if (error)
                    reject(error);
                else
                    resolve(stdout);
            });
        });
        res.json(JSON.parse(manifestContent));
    }
    catch {
        res.status(404).json({
            error: 'Manifest not found',
            message: 'The requested classification manifest was not found'
        });
    }
});
/**
 * Simple web UI
 */
app.get('/', (req, res) => {
    res.send(`
    <!DOCTYPE html>
    <html>
    <head>
      <title>Poker Tournament Classifier</title>
      <style>
        body { font-family: Arial, sans-serif; max-width: 800px; margin: 0 auto; padding: 20px; }
        .upload-area { border: 2px dashed #ccc; border-radius: 8px; padding: 40px; text-align: center; margin: 20px 0; }
        .upload-area.dragover { background: #f0f0f0; border-color: #666; }
        button { background: #007bff; color: white; border: none; padding: 10px 20px; border-radius: 4px; cursor: pointer; }
        button:hover { background: #0056b3; }
        .results { margin-top: 20px; padding: 20px; background: #f8f9fa; border-radius: 8px; }
        .error { color: #dc3545; }
        .success { color: #28a745; }
        pre { background: #fff; padding: 10px; border-radius: 4px; overflow-x: auto; }
      </style>
    </head>
    <body>
      <h1>Poker Tournament Classifier</h1>
      <p>Upload a ZIP file containing tournament text files to classify them as PKO, Mystery, or Non-KO tournaments.</p>
      
      <div class="upload-area" id="uploadArea">
        <p>Drag and drop your ZIP file here, or click to select</p>
        <input type="file" id="fileInput" accept=".zip" style="display: none;">
        <button onclick="document.getElementById('fileInput').click()">Select File</button>
      </div>
      
      <div id="results" style="display: none;" class="results">
        <h3>Classification Results</h3>
        <div id="resultContent"></div>
      </div>
      
      <script>
        const uploadArea = document.getElementById('uploadArea');
        const fileInput = document.getElementById('fileInput');
        const results = document.getElementById('results');
        const resultContent = document.getElementById('resultContent');
        
        // Drag and drop
        uploadArea.addEventListener('dragover', (e) => {
          e.preventDefault();
          uploadArea.classList.add('dragover');
        });
        
        uploadArea.addEventListener('dragleave', () => {
          uploadArea.classList.remove('dragover');
        });
        
        uploadArea.addEventListener('drop', (e) => {
          e.preventDefault();
          uploadArea.classList.remove('dragover');
          const file = e.dataTransfer.files[0];
          if (file) uploadFile(file);
        });
        
        fileInput.addEventListener('change', (e) => {
          const file = e.target.files[0];
          if (file) uploadFile(file);
        });
        
        async function uploadFile(file) {
          const formData = new FormData();
          formData.append('archive', file);
          
          results.style.display = 'block';
          resultContent.innerHTML = '<p>Processing...</p>';
          
          try {
            const response = await fetch('/classify', {
              method: 'POST',
              body: formData
            });
            
            const data = await response.json();
            
            if (data.success) {
              resultContent.innerHTML = \`
                <p class="success">Classification complete!</p>
                <p><strong>Session ID:</strong> \${data.sessionId}</p>
                <p><strong>Results:</strong></p>
                <pre>\${JSON.stringify(data.manifest.totals, null, 2)}</pre>
                <p><a href="\${data.downloadUrl}" download>Download Results</a></p>
              \`;
            } else {
              resultContent.innerHTML = \`<p class="error">Error: \${data.message}</p>\`;
            }
          } catch (error) {
            resultContent.innerHTML = \`<p class="error">Upload failed: \${error.message}</p>\`;
          }
        }
      </script>
    </body>
    </html>
  `);
});
// Start server
initDirectories().then(() => {
    app.listen(PORT, () => {
        console.log(`Poker Tournament Classifier API running on port ${PORT}`);
        console.log(`Health check: http://localhost:${PORT}/health`);
        console.log(`Web UI: http://localhost:${PORT}/`);
    });
}).catch(console.error);
//# sourceMappingURL=server.js.map