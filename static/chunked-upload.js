// Chunked upload functionality to bypass proxy limits

class ChunkedUploader {
    constructor(file, chunkSize = 25 * 1024 * 1024) { // 25MB chunks for better deployment compatibility
        this.file = file;
        this.chunkSize = chunkSize;
        this.totalChunks = Math.ceil(file.size / chunkSize);
        this.uploadId = this.generateUploadId();
        this.currentChunk = 0;
        this.retryCount = 0;
        this.maxRetries = 3;
    }

    generateUploadId() {
        return Date.now().toString(36) + Math.random().toString(36).substr(2);
    }

    async uploadChunk(chunkIndex) {
        const start = chunkIndex * this.chunkSize;
        const end = Math.min(start + this.chunkSize, this.file.size);
        const chunk = this.file.slice(start, end);

        const formData = new FormData();
        formData.append('chunk', chunk);
        formData.append('uploadId', this.uploadId);
        formData.append('chunkIndex', chunkIndex);
        formData.append('totalChunks', this.totalChunks);
        formData.append('fileName', this.file.name);

        for (let retry = 0; retry <= this.maxRetries; retry++) {
            try {
                const response = await fetch('/upload-chunk', {
                    method: 'POST',
                    body: formData
                });

                if (!response.ok) {
                    throw new Error(`Chunk ${chunkIndex} upload failed: ${response.statusText}`);
                }

                return await response.json();
            } catch (error) {
                if (retry === this.maxRetries) {
                    throw error;
                }
                // Wait before retry (exponential backoff)
                await new Promise(resolve => setTimeout(resolve, Math.pow(2, retry) * 1000));
            }
        }
    }

    async upload(onProgress) {
        try {
            for (let i = 0; i < this.totalChunks; i++) {
                await this.uploadChunk(i);
                this.currentChunk = i + 1;
                
                if (onProgress) {
                    const progress = (this.currentChunk / this.totalChunks) * 100;
                    onProgress(progress, this.currentChunk, this.totalChunks);
                }
            }

            // Finalize upload
            const response = await fetch('/finalize-upload', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({
                    uploadId: this.uploadId,
                    fileName: this.file.name,
                    totalSize: this.file.size
                })
            });

            if (!response.ok) {
                throw new Error(`Failed to finalize upload: ${response.statusText}`);
            }

            return await response.json();
        } catch (error) {
            console.error('Upload failed:', error);
            throw error;
        }
    }
}

// Override the original upload function for large files
function uploadLargeFile(file) {
    const MAX_DIRECT_UPLOAD = 50 * 1024 * 1024; // 50MB - more conservative limit
    
    if (file.size <= MAX_DIRECT_UPLOAD) {
        // Use regular upload for smaller files
        return uploadFileRegular(file);
    } else {
        // Use chunked upload for larger files
        return uploadFileChunked(file);
    }
}

function uploadFileRegular(file) {
    return new Promise((resolve, reject) => {
        const formData = new FormData();
        formData.append('file', file);

        fetch('/upload', {
            method: 'POST',
            body: formData
        })
        .then(response => {
            if (response.ok) {
                return response.blob();
            } else {
                throw new Error(`Upload failed: ${response.statusText}`);
            }
        })
        .then(blob => {
            // Handle download
            const url = window.URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.style.display = 'none';
            a.href = url;
            a.download = file.name.replace(/\.[^/.]+$/, '') + '_separada.zip';
            document.body.appendChild(a);
            a.click();
            window.URL.revokeObjectURL(url);
            document.body.removeChild(a);
            resolve();
        })
        .catch(reject);
    });
}

function uploadFileChunked(file) {
    return new Promise((resolve, reject) => {
        const uploader = new ChunkedUploader(file);
        
        showProcessing(`A fazer upload de ${file.name} (${(file.size / (1024*1024)).toFixed(1)} MB)...`);
        
        uploader.upload((progress, current, total) => {
            const progressBar = `
                <div class="progress mb-2" style="height: 20px;">
                    <div class="progress-bar" role="progressbar" style="width: ${progress}%" 
                         aria-valuenow="${progress}" aria-valuemin="0" aria-valuemax="100">
                        ${progress.toFixed(1)}%
                    </div>
                </div>
                <div><strong>A enviar:</strong> chunk ${current}/${total}</div>
            `;
            document.getElementById('statusText').innerHTML = progressBar;
        })
        .then(result => {
            if (result.success) {
                showProcessing('A processar ficheiro...');
                // Download the result
                window.location.href = `/download-result/${result.uploadId}`;
                showSuccess('Ficheiro processado com sucesso!');
            } else {
                throw new Error(result.error || 'Upload failed');
            }
            resolve();
        })
        .catch(error => {
            console.error('Chunked upload failed:', error);
            let errorMessage = 'Erro no upload';
            if (error.message) {
                if (error.message.includes('Chunk 0 upload failed')) {
                    errorMessage = 'Erro no primeiro chunk - pode ser um problema de conectividade ou limite do servidor';
                } else if (error.message.includes('upload failed')) {
                    errorMessage = 'Erro no upload - verifique a conexão e tente novamente';
                } else {
                    errorMessage += ': ' + error.message;
                }
            }
            showError(errorMessage);
            reject(error);
        });
    });
}