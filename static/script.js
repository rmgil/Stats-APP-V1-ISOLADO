// DOM elements
const dropzone = document.getElementById('dropzone');
const fileInput = document.getElementById('fileInput');
const statusArea = document.getElementById('statusArea');
const statusText = document.getElementById('statusText');
const successArea = document.getElementById('successArea');
const errorArea = document.getElementById('errorArea');
const errorText = document.getElementById('errorText');

// Hide all status areas initially
function hideAllStatus() {
    statusArea.classList.add('d-none');
    successArea.classList.add('d-none');
    errorArea.classList.add('d-none');
}

// Show processing status
function showProcessing(message = 'A processar ficheiro...') {
    hideAllStatus();
    statusText.textContent = message;
    statusArea.classList.remove('d-none');
}

// Show success status
function showSuccess(message = 'Processamento concluído!') {
    hideAllStatus();
    successArea.classList.remove('d-none');
}

// Show error status
function showError(message) {
    hideAllStatus();
    errorText.textContent = message;
    errorArea.classList.remove('d-none');
}

// Validate file type
function isValidFile(file) {
    const validTypes = ['application/zip', 'application/x-rar-compressed', 'application/x-rar'];
    const validExtensions = ['.zip', '.rar'];
    
    const hasValidType = validTypes.includes(file.type);
    const hasValidExtension = validExtensions.some(ext => 
        file.name.toLowerCase().endsWith(ext)
    );
    
    return hasValidType || hasValidExtension;
}

// Handle file upload
function uploadFile(file, dryRun = true) {
    if (!file) {
        showError('Nenhum ficheiro selecionado');
        return;
    }

    // Validate file type
    if (!isValidFile(file)) {
        showError('Formato de ficheiro não suportado. Use apenas ZIP ou RAR.');
        return;
    }

    // Check if we should use chunked upload for large files
    const maxDirectUpload = 50 * 1024 * 1024; // 50MB - more conservative limit
    
    if (file.size > maxDirectUpload && typeof uploadLargeFile === 'function') {
        // Use chunked upload for large files
        showProcessing(`Ficheiro grande detectado (${(file.size / (1024*1024)).toFixed(1)}MB). A usar upload em partes...`);
        uploadLargeFile(file).catch(error => {
            console.error('Chunked upload error:', error);
            showError(`Erro no upload: ${error.message}`);
        });
        return;
    }

    // Show processing status
    showProcessing(dryRun ? 'A analisar ficheiro...' : 'A processar ficheiro...');

    // Create form data
    const formData = new FormData();
    formData.append('file', file);

    // Upload file with dry_run parameter
    const url = dryRun ? '/upload?dry_run=true' : '/upload';
    
    fetch(url, {
        method: 'POST',
        body: formData
    })
    .then(async response => {
        // Check if response is JSON (error) or blob (success)
        const contentType = response.headers.get('content-type');
        
        if (!response.ok) {
            // Check content type first to avoid reading body twice
            if (contentType && contentType.includes('application/json')) {
                try {
                    const data = await response.json();
                    throw new Error(data.error || `Erro HTTP ${response.status}`);
                } catch (e) {
                    if (response.status === 413) {
                        throw new Error(`Ficheiro muito grande. Por favor, use um ficheiro menor que 50MB ou aguarde o upload em partes.`);
                    }
                    throw new Error(`Erro no servidor (${response.status}). Tente novamente.`);
                }
            } else {
                // Non-JSON error (probably HTML)
                if (response.status === 413) {
                    throw new Error(`Ficheiro muito grande. Por favor, use um ficheiro menor que 50MB.`);
                }
                throw new Error(`Erro no servidor (${response.status}). Tente novamente.`);
            }
        }
        
        if (contentType && contentType.includes('application/json')) {
            // Check if it's a dry_run response or error
            const data = await response.json();
            if (data.dry_run) {
                // Handle dry_run response
                showDryRunResults(data, file);
                return;
            }
            throw new Error(data.error || `Erro no processamento`);
        }
        
        showProcessing('A processar ficheiros TXT e XML...');
        return response.blob();
    })
    .then(blob => {
        if (!blob) return; // Skip if dry_run
        
        showProcessing('A preparar download...');
        
        // Create download link
        const url = window.URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.style.display = 'none';
        a.href = url;
        
        // Use original filename + "_separada" 
        const originalName = file.name.replace(/\.[^/.]+$/, ""); // Remove extension
        a.download = `${originalName}_separada.zip`;
        
        document.body.appendChild(a);
        a.click();
        
        // Cleanup
        window.URL.revokeObjectURL(url);
        document.body.removeChild(a);
        
        // Show success
        showSuccess('Download concluído! Os ficheiros foram filtrados com sucesso.');
    })
    .catch(error => {
        console.error('Upload error:', error);
        showError(`Erro no processamento: ${error.message}`);
    });
}

// Drag and drop event handlers
['dragenter', 'dragover'].forEach(eventName => {
    dropzone.addEventListener(eventName, (e) => {
        e.preventDefault();
        dropzone.classList.add('dragover');
    });
});

['dragleave', 'drop'].forEach(eventName => {
    dropzone.addEventListener(eventName, (e) => {
        e.preventDefault();
        dropzone.classList.remove('dragover');
    });
});

// Handle drop event
dropzone.addEventListener('drop', (e) => {
    e.preventDefault();
    
    const files = e.dataTransfer.files;
    if (files.length > 0) {
        uploadFile(files[0], true);  // Use dry_run by default
    }
});

// Store current file and dry run data
let currentFile = null;
let currentDryRunData = null;

// Show dry run results
function showDryRunResults(data, file) {
    hideAllStatus();
    
    currentFile = file;
    currentDryRunData = data;
    
    // Create results area if it doesn't exist
    let resultsArea = document.getElementById('dryRunResults');
    if (!resultsArea) {
        resultsArea = document.createElement('div');
        resultsArea.id = 'dryRunResults';
        resultsArea.className = 'alert alert-info mt-3';
        const successArea = document.getElementById('successArea');
        successArea.parentNode.insertBefore(resultsArea, successArea.nextSibling);
    }
    
    const stats = data.stats || {};
    const manifest = data.manifest || {};
    
    resultsArea.innerHTML = `
        <h5>📊 Análise do Ficheiro Concluída</h5>
        <div class="row mt-3">
            <div class="col-md-6">
                <strong>Ficheiro:</strong> ${file.name}<br>
                <strong>Total de ficheiros:</strong> ${stats.total_files || 0}<br>
                <strong>Ficheiros TXT/XML:</strong> ${stats.total_txt || 0}
            </div>
            <div class="col-md-6">
                <strong>PKO:</strong> ${stats.pko || 0} ficheiros<br>
                <strong>NON-KO:</strong> ${stats.nonko || 0} ficheiros<br>
                <strong>MYSTERIES:</strong> ${stats.mystery || 0} ficheiros
            </div>
        </div>
        <div class="mt-3">
            <button class="btn btn-success me-2" onclick="sendToPipeline()">
                <i class="fas fa-paper-plane me-2"></i>Enviar para Pipeline
            </button>
            <button class="btn btn-primary" onclick="processNow()">
                <i class="fas fa-download me-2"></i>Processar e Baixar Agora
            </button>
        </div>
    `;
    
    resultsArea.classList.remove('d-none');
}

// Send to pipeline for full processing
function sendToPipeline() {
    if (!currentFile || !currentDryRunData) {
        showError('Nenhum ficheiro para processar');
        return;
    }
    
    // Redirect to pipeline page with session data
    window.location.href = `/pipeline?session_id=${currentDryRunData.session_id}`;
}

// Process file now (without dry_run)
function processNow() {
    if (!currentFile) {
        showError('Nenhum ficheiro para processar');
        return;
    }
    
    // Re-upload without dry_run
    uploadFile(currentFile, false);
}

// Handle click on dropzone
dropzone.addEventListener('click', () => {
    fileInput.click();
});

// Handle file input change
fileInput.addEventListener('change', (e) => {
    if (e.target.files.length > 0) {
        uploadFile(e.target.files[0], true);  // Use dry_run by default
    }
});

// Prevent default drag behavior on document
document.addEventListener('dragover', (e) => {
    e.preventDefault();
});

document.addEventListener('drop', (e) => {
    e.preventDefault();
});

// CSV Merger functionality
const csvFiles = {
    '9max': null,
    '6max': null,
    'pko': null,
    'postflop': null
};

const csvDropzones = document.querySelectorAll('.csv-dropzone');
const processCsvBtn = document.getElementById('processCsvBtn');
const csvStatusArea = document.getElementById('csvStatusArea');
const csvStatusText = document.getElementById('csvStatusText');
const csvSuccessArea = document.getElementById('csvSuccessArea');
const csvErrorArea = document.getElementById('csvErrorArea');
const csvErrorText = document.getElementById('csvErrorText');

// Hide all CSV status areas
function hideAllCsvStatus() {
    csvStatusArea.classList.add('d-none');
    csvSuccessArea.classList.add('d-none');
    csvErrorArea.classList.add('d-none');
}

// Show CSV processing status
function showCsvProcessing(message = 'A processar ficheiros CSV...') {
    hideAllCsvStatus();
    csvStatusText.textContent = message;
    csvStatusArea.classList.remove('d-none');
}

// Show CSV success status
function showCsvSuccess(message = 'CSV combinado com sucesso!') {
    hideAllCsvStatus();
    csvSuccessArea.classList.remove('d-none');
}

// Show CSV error status
function showCsvError(message) {
    hideAllCsvStatus();
    csvErrorText.textContent = message;
    csvErrorArea.classList.remove('d-none');
}

// Validate CSV file
function isValidCsvFile(file) {
    const validTypes = ['text/csv', 'application/csv', 'text/plain'];
    const validExtensions = ['.csv'];
    
    const hasValidType = validTypes.includes(file.type);
    const hasValidExtension = validExtensions.some(ext => 
        file.name.toLowerCase().endsWith(ext)
    );
    
    return hasValidType || hasValidExtension;
}

// Update process button state
function updateProcessButton() {
    const allFilesPresent = Object.values(csvFiles).every(file => file !== null);
    processCsvBtn.disabled = !allFilesPresent;
}

// Handle CSV file upload
function handleCsvFile(file, type) {
    if (!isValidCsvFile(file)) {
        showCsvError('Formato de ficheiro não suportado. Use apenas ficheiros CSV.');
        return;
    }

    // Store the file
    csvFiles[type] = file;
    
    // Update the dropzone UI
    const dropzone = document.querySelector(`[data-type="${type}"]`);
    const fileStatus = dropzone.querySelector('.file-status');
    
    dropzone.classList.add('has-file');
    fileStatus.textContent = file.name;
    
    // Update process button
    updateProcessButton();
}

// Setup CSV dropzones
csvDropzones.forEach(dropzone => {
    const type = dropzone.dataset.type;
    
    // Drag and drop events
    ['dragenter', 'dragover'].forEach(eventName => {
        dropzone.addEventListener(eventName, (e) => {
            e.preventDefault();
            dropzone.classList.add('dragover');
        });
    });
    
    ['dragleave', 'drop'].forEach(eventName => {
        dropzone.addEventListener(eventName, (e) => {
            e.preventDefault();
            dropzone.classList.remove('dragover');
        });
    });
    
    // Handle drop
    dropzone.addEventListener('drop', (e) => {
        e.preventDefault();
        const files = e.dataTransfer.files;
        if (files.length > 0) {
            handleCsvFile(files[0], type);
        }
    });
    
    // Handle click (for file picker)
    dropzone.addEventListener('click', () => {
        const input = document.createElement('input');
        input.type = 'file';
        input.accept = '.csv';
        input.onchange = (e) => {
            if (e.target.files.length > 0) {
                handleCsvFile(e.target.files[0], type);
            }
        };
        input.click();
    });
});

// Process CSV files
processCsvBtn.addEventListener('click', () => {
    showCsvProcessing('A combinar ficheiros CSV...');
    
    // Create form data
    const formData = new FormData();
    Object.entries(csvFiles).forEach(([type, file]) => {
        if (file) {
            formData.append(type, file);
        }
    });
    
    // Submit to server
    fetch('/merge-csv', {
        method: 'POST',
        body: formData
    })
    .then(response => {
        if (!response.ok) {
            throw new Error(`Erro HTTP ${response.status}: ${response.statusText}`);
        }
        return response.json();
    })
    .then(data => {
        if (data.success) {
            // Display the data in a table
            displayCsvResults(data.headers, data.data);
            showCsvSuccess('Ficheiros CSV combinados com sucesso! Dados exibidos abaixo.');
        } else {
            throw new Error(data.error || 'Erro desconhecido');
        }
    })
    .catch(error => {
        console.error('CSV merge error:', error);
        showCsvError(`Erro no processamento: ${error.message}`);
    });
});

// Display CSV results in a table
function displayCsvResults(headers, data) {
    // Find or create results container
    let resultsContainer = document.getElementById('csvResults');
    if (!resultsContainer) {
        resultsContainer = document.createElement('div');
        resultsContainer.id = 'csvResults';
        resultsContainer.className = 'mt-4';
        
        // Insert after the CSV success area
        const csvSuccessArea = document.getElementById('csvSuccessArea');
        csvSuccessArea.parentNode.insertBefore(resultsContainer, csvSuccessArea.nextSibling);
    }
    
    // Clear previous results
    resultsContainer.innerHTML = '';
    
    // Create card container
    const card = document.createElement('div');
    card.className = 'card border-0 shadow-sm';
    
    const cardBody = document.createElement('div');
    cardBody.className = 'card-body';
    
    const cardTitle = document.createElement('h5');
    cardTitle.className = 'card-title d-flex align-items-center justify-content-between';
    cardTitle.innerHTML = `
        <span><i class="fas fa-table me-2"></i>Dados Combinados</span>
        <button class="btn btn-sm btn-outline-primary" onclick="copyTableData()">
            <i class="fas fa-copy me-1"></i>Copiar Dados
        </button>
    `;
    
    // Create scrollable table container
    const tableContainer = document.createElement('div');
    tableContainer.className = 'table-responsive';
    tableContainer.style.maxHeight = '400px';
    tableContainer.style.overflowY = 'auto';
    
    // Create table
    const table = document.createElement('table');
    table.className = 'table table-sm table-striped table-hover';
    table.id = 'csvDataTable';
    
    // Create header row
    const thead = document.createElement('thead');
    thead.className = 'table-dark sticky-top';
    const headerRow = document.createElement('tr');
    
    headers.forEach(header => {
        const th = document.createElement('th');
        th.textContent = header;
        th.style.fontSize = '0.8rem';
        th.style.whiteSpace = 'nowrap';
        headerRow.appendChild(th);
    });
    
    thead.appendChild(headerRow);
    table.appendChild(thead);
    
    // Create data row
    const tbody = document.createElement('tbody');
    const dataRow = document.createElement('tr');
    
    data.forEach(value => {
        const td = document.createElement('td');
        td.textContent = value;
        td.style.fontSize = '0.8rem';
        td.style.whiteSpace = 'nowrap';
        dataRow.appendChild(td);
    });
    
    tbody.appendChild(dataRow);
    table.appendChild(tbody);
    
    // Assemble the structure
    tableContainer.appendChild(table);
    cardBody.appendChild(cardTitle);
    cardBody.appendChild(tableContainer);
    card.appendChild(cardBody);
    resultsContainer.appendChild(card);
    
    // Scroll to results
    setTimeout(() => {
        resultsContainer.scrollIntoView({ behavior: 'smooth' });
    }, 100);
}

// Copy table data to clipboard
function copyTableData() {
    const table = document.getElementById('csvDataTable');
    if (!table) return;
    
    const rows = table.querySelectorAll('tbody tr');
    const rowData = [];
    
    rows.forEach(row => {
        const cells = row.querySelectorAll('td');
        const cellValues = [];
        cells.forEach(cell => {
            cellValues.push(cell.textContent);
        });
        rowData.push(cellValues.join('\t')); // Join cells with tabs
    });
    
    // Join rows with newlines for proper Excel formatting
    const textToCopy = rowData.join('\n');
    
    navigator.clipboard.writeText(textToCopy).then(() => {
        // Show feedback
        const copyBtn = document.querySelector('[onclick="copyTableData()"]');
        const originalText = copyBtn.innerHTML;
        copyBtn.innerHTML = '<i class="fas fa-check me-1"></i>Copiado!';
        copyBtn.classList.remove('btn-outline-primary');
        copyBtn.classList.add('btn-success');
        
        setTimeout(() => {
            copyBtn.innerHTML = originalText;
            copyBtn.classList.remove('btn-success');
            copyBtn.classList.add('btn-outline-primary');
        }, 2000);
    }).catch(err => {
        console.error('Error copying to clipboard:', err);
        showCsvError('Erro ao copiar dados para a área de transferência');
    });
}

// ========== ROOM REPORT FUNCTIONALITY ==========

// Room report status functions
function hideAllRoomStatus() {
    document.getElementById('room-processing').style.display = 'none';
    document.getElementById('room-success').style.display = 'none';
    document.getElementById('room-error').style.display = 'none';
    document.getElementById('room-results').style.display = 'none';
}

function showRoomProcessing(message = 'A processar ficheiro CSV...') {
    hideAllRoomStatus();
    const processing = document.getElementById('room-processing');
    processing.querySelector('span').textContent = message;
    processing.style.display = 'block';
}

function showRoomSuccess(message = 'CSV processado com sucesso!') {
    hideAllRoomStatus();
    const success = document.getElementById('room-success');
    success.querySelector('span').textContent = message;
    success.style.display = 'block';
    document.getElementById('room-results').style.display = 'block';
}

function showRoomError(message) {
    hideAllRoomStatus();
    const error = document.getElementById('room-error');
    error.querySelector('span').textContent = message;
    error.style.display = 'block';
}

function isValidRoomCsvFile(file) {
    const validTypes = ['text/csv', 'application/vnd.ms-excel'];
    const validExtensions = ['.csv'];
    
    const hasValidType = validTypes.includes(file.type);
    const hasValidExtension = validExtensions.some(ext => file.name.toLowerCase().endsWith(ext));
    
    return hasValidType || hasValidExtension;
}

function updateRoomProcessButton() {
    const roomFile = document.getElementById('room-csv-input').files[0];
    const processBtn = document.getElementById('process-room-btn');
    
    processBtn.disabled = !roomFile;
}

function handleRoomCsvFile(file) {
    if (!isValidRoomCsvFile(file)) {
        showRoomError('Por favor, selecione um ficheiro CSV válido.');
        return;
    }
    
    const fileInfo = document.getElementById('room-file-info');
    fileInfo.innerHTML = `
        <div class="d-flex align-items-center">
            <i class="fas fa-file-csv text-success me-2"></i>
            <span><strong>${file.name}</strong> (${(file.size / 1024).toFixed(1)} KB)</span>
        </div>
    `;
    fileInfo.style.display = 'block';
    
    updateRoomProcessButton();
}

// Room CSV drag and drop - Initialize when DOM is loaded
document.addEventListener('DOMContentLoaded', function() {
    const roomDropZone = document.getElementById('room-drop-zone');
    const roomCsvInput = document.getElementById('room-csv-input');
    
    if (roomDropZone && roomCsvInput) {
        // Click to select file
        roomDropZone.addEventListener('click', function() {
            roomCsvInput.click();
        });

        // File input change
        roomCsvInput.addEventListener('change', function(e) {
            const file = e.target.files[0];
            if (file) {
                handleRoomCsvFile(file);
            }
        });

        // Drag over
        roomDropZone.addEventListener('dragenter', function(e) {
            e.preventDefault();
            e.stopPropagation();
            this.classList.add('drag-over');
        });

        roomDropZone.addEventListener('dragover', function(e) {
            e.preventDefault();
            e.stopPropagation();
            this.classList.add('drag-over');
        });

        // Drag leave
        roomDropZone.addEventListener('dragleave', function(e) {
            e.preventDefault();
            e.stopPropagation();
            // Only remove class if we're leaving the drop zone completely
            if (!this.contains(e.relatedTarget)) {
                this.classList.remove('drag-over');
            }
        });

        // Drop
        roomDropZone.addEventListener('drop', function(e) {
            e.preventDefault();
            e.stopPropagation();
            this.classList.remove('drag-over');
            
            const files = e.dataTransfer.files;
            if (files.length > 0) {
                const file = files[0];
                // Create a new FileList-like object for the input
                const dt = new DataTransfer();
                dt.items.add(file);
                roomCsvInput.files = dt.files;
                handleRoomCsvFile(file);
            }
        });
    }
});

// Process room CSV
document.getElementById('process-room-btn').addEventListener('click', function() {
    const file = document.getElementById('room-csv-input').files[0];
    
    if (!file) {
        showRoomError('Por favor, selecione um ficheiro CSV.');
        return;
    }
    
    showRoomProcessing('A processar ficheiro CSV...');
    
    const formData = new FormData();
    formData.append('file', file);
    
    fetch('/process-room-csv', {
        method: 'POST',
        body: formData
    })
    .then(response => {
        if (!response.ok) {
            throw new Error(`Erro HTTP ${response.status}: ${response.statusText}`);
        }
        return response.json();
    })
    .then(data => {
        if (data.success) {
            displayRoomResults(data.headers, data.data);
            showRoomSuccess('CSV processado com sucesso! Dados formatados exibidos abaixo.');
        } else {
            throw new Error(data.error || 'Erro desconhecido');
        }
    })
    .catch(error => {
        console.error('Room CSV processing error:', error);
        showRoomError(`Erro no processamento: ${error.message}`);
    });
});

// Display room CSV results
function displayRoomResults(headers, data) {
    const headersRow = document.getElementById('room-headers-row');
    const dataBody = document.getElementById('room-data-body');
    
    // Clear previous results
    headersRow.innerHTML = '';
    dataBody.innerHTML = '';
    
    // Add headers
    headers.forEach(header => {
        const th = document.createElement('th');
        th.textContent = header;
        th.className = 'text-nowrap';
        headersRow.appendChild(th);
    });
    
    // Add data rows
    for (let i = 0; i < data.length; i += headers.length) {
        const row = document.createElement('tr');
        
        for (let j = 0; j < headers.length; j++) {
            const td = document.createElement('td');
            td.textContent = data[i + j] || '';
            td.className = 'text-nowrap';
            row.appendChild(td);
        }
        
        dataBody.appendChild(row);
    }
}

// Copy room data to clipboard
document.getElementById('copy-room-data').addEventListener('click', function() {
    const table = document.getElementById('room-data-table');
    if (!table) return;
    
    const rows = table.querySelectorAll('tbody tr');
    const rowData = [];
    
    rows.forEach(row => {
        const cells = row.querySelectorAll('td');
        const cellValues = [];
        cells.forEach(cell => {
            cellValues.push(cell.textContent);
        });
        rowData.push(cellValues.join('\t')); // Join cells with tabs
    });
    
    // Join rows with newlines for proper Excel formatting
    const textToCopy = rowData.join('\n');
    
    navigator.clipboard.writeText(textToCopy).then(() => {
        // Show feedback
        const copyBtn = document.getElementById('copy-room-data');
        const originalText = copyBtn.innerHTML;
        copyBtn.innerHTML = '<i class="fas fa-check me-2"></i>Copiado!';
        copyBtn.classList.remove('btn-secondary');
        copyBtn.classList.add('btn-success');
        
        setTimeout(() => {
            copyBtn.innerHTML = originalText;
            copyBtn.classList.remove('btn-success');
            copyBtn.classList.add('btn-secondary');
        }, 2000);
    }).catch(err => {
        console.error('Error copying to clipboard:', err);
        showRoomError('Erro ao copiar dados para a área de transferência');
    });
});
