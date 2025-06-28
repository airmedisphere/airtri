// Transcoding functionality
let transcodeFormats = null;
let currentTranscodeId = null;
let transcodeProgressInterval = null;

// Initialize transcoding features
async function initializeTranscoding() {
    try {
        const response = await postJson('/api/getTranscodeFormats', {});
        if (response.status === 'ok') {
            transcodeFormats = response.data;
            console.log('Transcode formats loaded:', transcodeFormats);
        }
    } catch (error) {
        console.error('Failed to load transcode formats:', error);
    }
}

// Show transcode modal
async function showTranscodeModal(filePath, fileName) {
    if (!transcodeFormats) {
        await initializeTranscoding();
    }

    if (!transcodeFormats) {
        alert('Transcoding not available. Please try again later.');
        return;
    }

    // Create transcode modal
    const modalHtml = `
        <div id="transcode-modal" class="create-new-folder">
            <span>Transcode Video</span>
            <div class="transcode-file-info">
                <strong>File:</strong> ${fileName}
            </div>
            
            <div class="transcode-form">
                <div class="form-group">
                    <label for="transcode-format">Output Format:</label>
                    <select id="transcode-format" class="form-control">
                        ${Object.entries(transcodeFormats.formats).map(([key, format]) => 
                            `<option value="${key}">${key.toUpperCase()} (${format.extension})</option>`
                        ).join('')}
                    </select>
                </div>
                
                <div class="form-group">
                    <label for="transcode-quality">Quality:</label>
                    <select id="transcode-quality" class="form-control">
                        ${Object.entries(transcodeFormats.qualities).map(([key, quality]) => 
                            `<option value="${key}">${quality.description}</option>`
                        ).join('')}
                    </select>
                </div>
                
                <div class="form-group">
                    <label for="transcode-speed">Encoding Speed:</label>
                    <select id="transcode-speed" class="form-control">
                        ${Object.entries(transcodeFormats.speed_presets).map(([key, description]) => 
                            `<option value="${key}" ${key === 'fast' ? 'selected' : ''}>${key} - ${description}</option>`
                        ).join('')}
                    </select>
                </div>
                
                <div class="form-group">
                    <label>
                        <input type="checkbox" id="get-video-info"> 
                        Show video information first
                    </label>
                </div>
            </div>
            
            <div id="video-info-display" class="video-info-display" style="display: none;">
                <!-- Video info will be displayed here -->
            </div>
            
            <div class="modal-actions">
                <button id="transcode-cancel">Cancel</button>
                <button id="get-info-btn">Get Info</button>
                <button id="transcode-start" class="primary-btn">Start Transcode</button>
            </div>
        </div>
    `;

    // Remove existing modal if any
    const existingModal = document.getElementById('transcode-modal');
    if (existingModal) {
        existingModal.remove();
    }

    // Add modal to page
    document.body.insertAdjacentHTML('beforeend', modalHtml);

    // Show modal
    document.getElementById('bg-blur').style.zIndex = '2';
    document.getElementById('bg-blur').style.opacity = '0.1';
    document.getElementById('transcode-modal').style.zIndex = '3';
    document.getElementById('transcode-modal').style.opacity = '1';

    // Add event listeners
    document.getElementById('transcode-cancel').addEventListener('click', closeTranscodeModal);
    document.getElementById('get-info-btn').addEventListener('click', () => getVideoInfo(filePath));
    document.getElementById('transcode-start').addEventListener('click', () => startTranscode(filePath));

    // Show/hide get info button based on checkbox
    const getInfoCheckbox = document.getElementById('get-video-info');
    const getInfoBtn = document.getElementById('get-info-btn');
    
    getInfoCheckbox.addEventListener('change', function() {
        getInfoBtn.style.display = this.checked ? 'inline-block' : 'none';
    });
}

// Close transcode modal
function closeTranscodeModal() {
    document.getElementById('bg-blur').style.opacity = '0';
    setTimeout(() => {
        document.getElementById('bg-blur').style.zIndex = '-1';
    }, 300);
    
    const modal = document.getElementById('transcode-modal');
    if (modal) {
        modal.style.opacity = '0';
        setTimeout(() => {
            modal.remove();
        }, 300);
    }
}

// Get video information
async function getVideoInfo(filePath) {
    const infoDisplay = document.getElementById('video-info-display');
    const getInfoBtn = document.getElementById('get-info-btn');
    
    try {
        getInfoBtn.textContent = 'Getting Info...';
        getInfoBtn.disabled = true;
        
        const response = await postJson('/api/getVideoInfo', { file_path: filePath });
        
        if (response.status === 'ok') {
            const info = response.data;
            
            const infoHtml = `
                <div class="video-info-content">
                    <h4>Video Information</h4>
                    <div class="info-grid">
                        <div class="info-item">
                            <strong>Duration:</strong> ${formatDuration(info.duration)}
                        </div>
                        <div class="info-item">
                            <strong>File Size:</strong> ${convertBytes(info.size)}
                        </div>
                        <div class="info-item">
                            <strong>Bitrate:</strong> ${Math.round(info.bitrate / 1000)} kbps
                        </div>
                        <div class="info-item">
                            <strong>Format:</strong> ${info.format_name}
                        </div>
                        <div class="info-item">
                            <strong>Resolution:</strong> ${info.video.width}x${info.video.height}
                        </div>
                        <div class="info-item">
                            <strong>Video Codec:</strong> ${info.video.codec}
                        </div>
                        <div class="info-item">
                            <strong>Frame Rate:</strong> ${info.video.fps.toFixed(2)} fps
                        </div>
                        <div class="info-item">
                            <strong>Audio Codec:</strong> ${info.audio.codec}
                        </div>
                        <div class="info-item">
                            <strong>Audio Channels:</strong> ${info.audio.channels}
                        </div>
                        <div class="info-item">
                            <strong>Sample Rate:</strong> ${info.audio.sample_rate} Hz
                        </div>
                    </div>
                </div>
            `;
            
            infoDisplay.innerHTML = infoHtml;
            infoDisplay.style.display = 'block';
        } else {
            infoDisplay.innerHTML = `<div class="error-message">Failed to get video info: ${response.message}</div>`;
            infoDisplay.style.display = 'block';
        }
    } catch (error) {
        infoDisplay.innerHTML = `<div class="error-message">Error: ${error.message}</div>`;
        infoDisplay.style.display = 'block';
    } finally {
        getInfoBtn.textContent = 'Get Info';
        getInfoBtn.disabled = false;
    }
}

// Start transcoding
async function startTranscode(filePath) {
    const format = document.getElementById('transcode-format').value;
    const quality = document.getElementById('transcode-quality').value;
    const speed = document.getElementById('transcode-speed').value;
    
    try {
        const response = await postJson('/api/startTranscode', {
            file_path: filePath,
            output_format: format,
            quality: quality,
            speed_preset: speed
        });
        
        if (response.status === 'ok') {
            currentTranscodeId = response.transcode_id;
            closeTranscodeModal();
            showTranscodeProgress();
        } else {
            alert('Failed to start transcoding: ' + response.message);
        }
    } catch (error) {
        alert('Error starting transcode: ' + error.message);
    }
}

// Show transcoding progress
function showTranscodeProgress() {
    // Create progress modal
    const progressHtml = `
        <div id="transcode-progress-modal" class="file-uploader">
            <span class="upload-head">🎬 Transcoding Video...</span>
            <span id="transcode-filename" class="upload-info">Processing video file...</span>
            <span id="transcode-status" class="upload-info">Status: Starting...</span>
            <span id="transcode-progress-text" class="upload-info">Progress: 0%</span>
            <span id="transcode-speed" class="upload-info">Speed: --</span>
            <span id="transcode-eta" class="upload-info">ETA: --</span>
            <div class="progress">
                <div class="progress-bar" id="transcode-progress-bar"></div>
            </div>
            <div class="btn-div">
                <button id="cancel-transcode">Cancel Transcode</button>
            </div>
        </div>
    `;

    // Remove existing progress modal if any
    const existingModal = document.getElementById('transcode-progress-modal');
    if (existingModal) {
        existingModal.remove();
    }

    // Add modal to page
    document.body.insertAdjacentHTML('beforeend', progressHtml);

    // Show modal
    document.getElementById('bg-blur').style.zIndex = '2';
    document.getElementById('bg-blur').style.opacity = '0.1';
    document.getElementById('transcode-progress-modal').style.zIndex = '3';
    document.getElementById('transcode-progress-modal').style.opacity = '1';

    // Add cancel event listener
    document.getElementById('cancel-transcode').addEventListener('click', cancelTranscode);

    // Start progress monitoring
    startTranscodeProgressMonitoring();
}

// Start monitoring transcode progress
function startTranscodeProgressMonitoring() {
    if (transcodeProgressInterval) {
        clearInterval(transcodeProgressInterval);
    }

    transcodeProgressInterval = setInterval(async () => {
        if (!currentTranscodeId) {
            clearInterval(transcodeProgressInterval);
            return;
        }

        try {
            const response = await postJson('/api/getTranscodeProgress', {
                transcode_id: currentTranscodeId
            });

            if (response.status === 'ok') {
                updateTranscodeProgress(response.data);
            } else if (response.status === 'not found') {
                // Transcode might be completed or cancelled
                clearInterval(transcodeProgressInterval);
                closeTranscodeProgress();
            }
        } catch (error) {
            console.error('Error getting transcode progress:', error);
        }
    }, 2000); // Check every 2 seconds
}

// Update transcode progress display
function updateTranscodeProgress(progress) {
    const statusElement = document.getElementById('transcode-status');
    const progressElement = document.getElementById('transcode-progress-text');
    const progressBar = document.getElementById('transcode-progress-bar');
    const speedElement = document.getElementById('transcode-speed');
    const etaElement = document.getElementById('transcode-eta');

    if (statusElement) {
        statusElement.textContent = `Status: ${progress.status}`;
    }

    if (progressElement && progressBar) {
        const progressPercent = Math.round(progress.progress || 0);
        progressElement.textContent = `Progress: ${progressPercent}%`;
        progressBar.style.width = `${progressPercent}%`;
    }

    if (speedElement && progress.speed) {
        const speed = progress.speed > 1 ? `${progress.speed.toFixed(1)}x` : `${(progress.speed * 100).toFixed(0)}%`;
        speedElement.textContent = `Speed: ${speed}`;
    }

    if (etaElement && progress.eta) {
        const eta = formatDuration(progress.eta);
        etaElement.textContent = `ETA: ${eta}`;
    }

    // Handle completion
    if (progress.status === 'completed') {
        clearInterval(transcodeProgressInterval);
        setTimeout(() => {
            closeTranscodeProgress();
            alert('Transcoding completed successfully!');
            window.location.reload();
        }, 2000);
    } else if (progress.status === 'error') {
        clearInterval(transcodeProgressInterval);
        setTimeout(() => {
            closeTranscodeProgress();
            alert('Transcoding failed: ' + (progress.error || 'Unknown error'));
        }, 1000);
    } else if (progress.status === 'cancelled') {
        clearInterval(transcodeProgressInterval);
        setTimeout(() => {
            closeTranscodeProgress();
            alert('Transcoding was cancelled.');
        }, 1000);
    }
}

// Cancel transcoding
async function cancelTranscode() {
    if (!currentTranscodeId) return;

    try {
        const response = await postJson('/api/cancelTranscode', {
            transcode_id: currentTranscodeId
        });

        if (response.status === 'ok') {
            clearInterval(transcodeProgressInterval);
            closeTranscodeProgress();
            alert('Transcoding cancelled.');
        } else {
            alert('Failed to cancel transcoding: ' + response.message);
        }
    } catch (error) {
        alert('Error cancelling transcode: ' + error.message);
    }
}

// Close transcode progress modal
function closeTranscodeProgress() {
    if (transcodeProgressInterval) {
        clearInterval(transcodeProgressInterval);
        transcodeProgressInterval = null;
    }

    currentTranscodeId = null;

    document.getElementById('bg-blur').style.opacity = '0';
    setTimeout(() => {
        document.getElementById('bg-blur').style.zIndex = '-1';
    }, 300);

    const modal = document.getElementById('transcode-progress-modal');
    if (modal) {
        modal.style.opacity = '0';
        setTimeout(() => {
            modal.remove();
        }, 300);
    }
}

// Helper function to format duration
function formatDuration(seconds) {
    if (!seconds || seconds === 0) return '0:00';
    
    const hours = Math.floor(seconds / 3600);
    const minutes = Math.floor((seconds % 3600) / 60);
    const secs = Math.floor(seconds % 60);
    
    if (hours > 0) {
        return `${hours}:${minutes.toString().padStart(2, '0')}:${secs.toString().padStart(2, '0')}`;
    } else {
        return `${minutes}:${secs.toString().padStart(2, '0')}`;
    }
}

// Initialize transcoding when page loads
document.addEventListener('DOMContentLoaded', function() {
    initializeTranscoding();
});

// Export functions for use in other scripts
window.showTranscodeModal = showTranscodeModal;