function openFolder() {
    let path = (getCurrentPath() + '/' + this.getAttribute('data-id') + '/').replaceAll('//', '/')

    const auth = getFolderAuthFromPath()
    if (auth) {
        path = path + '&auth=' + auth
    }
    window.location.href = `/?path=${path}`
}

function openFile() {
    const fileName = this.getAttribute('data-name').toLowerCase()
    let path = '/file?path=' + this.getAttribute('data-path') + '/' + this.getAttribute('data-id')

    // Check if it's a PDF file
    if (fileName.endsWith('.pdf')) {
        // Open PDF in the built-in viewer
        const viewerPath = '/pdf-viewer?path=' + this.getAttribute('data-path') + '/' + this.getAttribute('data-id')
        window.open(viewerPath, '_blank')
        return
    }

    // Check if it's a video file
    if (fileName.endsWith('.mp4') || fileName.endsWith('.mkv') || fileName.endsWith('.webm') || fileName.endsWith('.mov') || fileName.endsWith('.avi') || fileName.endsWith('.ts') || fileName.endsWith('.ogv')) {
        path = '/stream?url=' + getRootUrl() + path
    }

    window.open(path, '_blank')
}


// File More Button Handler Start

function openMoreButton(div) {
    const id = div.getAttribute('data-id')
    const moreDiv = document.getElementById(`more-option-${id}`)

    const rect = div.getBoundingClientRect();
    const x = rect.left + window.scrollX - 40;
    const y = rect.top + window.scrollY;

    moreDiv.style.zIndex = 2
    moreDiv.style.opacity = 1
    moreDiv.style.left = `${x}px`
    moreDiv.style.top = `${y}px`

    const isTrash = getCurrentPath().includes('/trash')

    moreDiv.querySelector('.more-options-focus').focus()
    moreDiv.querySelector('.more-options-focus').addEventListener('blur', closeMoreBtnFocus);
    moreDiv.querySelector('.more-options-focus').addEventListener('focusout', closeMoreBtnFocus);
    if (!isTrash) {
        moreDiv.querySelector(`#rename-${id}`).addEventListener('click', renameFileFolder)
        moreDiv.querySelector(`#trash-${id}`).addEventListener('click', trashFileFolder)
        moreDiv.querySelector(`#move-${id}`).addEventListener('click', moveFileFolder)
        moreDiv.querySelector(`#copy-${id}`).addEventListener('click', copyFileFolder)
        try {
            moreDiv.querySelector(`#share-${id}`).addEventListener('click', shareFile)
        }
        catch { }
        try {
            moreDiv.querySelector(`#folder-share-${id}`).addEventListener('click', shareFolder)
        }
        catch { }
        try {
            moreDiv.querySelector(`#transcode-${id}`).addEventListener('click', transcodeVideo)
        }
        catch { }
    }
    else {
        moreDiv.querySelector(`#restore-${id}`).addEventListener('click', restoreFileFolder)
        moreDiv.querySelector(`#delete-${id}`).addEventListener('click', deleteFileFolder)
    }
}

function closeMoreBtnFocus() {
    const moreDiv = this.parentElement
    moreDiv.style.opacity = '0'
    setTimeout(() => {
        moreDiv.style.zIndex = '-1'
    }, 300)
}

// Rename File Folder Start
function renameFileFolder() {
    const id = this.getAttribute('id').split('-')[1]
    console.log(id)

    document.getElementById('rename-name').value = this.parentElement.getAttribute('data-name');
    document.getElementById('bg-blur').style.zIndex = '2';
    document.getElementById('bg-blur').style.opacity = '0.1';

    document.getElementById('rename-file-folder').style.zIndex = '3';
    document.getElementById('rename-file-folder').style.opacity = '1';
    document.getElementById('rename-file-folder').setAttribute('data-id', id);
    setTimeout(() => {
        document.getElementById('rename-name').focus();
    }, 300)
}

document.getElementById('rename-cancel').addEventListener('click', () => {
    document.getElementById('rename-name').value = '';
    document.getElementById('bg-blur').style.opacity = '0';
    setTimeout(() => {
        document.getElementById('bg-blur').style.zIndex = '-1';
    }, 300)
    document.getElementById('rename-file-folder').style.opacity = '0';
    setTimeout(() => {
        document.getElementById('rename-file-folder').style.zIndex = '-1';
    }, 300)
});

document.getElementById('rename-create').addEventListener('click', async () => {
    const name = document.getElementById('rename-name').value;
    if (name === '') {
        alert('Name cannot be empty')
        return
    }

    const id = document.getElementById('rename-file-folder').getAttribute('data-id')

    const path = document.getElementById(`more-option-${id}`).getAttribute('data-path') + '/' + id

    const data = {
        'name': name,
        'path': path
    }

    const response = await postJson('/api/renameFileFolder', data)
    if (response.status === 'ok') {
        alert('File/Folder Renamed Successfully')
        window.location.reload();
    } else {
        alert('Failed to rename file/folder')
        window.location.reload();
    }
});

// Rename File Folder End

// Move File Folder Start
function moveFileFolder() {
    const id = this.getAttribute('id').split('-')[1]
    const itemPath = document.getElementById(`more-option-${id}`).getAttribute('data-path') + '/' + id
    const itemName = document.getElementById(`more-option-${id}`).getAttribute('data-name')
    
    // Close the more options menu first
    closeMoreBtnFocus.call(this.parentElement.querySelector('.more-options-focus'))
    
    // Show move modal
    showMoveModal(itemPath, itemName)
}

// Copy File Folder Start
function copyFileFolder() {
    const id = this.getAttribute('id').split('-')[1]
    const itemPath = document.getElementById(`more-option-${id}`).getAttribute('data-path') + '/' + id
    const itemName = document.getElementById(`more-option-${id}`).getAttribute('data-name')
    
    // Close the more options menu first
    closeMoreBtnFocus.call(this.parentElement.querySelector('.more-options-focus'))
    
    // Show copy modal
    showCopyModal(itemPath, itemName)
}

// Transcode Video Start
function transcodeVideo() {
    const id = this.getAttribute('id').split('-')[1]
    const itemPath = document.getElementById(`more-option-${id}`).getAttribute('data-path') + '/' + id
    const itemName = document.getElementById(`more-option-${id}`).getAttribute('data-name')
    
    // Close the more options menu first
    closeMoreBtnFocus.call(this.parentElement.querySelector('.more-options-focus'))
    
    // Show transcode modal
    if (window.showTranscodeModal) {
        window.showTranscodeModal(itemPath, itemName)
    } else {
        alert('Transcoding feature not available')
    }
}

async function trashFileFolder() {
    const id = this.getAttribute('id').split('-')[1]
    console.log(id)
    const path = document.getElementById(`more-option-${id}`).getAttribute('data-path') + '/' + id
    const data = {
        'path': path,
        'trash': true
    }
    const response = await postJson('/api/trashFileFolder', data)

    if (response.status === 'ok') {
        alert('File/Folder Sent to Trash Successfully')
        window.location.reload();
    } else {
        alert('Failed to Send File/Folder to Trash')
        window.location.reload();
    }
}

async function restoreFileFolder() {
    const id = this.getAttribute('id').split('-')[1]
    const path = this.getAttribute('data-path') + '/' + id
    const data = {
        'path': path,
        'trash': false
    }
    const response = await postJson('/api/trashFileFolder', data)

    if (response.status === 'ok') {
        alert('File/Folder Restored Successfully')
        window.location.reload();
    } else {
        alert('Failed to Restored File/Folder')
        window.location.reload();
    }
}

async function deleteFileFolder() {
    const id = this.getAttribute('id').split('-')[1]
    const path = this.getAttribute('data-path') + '/' + id
    const data = {
        'path': path
    }
    const response = await postJson('/api/deleteFileFolder', data)

    if (response.status === 'ok') {
        alert('File/Folder Deleted Successfully')
        window.location.reload();
    } else {
        alert('Failed to Delete File/Folder')
        window.location.reload();
    }
}

async function shareFile() {
    const fileName = this.parentElement.getAttribute('data-name').toLowerCase()
    const id = this.getAttribute('id').split('-')[1]
    const path = document.getElementById(`more-option-${id}`).getAttribute('data-path') + '/' + id
    const root_url = getRootUrl()

    let link
    if (fileName.endsWith('.pdf')) {
        // Share PDF viewer link instead of direct download
        link = `${root_url}/pdf-viewer?path=${path}`
    } else if (fileName.endsWith('.mp4') || fileName.endsWith('.mkv') || fileName.endsWith('.webm') || fileName.endsWith('.mov') || fileName.endsWith('.avi') || fileName.endsWith('.ts') || fileName.endsWith('.ogv')) {
        link = `${root_url}/stream?url=${root_url}/file?path=${path}`
    } else {
        link = `${root_url}/file?path=${path}`
    }

    copyTextToClipboard(link)
}


async function shareFolder() {
    const id = this.getAttribute('id').split('-')[2]
    console.log(id)
    let path = document.getElementById(`more-option-${id}`).getAttribute('data-path') + '/' + id
    const root_url = getRootUrl()

    const auth = await getFolderShareAuth(path)
    path = path.slice(1)

    let link = `${root_url}/?path=/share_${path}&auth=${auth}`
    console.log(link)

    copyTextToClipboard(link)
}

// File More Button Handler  End