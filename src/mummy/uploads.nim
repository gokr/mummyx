import std/[os, strutils, times, options, tables, nativesockets]
import std/[sequtils, hashes, random, strformat, sha1]
import common
import webby/[queryparams, httpheaders]

export os, times, options

type
  UploadError* = object of CatchableError

  UploadStatus* = enum
    ## Status of an upload operation
    UploadPending,     ## Upload created but not started
    UploadInProgress,  ## Upload is actively receiving data
    UploadCompleted,   ## Upload finished successfully
    UploadFailed,      ## Upload failed due to error
    UploadCancelled    ## Upload was cancelled

  UploadProgressCallback* = proc(bytesReceived: int64, totalBytes: int64) {.gcsafe.}
  UploadCompleteCallback* = proc(finalPath: string) {.gcsafe.}
  UploadErrorCallback* = proc(error: string) {.gcsafe.}

  UploadConfig* = object
    ## Configuration for upload operations
    uploadDir*: string              ## Directory for storing uploads
    tempDir*: string                ## Directory for temporary files during upload
    maxFileSize*: int64             ## Maximum file size in bytes (0 = unlimited)
    maxConcurrentUploads*: int      ## Maximum concurrent uploads (0 = unlimited)
    uploadTimeout*: float           ## Upload timeout in seconds (0 = unlimited)
    bufferSize*: int                ## Buffer size for streaming operations
    enableResumableUploads*: bool   ## Enable TUS resumable upload protocol
    enableRangeRequests*: bool      ## Enable HTTP Range request support
    enableIntegrityCheck*: bool     ## Enable checksum verification
    cleanupInterval*: float         ## Interval for cleaning up expired uploads
    autoCreateDirs*: bool           ## Automatically create upload directories
    maxUploadRate*: int64           ## Maximum upload rate in bytes/sec (0 = unlimited)

  UploadSession* = object
    ## Represents an active or pending upload session
    id*: string                     ## Unique upload identifier
    filename*: string               ## Original filename
    tempPath*: string               ## Path to temporary file during upload
    finalPath*: string              ## Final path after upload completion
    totalSize*: int64               ## Expected total size in bytes (-1 if unknown)
    bytesReceived*: int64           ## Bytes received so far
    status*: UploadStatus           ## Current upload status
    createdAt*: DateTime            ## When upload was created
    lastAccessedAt*: DateTime       ## Last time upload was accessed
    contentType*: string            ## MIME content type
    metadata*: Table[string, string] ## Additional metadata
    file*: File                     ## File handle for streaming operations
    expectedChecksum*: string       ## Expected checksum (SHA1 hex)
    actualChecksum*: string         ## Calculated checksum during upload
    checksumContext*: Sha1State     ## Running checksum calculation
    uploadRate*: int64              ## Current upload rate in bytes/sec
    lastChunkTime*: DateTime        ## Time of last chunk received
    supportsRanges*: bool           ## Whether this upload supports range requests
    onProgress*: UploadProgressCallback
    onComplete*: UploadCompleteCallback
    onError*: UploadErrorCallback

  StreamingRequest* = object
    ## Request object for streaming uploads that doesn't buffer body in memory
    httpVersion*: HttpVersion
    httpMethod*: string
    uri*: string
    path*: string
    queryParams*: QueryParams
    pathParams*: PathParams
    headers*: HttpHeaders
    remoteAddress*: string
    serverPtr*: pointer  ## Pointer to server (avoid circular import)
    clientSocket*: SocketHandle
    clientId*: uint64
    uploadConfig*: UploadConfig

  UploadManager* = object
    ## Manages upload sessions and configuration
    config*: UploadConfig
    sessions*: Table[string, UploadSession]
    sessionsByClient*: Table[uint64, seq[string]] ## Track uploads per client
    lastCleanup*: DateTime

proc defaultUploadConfig*(): UploadConfig =
  ## Returns default upload configuration
  result = UploadConfig(
    uploadDir: "uploads",
    tempDir: "uploads/tmp",
    maxFileSize: 100 * 1024 * 1024, # 100 MB default
    maxConcurrentUploads: 10,
    uploadTimeout: 300.0, # 5 minutes
    bufferSize: 64 * 1024, # 64 KB
    enableResumableUploads: true,
    enableRangeRequests: true,
    enableIntegrityCheck: true,
    cleanupInterval: 3600.0, # 1 hour
    autoCreateDirs: true,
    maxUploadRate: 0 # Unlimited by default
  )

proc generateUploadId*(): string =
  ## Generate a unique upload identifier
  randomize()
  let chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
  result = ""
  for i in 0..<32:
    result.add(chars[rand(chars.len - 1)])

proc createUploadDirectories*(config: UploadConfig) =
  ## Create necessary upload directories
  if config.autoCreateDirs:
    createDir(config.uploadDir)
    createDir(config.tempDir)

proc validateUploadConfig*(config: UploadConfig) =
  ## Validate upload configuration
  if config.uploadDir.len == 0:
    raise newException(UploadError, "Upload directory cannot be empty")
  if config.tempDir.len == 0:
    raise newException(UploadError, "Temp directory cannot be empty")
  if config.bufferSize <= 0:
    raise newException(UploadError, "Buffer size must be positive")

proc getTempFilePath*(config: UploadConfig, uploadId: string): string =
  ## Get temporary file path for an upload
  config.tempDir / (uploadId & ".tmp")

proc getFinalFilePath*(config: UploadConfig, filename: string): string =
  ## Get final file path for an upload
  config.uploadDir / filename

proc sanitizeFilename*(filename: string): string =
  ## Sanitize filename to prevent directory traversal
  result = filename
  # Remove path separators and dangerous characters
  result = result.replace("/", "_").replace("\\", "_")
  result = result.replace("..", "_").replace(":", "_")
  # Ensure filename is not empty and doesn't start with dot
  if result.len == 0 or result[0] == '.':
    result = "upload_" & result

proc newUploadSession*(
  uploadId: string,
  filename: string,
  config: UploadConfig,
  totalSize: int64 = -1,
  contentType: string = "application/octet-stream"
): UploadSession =
  ## Create a new upload session
  let sanitizedFilename = sanitizeFilename(filename)
  let now = now()
  
  result = UploadSession(
    id: uploadId,
    filename: sanitizedFilename,
    tempPath: getTempFilePath(config, uploadId),
    finalPath: getFinalFilePath(config, sanitizedFilename),
    totalSize: totalSize,
    bytesReceived: 0,
    status: UploadPending,
    createdAt: now,
    lastAccessedAt: now,
    contentType: contentType,
    metadata: initTable[string, string]()
  )

proc openForWriting*(session: var UploadSession) =
  ## Open temporary file for writing
  try:
    session.file = open(session.tempPath, fmWrite)
    session.status = UploadInProgress
  except IOError as e:
    session.status = UploadFailed
    if session.onError != nil:
      session.onError("Failed to open file for writing: " & e.msg)
    raise newException(UploadError, "Failed to open file for writing: " & e.msg)

proc writeChunk*(session: var UploadSession, data: openArray[byte]) =
  ## Write data chunk to upload file
  try:
    if session.status != UploadInProgress:
      raise newException(UploadError, "Upload not in progress")
    
    # Rate limiting check
    let currentTime = now()
    if session.uploadRate > 0:
      let timeSinceLastChunk = (currentTime - session.lastChunkTime).inMilliseconds.float / 1000.0
      if timeSinceLastChunk > 0:
        let currentRate = data.len.float / timeSinceLastChunk
        if currentRate > session.uploadRate.float:
          # Could implement rate limiting here
          discard
    
    discard session.file.writeBuffer(data[0].unsafeAddr, data.len)
    session.bytesReceived += data.len
    session.lastAccessedAt = currentTime
    session.lastChunkTime = currentTime
    
    # Update checksum if enabled
    if session.expectedChecksum.len > 0:
      session.checksumContext.update(cast[ptr UncheckedArray[char]](data[0].unsafeAddr).toOpenArray(0, data.len - 1))
    
    # Check size limits
    if session.totalSize > 0 and session.bytesReceived > session.totalSize:
      session.status = UploadFailed
      if session.onError != nil:
        session.onError("Upload exceeded expected size")
      raise newException(UploadError, "Upload exceeded expected size")
    
    # Call progress callback
    if session.onProgress != nil:
      session.onProgress(session.bytesReceived, session.totalSize)
      
  except IOError as e:
    session.status = UploadFailed
    if session.onError != nil:
      session.onError("Failed to write chunk: " & e.msg)
    raise newException(UploadError, "Failed to write chunk: " & e.msg)

proc completeUpload*(session: var UploadSession) =
  ## Complete the upload by atomically moving temp file to final location
  try:
    # Close the file and flush all buffers
    if session.file != nil:
      session.file.flushFile()
      session.file.close()
      session.file = nil
    
    # Verify file size matches expected if known
    if session.totalSize > 0:
      let actualSize = getFileSize(session.tempPath)
      if actualSize != session.totalSize:
        session.status = UploadFailed
        let errMsg = fmt"File size mismatch: expected {session.totalSize}, got {actualSize}"
        if session.onError != nil:
          session.onError(errMsg)
        raise newException(UploadError, errMsg)
    
    # Verify checksum if expected
    if session.expectedChecksum.len > 0:
      session.actualChecksum = $SecureHash(session.checksumContext.finalize)
      if session.actualChecksum.toLowerAscii() != session.expectedChecksum.toLowerAscii():
        session.status = UploadFailed
        let errMsg = fmt"Checksum mismatch: expected {session.expectedChecksum}, got {session.actualChecksum}"
        if session.onError != nil:
          session.onError(errMsg)
        raise newException(UploadError, errMsg)
    
    # Ensure final directory exists
    let finalDir = parentDir(session.finalPath)
    if not dirExists(finalDir):
      createDir(finalDir)
    
    # Handle file conflicts - append number if file exists
    var finalPath = session.finalPath
    var counter = 1
    while fileExists(finalPath):
      let (dir, name, ext) = splitFile(session.finalPath)
      finalPath = dir / fmt"{name}_{counter}{ext}"
      inc counter
    
    session.finalPath = finalPath
    
    # Atomic move operation
    moveFile(session.tempPath, session.finalPath)
    session.status = UploadCompleted
    
    # Call completion callback
    if session.onComplete != nil:
      session.onComplete(session.finalPath)
      
  except OSError as e:
    session.status = UploadFailed
    # Clean up temp file on failure
    if fileExists(session.tempPath):
      try:
        removeFile(session.tempPath)
      except OSError:
        discard # Ignore cleanup errors
    if session.onError != nil:
      session.onError("Failed to complete upload: " & e.msg)
    raise newException(UploadError, "Failed to complete upload: " & e.msg)

proc cancelUpload*(session: var UploadSession) =
  ## Cancel the upload and clean up temporary files
  try:
    # Close file if open
    if session.file != nil:
      session.file.close()
    
    # Remove temporary file
    if fileExists(session.tempPath):
      removeFile(session.tempPath)
    
    session.status = UploadCancelled
    
  except OSError:
    # Ignore errors during cleanup
    discard

proc getUploadProgress*(session: UploadSession): float =
  ## Get upload progress as percentage (0.0 to 1.0)
  if session.totalSize <= 0:
    return 0.0
  result = session.bytesReceived.float / session.totalSize.float
  if result > 1.0:
    result = 1.0

proc isExpired*(session: UploadSession, timeout: float): bool =
  ## Check if upload session has expired
  if timeout <= 0:
    return false
  let elapsed = (now() - session.lastAccessedAt).inSeconds.float
  result = elapsed > timeout

proc newUploadManager*(config: UploadConfig): UploadManager =
  ## Create a new upload manager
  validateUploadConfig(config)
  createUploadDirectories(config)
  
  result = UploadManager(
    config: config,
    sessions: initTable[string, UploadSession](),
    sessionsByClient: initTable[uint64, seq[string]](),
    lastCleanup: now()
  )

proc createUpload*(
  manager: var UploadManager,
  filename: string,
  clientId: uint64,
  totalSize: int64 = -1,
  contentType: string = "application/octet-stream"
): string =
  ## Create a new upload session and return upload ID
  
  # Check concurrent upload limit
  if manager.config.maxConcurrentUploads > 0:
    let activeUploads = manager.sessionsByClient.getOrDefault(clientId, @[])
    if activeUploads.len >= manager.config.maxConcurrentUploads:
      raise newException(UploadError, "Maximum concurrent uploads exceeded")
  
  # Check file size limit
  if manager.config.maxFileSize > 0 and totalSize > manager.config.maxFileSize:
    raise newException(UploadError, "File size exceeds maximum allowed")
  
  let uploadId = generateUploadId()
  let session = newUploadSession(uploadId, filename, manager.config, totalSize, contentType)
  
  manager.sessions[uploadId] = session
  
  # Track by client
  if clientId notin manager.sessionsByClient:
    manager.sessionsByClient[clientId] = @[]
  manager.sessionsByClient[clientId].add(uploadId)
  
  result = uploadId

proc getUpload*(manager: var UploadManager, uploadId: string): ptr UploadSession =
  ## Get upload session by ID
  if uploadId in manager.sessions:
    result = manager.sessions[uploadId].addr
    result.lastAccessedAt = now()
  else:
    result = nil

proc removeUpload*(manager: var UploadManager, uploadId: string) =
  ## Remove upload session
  if uploadId in manager.sessions:
    let session = manager.sessions[uploadId]
    
    # Clean up any temporary files
    var mutableSession = session
    mutableSession.cancelUpload()
    
    # Remove from tracking
    manager.sessions.del(uploadId)
    
    # Remove from client tracking
    for clientId, uploads in manager.sessionsByClient.mpairs:
      uploads = uploads.filterIt(it != uploadId)
      if uploads.len == 0:
        manager.sessionsByClient.del(clientId)

proc cleanupExpiredUploads*(manager: var UploadManager) =
  ## Clean up expired upload sessions
  let currentTime = now()
  if (currentTime - manager.lastCleanup).inSeconds.float < manager.config.cleanupInterval:
    return
  
  var toRemove: seq[string]
  for uploadId, session in manager.sessions:
    if session.isExpired(manager.config.uploadTimeout):
      toRemove.add(uploadId)
  
  for uploadId in toRemove:
    manager.removeUpload(uploadId)
  
  manager.lastCleanup = currentTime

proc getAvailableDiskSpace*(path: string): int64 =
  ## Get available disk space in bytes for the given path
  when defined(windows):
    # Windows implementation would go here
    result = -1 # Unknown
  else:
    # Unix/Linux implementation using statvfs
    try:
      discard getFileInfo(path)
      # For now, return -1 to indicate unknown disk space
      # This could be enhanced with proper statvfs calls
      result = -1
    except OSError:
      result = -1

proc validateDiskSpace*(config: UploadConfig, requiredBytes: int64): bool =
  ## Check if enough disk space is available for upload
  if requiredBytes <= 0:
    return true
  
  let available = getAvailableDiskSpace(config.tempDir)
  if available < 0:
    return true # Unknown disk space, allow upload
  
  # Require at least 10% more space than the upload size as safety margin
  let requiredWithMargin = (requiredBytes.float * 1.1).int64
  result = available >= requiredWithMargin

proc resumeUpload*(
  manager: var UploadManager,
  uploadId: string,
  clientId: uint64
): ptr UploadSession =
  ## Resume an existing upload session
  let session = manager.getUpload(uploadId)
  if session == nil:
    return nil
  
  # Verify the upload belongs to this client
  let clientUploads = manager.sessionsByClient.getOrDefault(clientId, @[])
  if uploadId notin clientUploads:
    return nil
  
  # Check if upload can be resumed
  if session.status notin {UploadPending, UploadInProgress}:
    return nil
  
  # Check if temp file exists and get current size
  if fileExists(session.tempPath):
    session.bytesReceived = getFileSize(session.tempPath)
    session.status = UploadInProgress
  else:
    # Temp file missing, reset upload
    session.bytesReceived = 0
    session.status = UploadPending
  
  session.lastAccessedAt = now()
  result = session

proc getUploadStats*(manager: UploadManager): tuple[total: int, active: int, completed: int, failed: int] =
  ## Get statistics about uploads
  result = (total: 0, active: 0, completed: 0, failed: 0)
  for session in manager.sessions.values:
    inc result.total
    case session.status:
    of UploadInProgress:
      inc result.active
    of UploadCompleted:
      inc result.completed
    of UploadFailed:
      inc result.failed
    else:
      discard

proc validateUploadHeaders*(headers: HttpHeaders): tuple[valid: bool, contentType: string, contentLength: int64] =
  ## Validate HTTP headers for upload and extract relevant information
  result = (valid: true, contentType: "application/octet-stream", contentLength: -1)
  
  # Extract Content-Type
  for (key, value) in headers:
    if key.toLowerAscii() == "content-type":
      result.contentType = value
    elif key.toLowerAscii() == "content-length":
      try:
        result.contentLength = parseBiggestInt(value)
        if result.contentLength < 0:
          result.valid = false
          return
      except ValueError:
        result.valid = false
        return
  
  # Additional validation could be added here
  # For example, checking for required TUS headers for resumable uploads

proc writeRangeChunk*(
  session: var UploadSession,
  data: openArray[byte],
  rangeStart: int64,
  rangeEnd: int64
) =
  ## Write data chunk at specific range position for partial uploads
  try:
    if session.status != UploadInProgress:
      raise newException(UploadError, "Upload not in progress")
    
    if not session.supportsRanges:
      raise newException(UploadError, "Upload session does not support range requests")
    
    # Validate range
    if rangeStart < 0 or rangeEnd < rangeStart:
      raise newException(UploadError, "Invalid range specification")
    
    if session.totalSize > 0 and rangeEnd >= session.totalSize:
      raise newException(UploadError, "Range end exceeds expected file size")
    
    # Seek to the correct position
    if session.file != nil:
      session.file.setFilePos(rangeStart)
      discard session.file.writeBuffer(data[0].unsafeAddr, data.len)
      session.file.flushFile()
    
    # Update bytes received (for ranges, this is more complex)
    let chunkSize = rangeEnd - rangeStart + 1
    if chunkSize != data.len:
      raise newException(UploadError, "Chunk size doesn't match range")
    
    # Update checksum if enabled (order-dependent, so ranges need special handling)
    if session.expectedChecksum.len > 0:
      # For range uploads, we need to handle checksums carefully
      # This is a simplified approach - production might need more sophisticated handling
      session.checksumContext.update(cast[ptr UncheckedArray[char]](data[0].unsafeAddr).toOpenArray(0, data.len - 1))
    
    session.lastAccessedAt = now()
    
    # Update progress (approximate for range uploads)
    session.bytesReceived = max(session.bytesReceived, rangeEnd + 1)
    
    if session.onProgress != nil:
      session.onProgress(session.bytesReceived, session.totalSize)
      
  except IOError as e:
    session.status = UploadFailed
    if session.onError != nil:
      session.onError("Failed to write range chunk: " & e.msg)
    raise newException(UploadError, "Failed to write range chunk: " & e.msg)

proc setExpectedChecksum*(session: var UploadSession, checksum: string) =
  ## Set expected checksum for integrity verification
  session.expectedChecksum = checksum
  if checksum.len > 0:
    session.checksumContext = newSha1State()

proc getCurrentChecksum*(session: UploadSession): string =
  ## Get current calculated checksum
  result = session.actualChecksum

proc supportsRange*(session: UploadSession): bool =
  ## Check if upload session supports range requests
  result = session.supportsRanges

proc setRangeSupport*(session: var UploadSession, enabled: bool) =
  ## Enable or disable range support for upload session
  session.supportsRanges = enabled