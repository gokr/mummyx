## Integration tests for upload functionality
## Tests real upload scenarios against a running Mummy server

import std/[unittest, httpclient, json, strutils, os, times, base64, sha1, tables]
import std/[asyncdispatch, threadpool, locks]
import ../src/mummy, ../src/mummy/routers, ../src/mummy/tus

# Global variables for server control
var
  testServer: Server
  serverThread: Thread[void]
  serverRunning: bool
  serverLock: Lock

const
  TEST_PORT = 8899
  TEST_BASE_URL = "http://localhost:" & $TEST_PORT

proc serverProc() {.gcsafe.} =
  ## Run the test server in a background thread
  
  # TUS handler
  proc tusHandler(request: Request) =
    let uploadId = extractUploadIdFromPath(request.path, "/tus/")
    let tusResponse = request.handleTUSRequest(uploadId)
    request.respondTUS(tusResponse)
  
  # Range upload handlers
  proc createRangeUpload(request: Request) =
    var headers: mummy.HttpHeaders
    headers["Content-Type"] = "application/json"
    
    try:
      let requestBody = parseJson(request.body)
      let filename = requestBody["filename"].getStr()
      let size = requestBody["size"].getBiggestInt()
      
      let uploadId = request.createUpload(filename, size)
      let upload = request.getUpload(uploadId)
      
      if upload != nil:
        upload[].setRangeSupport(true)
        
        let response = %*{
          "success": true,
          "uploadId": uploadId
        }
        request.respond(200, headers, $response)
      else:
        let response = %*{
          "success": false,
          "error": "Failed to create upload"
        }
        request.respond(500, headers, $response)
        
    except Exception as e:
      let response = %*{
        "success": false,
        "error": e.msg
      }
      request.respond(500, headers, $response)
  
  proc rangeUploadHandler(request: Request) =
    let uploadId = request.pathParams["uploadId"]
    
    # Get Content-Range header
    var contentRange = ""
    for (key, value) in request.headers:
      if key.toLowerAscii() == "content-range":
        contentRange = value
        break
    
    if contentRange.len > 0:
      request.handleRangeRequest(uploadId, contentRange)
    else:
      request.respond(400, emptyHttpHeaders(), "Content-Range header required")
  
  # Simple upload handler
  proc simpleUploadHandler(request: Request) =
    var headers: mummy.HttpHeaders
    headers["Content-Type"] = "application/json"
    
    try:
      # For simple uploads, save the body directly
      let filename = "test_upload.bin"
      let uploadId = request.createUpload(filename, request.body.len)
      let upload = request.getUpload(uploadId)
      
      if upload != nil:
        upload[].openForWriting()
        upload[].writeChunk(request.body.toOpenArrayByte(0, request.body.len - 1))
        upload[].completeUpload()
        
        let response = %*{
          "success": true,
          "uploadId": uploadId,
          "size": request.body.len,
          "path": upload[].finalPath
        }
        request.respond(200, headers, $response)
      else:
        let response = %*{
          "success": false,
          "error": "Failed to create upload"
        }
        request.respond(500, headers, $response)
        
    except Exception as e:
      let response = %*{
        "success": false,
        "error": e.msg
      }
      request.respond(500, headers, $response)
  
  # Health check endpoint
  proc healthHandler(request: Request) =
    var headers: mummy.HttpHeaders
    headers["Content-Type"] = "application/json"
    let response = %*{"status": "healthy", "timestamp": $now()}
    request.respond(200, headers, $response)
  
  # Setup router
  var router: Router
  router.get("/health", healthHandler)
  router.post("/simple", simpleUploadHandler)
  router.post("/range/create", createRangeUpload)
  router.patch("/range/upload/@uploadId", rangeUploadHandler)
  
  # TUS endpoints
  router.options("/tus/", tusHandler)
  router.options("/tus/@uploadId", tusHandler)
  router.post("/tus/", tusHandler)
  router.head("/tus/@uploadId", tusHandler)
  router.patch("/tus/@uploadId", tusHandler)
  router.delete("/tus/@uploadId", tusHandler)
  
  # Configure uploads
  var uploadConfig = defaultUploadConfig()
  uploadConfig.uploadDir = "test_integration_uploads"
  uploadConfig.tempDir = "test_integration_uploads/tmp"
  uploadConfig.maxFileSize = 10 * 1024 * 1024  # 10MB
  uploadConfig.enableResumableUploads = true
  uploadConfig.enableRangeRequests = true
  uploadConfig.enableIntegrityCheck = true
  
  # Configure TUS
  var tusConfig = defaultTUSConfig()
  tusConfig.maxSize = 10 * 1024 * 1024  # 10MB
  tusConfig.enableChecksum = true
  
  # Create server
  testServer = newServer(
    router,
    enableUploads = true,
    uploadConfig = uploadConfig,
    tusConfig = tusConfig,
    maxBodyLen = 10 * 1024 * 1024
  )
  
  withLock serverLock:
    serverRunning = true
  
  echo "Integration test server starting on port ", TEST_PORT
  {.cast(gcsafe).}:
    testServer.serve(Port(TEST_PORT))

proc startTestServer() =
  ## Start the test server in background
  initLock(serverLock)
  serverRunning = false
  
  # Create test directories
  createDir("test_integration_uploads")
  createDir("test_integration_uploads/tmp")
  
  # Start server thread
  createThread(serverThread, serverProc)
  
  # Wait for server to start
  var attempts = 0
  while attempts < 50:  # Wait up to 5 seconds
    sleep(100)
    var running = false
    withLock serverLock:
      running = serverRunning
    
    if running:
      # Test if server is actually responding
      try:
        let client = newHttpClient(timeout = 1000)
        let response = client.get(TEST_BASE_URL & "/health")
        client.close()
        if response.code == Http200:
          echo "Test server is ready"
          return
      except:
        discard
    
    inc attempts
  
  raise newException(Exception, "Failed to start test server")

proc stopTestServer() =
  ## Stop the test server
  if serverRunning:
    # Note: In a real test, we'd need a proper shutdown mechanism
    # For now, we'll just clean up the directories
    try:
      if dirExists("test_integration_uploads"):
        removeDir("test_integration_uploads")
    except:
      discard
    
    withLock serverLock:
      serverRunning = false
    
    echo "Test server cleanup completed"

suite "Upload Integration Tests":

  setup:
    startTestServer()

  teardown:
    stopTestServer()

  test "Server health check":
    let client = newHttpClient()
    defer: client.close()
    
    let response = client.get(TEST_BASE_URL & "/health")
    check response.code == Http200
    
    let jsonResponse = parseJson(response.body)
    check jsonResponse["status"].getStr() == "healthy"

  test "Simple upload integration":
    let client = newHttpClient()
    defer: client.close()
    
    let testData = "Hello, integration test world!"
    
    let response = client.post(TEST_BASE_URL & "/simple", body = testData)
    check response.code == Http200
    
    let jsonResponse = parseJson(response.body)
    check jsonResponse["success"].getBool() == true
    check jsonResponse["size"].getInt() == testData.len

  test "TUS upload creation":
    let client = newHttpClient()
    defer: client.close()
    
    let testData = "TUS integration test data"
    
    # Create upload
    client.headers = newHttpHeaders([
      ("Tus-Resumable", "1.0.0"),
      ("Upload-Length", $testData.len),
      ("Upload-Metadata", "filename " & base64.encode("test.txt"))
    ])
    
    let createResponse = client.post(TEST_BASE_URL & "/tus/")
    check createResponse.code == Http201
    check createResponse.headers.hasKey("Location")
    check createResponse.headers.hasKey("Tus-Resumable")
    
    let location = createResponse.headers["Location"]
    let uploadId = location.split("/")[^1]
    
    # Upload data
    client.headers = newHttpHeaders([
      ("Tus-Resumable", "1.0.0"),
      ("Upload-Offset", "0"),
      ("Content-Type", "application/offset+octet-stream")
    ])
    
    let uploadResponse = client.patch(TEST_BASE_URL & "/tus/" & uploadId, body = testData)
    check uploadResponse.code == Http204
    check uploadResponse.headers["Upload-Offset"] == $testData.len

  test "TUS upload status check":
    let client = newHttpClient()
    defer: client.close()
    
    let testData = "Status check test"
    
    # Create upload
    client.headers = newHttpHeaders([
      ("Tus-Resumable", "1.0.0"),
      ("Upload-Length", $testData.len)
    ])
    
    let createResponse = client.post(TEST_BASE_URL & "/tus/")
    check createResponse.code == Http201
    
    let location = createResponse.headers["Location"]
    let uploadId = location.split("/")[^1]
    
    # Check status
    client.headers = newHttpHeaders([("Tus-Resumable", "1.0.0")])
    let statusResponse = client.head(TEST_BASE_URL & "/tus/" & uploadId)
    check statusResponse.code == Http200
    check statusResponse.headers["Upload-Offset"] == "0"
    check statusResponse.headers["Upload-Length"] == $testData.len

  test "Range upload integration":
    let client = newHttpClient()
    defer: client.close()
    
    let testData = "Range upload integration test data"
    
    # Create range upload session
    let createBody = %*{
      "filename": "range_test.txt",
      "size": testData.len
    }
    
    client.headers = newHttpHeaders([("Content-Type", "application/json")])
    let createResponse = client.post(TEST_BASE_URL & "/range/create", body = $createBody)
    check createResponse.code == Http200
    
    let createJson = parseJson(createResponse.body)
    check createJson["success"].getBool() == true
    let uploadId = createJson["uploadId"].getStr()
    
    # Upload in chunks
    let chunkSize = 10
    var offset = 0
    
    while offset < testData.len:
      let endPos = min(offset + chunkSize - 1, testData.len - 1)
      let chunk = testData[offset .. endPos]
      
      client.headers = newHttpHeaders([
        ("Content-Range", fmt"bytes {offset}-{endPos}/{testData.len}"),
        ("Content-Type", "application/octet-stream")
      ])
      
      let chunkResponse = client.patch(TEST_BASE_URL & "/range/upload/" & uploadId, body = chunk)
      check chunkResponse.code == Http204
      
      offset = endPos + 1

  test "Large file upload simulation":
    let client = newHttpClient()
    defer: client.close()
    
    # Create a larger test file (1KB)
    var largeData = ""
    for i in 0..<1024:
      largeData.add(char(i mod 256))
    
    # Test with simple upload
    let response = client.post(TEST_BASE_URL & "/simple", body = largeData)
    check response.code == Http200
    
    let jsonResponse = parseJson(response.body)
    check jsonResponse["success"].getBool() == true
    check jsonResponse["size"].getInt() == largeData.len

  test "Concurrent uploads":
    # Test multiple uploads happening simultaneously
    proc uploadWorker(data: string): bool {.gcsafe.} =
      try:
        let client = newHttpClient()
        defer: client.close()
        
        let response = client.post(TEST_BASE_URL & "/simple", body = data)
        return response.code == Http200
      except:
        return false
    
    let testData1 = "Concurrent upload test 1"
    let testData2 = "Concurrent upload test 2"
    let testData3 = "Concurrent upload test 3"
    
    let future1 = spawn uploadWorker(testData1)
    let future2 = spawn uploadWorker(testData2)
    let future3 = spawn uploadWorker(testData3)
    
    check ^future1 == true
    check ^future2 == true
    check ^future3 == true

  test "Upload with checksum verification":
    let client = newHttpClient()
    defer: client.close()
    
    let testData = "Checksum verification test"
    let expectedChecksum = $secureHash(testData)
    
    # Create TUS upload with checksum
    client.headers = newHttpHeaders([
      ("Tus-Resumable", "1.0.0"),
      ("Upload-Length", $testData.len),
      ("Upload-Checksum", "sha1 " & base64.encode(expectedChecksum))
    ])
    
    let createResponse = client.post(TEST_BASE_URL & "/tus/")
    check createResponse.code == Http201
    
    let location = createResponse.headers["Location"]
    let uploadId = location.split("/")[^1]
    
    # Upload data
    client.headers = newHttpHeaders([
      ("Tus-Resumable", "1.0.0"),
      ("Upload-Offset", "0"),
      ("Content-Type", "application/offset+octet-stream")
    ])
    
    let uploadResponse = client.patch(TEST_BASE_URL & "/tus/" & uploadId, body = testData)
    check uploadResponse.code == Http204

  test "Upload error handling":
    let client = newHttpClient()
    defer: client.close()
    
    # Test invalid TUS request (missing Upload-Length)
    client.headers = newHttpHeaders([("Tus-Resumable", "1.0.0")])
    let invalidResponse = client.post(TEST_BASE_URL & "/tus/")
    check invalidResponse.code == Http400
    
    # Test upload to non-existent session
    client.headers = newHttpHeaders([
      ("Tus-Resumable", "1.0.0"),
      ("Upload-Offset", "0"),
      ("Content-Type", "application/offset+octet-stream")
    ])
    
    let notFoundResponse = client.patch(TEST_BASE_URL & "/tus/nonexistent", body = "test")
    check notFoundResponse.code == Http404

when isMainModule:
  echo "Running upload integration tests..."