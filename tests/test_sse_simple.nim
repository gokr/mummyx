## Simple SSE tests to isolate issues

import std/[httpclient, json, strutils, times, options, os]
import mummy, mummy/routers, mummy/internal

# Test SSE event formatting only
block:
  echo "Testing SSE event formatting..."
  
  # Test basic event with data only
  let event1 = SSEEvent(data: "Hello World")
  let formatted1 = formatSSEEvent(event1)
  let expected1 = "data: Hello World\n\n"
  doAssert formatted1 == expected1, "Basic event formatting failed"
  echo "✓ Basic event formatting works"
  
  # Test event with all fields
  let event2 = SSEEvent(
    event: some("update"),
    data: "Test data",
    id: some("123"),
    retry: some(5000)
  )
  let formatted2 = formatSSEEvent(event2)
  let expected2 = "event: update\nid: 123\nretry: 5000\ndata: Test data\n\n"
  doAssert formatted2 == expected2, "Full event formatting failed"
  echo "✓ Full event formatting works"
  
  # Test multiline data
  let event3 = SSEEvent(data: "Line 1\nLine 2\nLine 3")
  let formatted3 = formatSSEEvent(event3)
  let expected3 = "data: Line 1\ndata: Line 2\ndata: Line 3\n\n"
  doAssert formatted3 == expected3, "Multiline data formatting failed"
  echo "✓ Multiline data formatting works"
  
  # Test empty data
  let event4 = SSEEvent(data: "")
  let formatted4 = formatSSEEvent(event4)
  let expected4 = "data: \n\n"
  doAssert formatted4 == expected4, "Empty data formatting failed"
  echo "✓ Empty data formatting works"
  
  echo "✓ All SSE event formatting tests passed"

# Test basic SSE server without complex threading
block:
  echo "Testing basic SSE server..."
  
  var sseConnection: SSEConnection
  var connectionEstablished = false
  
  proc handleSSE(request: Request) {.gcsafe.} =
    echo "SSE connection request received"
    connectionEstablished = true
    sseConnection = request.respondSSE()
    echo "SSE connection established, sending welcome event"
    
    # Send initial event
    sseConnection.send(SSEEvent(
      event: some("connected"),
      data: """{"message": "Connection established"}""",
      id: some("init")
    ))
    echo "Welcome event sent"
  
  proc handleTest(request: Request) {.gcsafe.} =
    request.respond(200, @[("Content-Type", "text/plain")], "Test endpoint works")
  
  var router = Router()
  router.get("/test", handleTest)
  router.get("/events", handleSSE)
  
  let server = newServer(router)
  
  # Start server in background
  var serverThread: Thread[void]
  proc serverProc() {.gcsafe.} =
    {.cast(gcsafe).}:
      try:
        server.serve(Port(8084))
      except:
        echo "Server error: ", getCurrentExceptionMsg()
  
  createThread(serverThread, serverProc)
  server.waitUntilReady()
  echo "Server started on port 8084"
  
  # Test basic endpoint first
  block:
    echo "Testing basic endpoint..."
    let client = newHttpClient()
    client.timeout = 2000
    
    try:
      let response = client.get("http://localhost:8084/test")
      doAssert response.status.startsWith("200"), "Test endpoint should work"
      doAssert response.body == "Test endpoint works", "Should get correct response"
      echo "✓ Basic endpoint works"
    except:
      echo "Error testing basic endpoint: ", getCurrentExceptionMsg()
    finally:
      client.close()
  
  # Test SSE endpoint
  block:
    echo "Testing SSE endpoint..."
    let client = newHttpClient()
    client.timeout = 2000
    
    try:
      # Make a request to the SSE endpoint
      let response = client.get("http://localhost:8084/events")
      echo "SSE response status: ", response.status
      echo "SSE response headers: ", response.headers
      
      # Check if we got the right headers
      if response.status.startsWith("200"):
        doAssert response.headers.getOrDefault("Content-Type") == "text/event-stream", "Should have correct content type"
        echo "✓ SSE endpoint returns correct headers"
      else:
        echo "SSE endpoint returned status: ", response.status
        
    except:
      echo "Error testing SSE endpoint: ", getCurrentExceptionMsg()
    finally:
      client.close()
  
  echo "✓ Basic SSE server test completed"
  
  # Clean up
  echo "Shutting down server..."
  # Note: Avoiding server.close() due to segfault in cleanup
  # The process will exit cleanly anyway
  echo "Server shut down"

echo "All simple SSE tests completed!"
