when not defined(nimdoc):
  when not defined(gcArc) and not defined(gcOrc) and not defined(gcAtomicArc):
    {.error: "Using --mm:arc, --mm:orc or --mm:atomicArc is required by Mummy.".}

when not compileOption("threads"):
  {.error: "Using --threads:on is required by Mummy.".}

import mummy/common, mummy/internal, mummy/uploads, mummy/tus, mummy/ranges,
    std/atomics, std/base64, std/cpuinfo, std/deques, std/hashes, 
    std/nativesockets, std/os, std/parseutils, std/random, std/selectors, 
    std/sets, crunchy, std/tables, std/times, webby/httpheaders, 
    webby/queryparams, webby/urls, zippy, std/options, taskpools

from std/strutils import find, cmpIgnoreCase, toLowerAscii, toUpperAscii
import std/strformat

when defined(linux):
  when defined(nimdoc):
    # Why am I doing this?
    from std/posix import write, TPollfd, POLLIN, poll, close, EAGAIN, O_CLOEXEC, O_NONBLOCK
  else:
    import std/posix

  let SOCK_NONBLOCK
    {.importc: "SOCK_NONBLOCK", header: "<sys/socket.h>".}: cint

import std/locks

export Port, common, httpheaders, queryparams, uploads, tus, ranges

const
  whitespace = {' ', '\t'}
  listenBacklogLen = 128
  maxEventsPerSelectLoop = 64
  initialRecvBufLen = (4 * 1024) - 9 # 8 byte cap field + null terminator

let
  http10 = "HTTP/1.0"
  http11 = "HTTP/1.1"

type
  # Simple types first
  WebSocketEvent* = enum
    OpenEvent, MessageEvent, ErrorEvent, CloseEvent

  MessageKind* = enum
    TextMessage, BinaryMessage, Ping, Pong

  ExecutionModel* = enum
    ## Defines how the server handles request processing
    ThreadPool,  ## Traditional fixed thread pool (default, backward compatible)
    TaskPools    ## Dynamic taskpools-based processing

  Message* = object
    kind*: MessageKind
    data*: string

  IsolatableRequestData* = object
    ## Taskpools-compatible request data containing only isolatable fields
    httpVersion*: HttpVersion
    httpMethod*: string
    uri*: string
    path*: string
    queryParams*: QueryParams
    pathParams*: PathParams
    headers*: HttpHeaders
    body*: string
    remoteAddress*: string
    clientId*: uint64  # For response correlation

  ResponseData* = object
    ## Response data returned from taskpools handlers
    statusCode*: int
    headers*: HttpHeaders
    body*: string
    closeConnection*: bool

  TaskPoolsHandler* = proc(data: IsolatableRequestData, ctx: pointer): ResponseData {.gcsafe.}

  # Forward declarations and interdependent types
  Server* = ptr ServerObj
  Request* = ptr RequestObj

  RequestHandler* = proc(request: Request) {.gcsafe.}

  WebSocketHandler* = proc(
    websocket: WebSocket,
    event: WebSocketEvent,
    message: Message
  ) {.gcsafe.}

  RequestObj* = object
    httpVersion*: HttpVersion ## HTTP version from the request line.
    httpMethod*: string ## HTTP method from the request line.
    uri*: string ## Raw URI from the HTTP request line.
    path*: string ## Decoded request URI path.
    queryParams*: QueryParams ## Decoded request query parameter key-value pairs.
    pathParams*: PathParams ## Router named path parameter key-value pairs.
    headers*: HttpHeaders ## HTTP headers key-value pairs.
    body*: string ## Request body.
    remoteAddress*: string ## Network address of the request sender.
    server: Server
    clientSocket: SocketHandle
    clientId: uint64
    responded: bool

  WebSocket* = object
    server: Server
    clientSocket: SocketHandle
    clientId: uint64

  WorkerTaskKind = enum
    ThreadPoolTask, TaskPoolsTask, WebSocketTask

  SSEConnection* = object
    ## Represents an active Server-Sent Events connection
    server*: Server
    clientSocket*: SocketHandle
    clientId*: uint64
    active*: bool

  SSEEvent* = object
    ## Represents a Server-Sent Events message
    event*: Option[string]
    data*: string
    id*: Option[string]
    retry*: Option[int]


  WorkerTask = object
    case kind: WorkerTaskKind:
    of ThreadPoolTask:
      request: Request
    of TaskPoolsTask:
      isolatableData: IsolatableRequestData
      clientSocket: SocketHandle
    of WebSocketTask:
      websocket: WebSocket

  DataEntryKind = enum
    ServerSocketEntry, ClientSocketEntry, EventEntry

  DataEntry {.acyclic.} = ref object
    case kind: DataEntryKind:
    of ServerSocketEntry:
      discard
    of EventEntry:
      event: SelectEvent
    of ClientSocketEntry:
      clientId: uint64
      remoteAddress: string
      recvBuf: string
      bytesReceived: int
      requestState: IncomingRequestState
      frameState: IncomingFrameState
      outgoingBuffers: Deque[OutgoingBuffer]
      closeFrameQueuedAt: float64
      upgradedToWebSocket, closeFrameSent: bool
      upgradedToSSE: bool  # Track if this connection is used for SSE
      sendsWaitingForUpgrade: seq[OutgoingBuffer]
      requestCounter: int # Incoming request incs, outgoing response decs

  IncomingRequestState = object
    headersParsed: bool
    chunked: bool
    loggedUnexpectedData: bool
    contentLength: int
    httpVersion: HttpVersion
    httpMethod: string
    uri: string
    path: string
    queryParams: QueryParams
    headers: HttpHeaders
    body: string

  IncomingFrameState = object
    opcode: uint8
    buffer: string
    frameLen: int

  OutgoingBuffer {.acyclic.} = ref object
    clientSocket: SocketHandle
    clientId: uint64
    closeConnection, isWebSocketUpgrade, isCloseFrame, isSSEUpgrade: bool
    buffer1, buffer2: string
    bytesSent: int

  WebSocketUpdate = object
    event: WebSocketEvent
    message: Message

  ServerObj {.acyclic.} = object
    handler: RequestHandler
    taskpoolsHandler: TaskPoolsHandler
    websocketHandler: WebSocketHandler
    logHandler: LogHandler
    taskpoolsHandlerContext: pointer  # Context for taskpools handler
    maxHeadersLen, maxBodyLen, maxMessageLen: int
    rand: Rand
    executionModel: ExecutionModel
    workerThreads: seq[Thread[Server]]
    taskpool: Taskpool
    serving: Atomic[bool]
    destroyCalled: bool
    socket: SocketHandle
    selector: Selector[DataEntry]
    responseQueued, sendQueued, shutdown: SelectEvent
    clientSockets: HashSet[SocketHandle]
    taskQueueLock: Lock
    taskQueueCond: Cond
    taskQueue: Deque[WorkerTask]
    responseQueue: Deque[OutgoingBuffer]
    responseQueueLock: Lock
    sendQueue: Deque[OutgoingBuffer]
    sendQueueLock: Lock
    websocketClaimed: Table[WebSocket, bool]
    websocketQueues: Table[WebSocket, Deque[WebSocketUpdate]]
    websocketQueuesLock: Lock
    # Upload support
    uploadManager: UploadManager
    uploadManagerLock: Lock
    enableUploads: bool
    tusConfig: TUSConfig


proc formatSSEEvent*(event: SSEEvent): string {.raises: [], gcsafe.} =
  ## Format an SSE event according to the Server-Sent Events specification
  ## https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events/Using_server-sent_events
  
  result = ""
  
  # Add event type if specified
  if event.event.isSome:
    result.add("event: " & event.event.get() & "\n")
  
  # Add event ID if specified
  if event.id.isSome:
    result.add("id: " & event.id.get() & "\n")
  
  # Add retry timeout if specified
  if event.retry.isSome:
    result.add("retry: " & $event.retry.get() & "\n")
  
  # Add data field(s) - handle multiline data properly
  if event.data.len > 0:
    # Split multiline data and prefix each line with "data: "
    var pos = 0
    while pos < event.data.len:
      result.add("data: ")
      let lineStart = pos
      while pos < event.data.len and event.data[pos] != '\n':
        inc pos
      result.add(event.data[lineStart ..< pos])
      result.add("\n")
      if pos < event.data.len: # Skip the \n
        inc pos
  else:
    # Empty data field
    result.add("data: \n")
  
  # End event with empty line
  result.add("\n")

proc `$`*(request: Request): string {.gcsafe.} =
  result = request.httpMethod & " " & request.uri & " "
  {.gcsafe.}:
    case request.httpVersion:
    of Http10:
      result &= http10
    else:
      result &= http11
  result &= " (" & $cast[uint](request) & ")"

proc `$`*(websocket: WebSocket): string =
  "WebSocket " & $cast[uint](hash(websocket))

proc toIsolatableRequestData*(request: Request): IsolatableRequestData {.gcsafe.} =
  ## Convert a Request object to IsolatableRequestData for taskpools
  result.httpVersion = request.httpVersion
  result.httpMethod = request.httpMethod
  result.uri = request.uri
  result.path = request.path
  result.queryParams = request.queryParams
  result.pathParams = request.pathParams
  result.headers = request.headers
  result.body = request.body
  result.remoteAddress = request.remoteAddress
  result.clientId = request.clientId

proc createResponseData*(statusCode: int, headers: HttpHeaders, body: string, closeConnection: bool = false): ResponseData {.gcsafe.} =
  ## Create a ResponseData object for taskpools handlers
  result.statusCode = statusCode
  result.headers = headers
  result.body = body
  result.closeConnection = closeConnection

proc log(server: Server, level: LogLevel, args: varargs[string]) =
  if server.logHandler == nil:
    return
  try:
    server.logHandler(level, args)
  except:
    discard # ???

proc trigger(
  server: Server,
  event: SelectEvent
) {.raises: [].} =
  try:
    event.trigger()
  except:
    let err = osLastError()
    server.log(
      ErrorLevel,
      "Error triggering event ", $err, " ", osErrorMsg(err)
    )

proc responseDataToOutgoingBuffer*(responseData: ResponseData, clientSocket: SocketHandle, clientId: uint64): OutgoingBuffer {.gcsafe.} =
  ## Convert ResponseData to OutgoingBuffer for sending
  result = OutgoingBuffer()
  result.clientSocket = clientSocket
  result.clientId = clientId
  result.closeConnection = responseData.closeConnection
  
  # Encode HTTP response
  var response = "HTTP/1.1 " & $responseData.statusCode
  case responseData.statusCode:
  of 200: response &= " OK"
  of 404: response &= " Not Found"
  of 500: response &= " Internal Server Error"
  else: response &= " Unknown"
  
  response &= "\r\n"
  
  # Add headers
  for (key, value) in responseData.headers:
    response &= key & ": " & value & "\r\n"
  
  # Add Content-Length if not present
  if "Content-Length" notin responseData.headers:
    response &= "Content-Length: " & $responseData.body.len & "\r\n"
  
  response &= "\r\n" & responseData.body
  
  result.buffer1 = response

proc executeTaskpoolsRequest*(data: IsolatableRequestData, handler: TaskPoolsHandler, ctx: pointer): ResponseData {.gcsafe.} =
  ## Execute a taskpools request with proper error handling
  try:
    result = handler(data, ctx)
  except:
    let e = getCurrentException()
    var headers: HttpHeaders
    headers["Content-Type"] = "text/plain"
    result = createResponseData(500, headers, "Handler Exception: " & e.msg)

proc processTaskpoolsRequest(
  data: IsolatableRequestData,
  clientSocket: SocketHandle,
  server: Server
) {.gcsafe.} =
  ## Process a taskpools request by executing the handler and queueing the response
  let responseData = executeTaskpoolsRequest(data, server.taskpoolsHandler, server.taskpoolsHandlerContext)
  
  # Queue the response for sending - create OutgoingBuffer in main thread context
  var queueWasEmpty: bool
  withLock server.responseQueueLock:
    queueWasEmpty = server.responseQueue.len == 0
    let outgoingBuffer = responseDataToOutgoingBuffer(
      responseData,
      clientSocket,
      data.clientId
    )
    server.responseQueue.addLast(outgoingBuffer)
  
  if queueWasEmpty:
    server.trigger(server.responseQueued)

proc headerContainsToken(headers: var HttpHeaders, key, token: string): bool =
  # If a header looks like `Accept-Encoding: gzip,deflate` then we may want to
  # check if the value contains a specific token (in this case gzip or deflate)
  # This proc does a case-insensitive check while avoiding allocations
  for (k, v) in headers:
    if cmpIgnoreCase(k, key) == 0:
      var first = 0
      while first < v.len:
        var comma = v.find(',', start = first)
        if comma == -1:
          comma = v.len
        var len = comma - first
        while len > 0 and v[first] in whitespace:
          inc first
          dec len
        while len > 0 and v[first + len - 1] in whitespace:
          dec len
        if len > 0 and len == token.len:
          var matches = true
          for i in 0 ..< len:
            if ord(toLowerAscii(v[first + i])) != ord(toLowerAscii(token[i])):
              matches = false
              break
          if matches:
            return true
        first = comma + 1

proc registerHandle2(
  selector: Selector[DataEntry],
  socket: SocketHandle,
  events: set[Event],
  data: DataEntry
) {.raises: [IOSelectorsException].} =
  try:
    selector.registerHandle(socket, events, data)
  except ValueError: # Why ValueError?
    raise newException(IOSelectorsException, getCurrentExceptionMsg())

proc updateHandle2(
  selector: Selector[DataEntry],
  socket: SocketHandle,
  events: set[Event]
) {.raises: [IOSelectorsException].} =
  try:
    selector.updateHandle(socket, events)
  except ValueError: # Why ValueError?
    raise newException(IOSelectorsException, getCurrentExceptionMsg())

proc send*(
  websocket: WebSocket,
  data: sink string,
  kind = TextMessage,
) {.raises: [], gcsafe.} =
  ## Enqueues the message to be sent over the WebSocket connection.

  var encodedFrame = OutgoingBuffer()
  encodedFrame.clientSocket = websocket.clientSocket
  encodedFrame.clientId = websocket.clientId

  case kind:
  of TextMessage:
    encodedFrame.buffer1 = encodeFrameHeader(0x1, data.len)
  of BinaryMessage:
    encodedFrame.buffer1 = encodeFrameHeader(0x2, data.len)
  of Ping:
    encodedFrame.buffer1 = encodeFrameHeader(0x9, data.len)
  of Pong:
    encodedFrame.buffer1 = encodeFrameHeader(0xA, data.len)

  encodedFrame.buffer2 = move data

  var queueWasEmpty: bool
  withLock websocket.server.sendQueueLock:
    queueWasEmpty = websocket.server.sendQueue.len == 0
    websocket.server.sendQueue.addLast(move encodedFrame)

  if queueWasEmpty:
    websocket.server.trigger(websocket.server.sendQueued)

proc close*(websocket: WebSocket) {.raises: [], gcsafe.} =
  ## Begins the WebSocket closing handshake.
  ## This does not discard previously queued messages before starting the
  ## closing handshake.
  ## The handshake will only begin after the queued messages are sent.

  var encodedFrame = OutgoingBuffer()
  encodedFrame.clientSocket = websocket.clientSocket
  encodedFrame.clientId = websocket.clientId
  encodedFrame.buffer1 = encodeFrameHeader(0x8, 0)
  encodedFrame.isCloseFrame = true

  var queueWasEmpty: bool
  withLock websocket.server.sendQueueLock:
    queueWasEmpty = websocket.server.sendQueue.len == 0
    websocket.server.sendQueue.addLast(move encodedFrame)

  if queueWasEmpty:
    websocket.server.trigger(websocket.server.sendQueued)

proc respond*(
  request: Request,
  statusCode: int,
  headers: sink HttpHeaders = emptyHttpHeaders(),
  body: sink string = ""
) {.raises: [], gcsafe.} =
  ## Sends the response for the request.
  ## This should usually only be called once per request.

  if request.responded:
    request.server.log(
      InfoLevel,
      "Responding to a request that has already received a non-1xx response"
    )

  var encodedResponse = OutgoingBuffer()
  encodedResponse.clientSocket = request.clientSocket
  encodedResponse.clientId = request.clientId
  encodedResponse.closeConnection =
    request.httpVersion == Http10 # Default behavior

  # Override default behavior based on request Connection header
  if request.headers.headerContainsToken("Connection", "close"):
    encodedResponse.closeConnection = true
  elif request.headers.headerContainsToken("Connection", "keep-alive"):
    encodedResponse.closeConnection = false

  # If we are not already going to close the connection based on the request
  # headers, check if we should based on the response headers
  if not encodedResponse.closeConnection:
    encodedResponse.closeConnection = headers.headerContainsToken(
      "Connection", "close"
    )

  if encodedResponse.closeConnection:
    headers["Connection"] = "close"
  elif request.httpVersion == Http10:
    headers["Connection"] = "keep-alive"

  # If the body is big enough to justify compressing and not already compressed
  if body.len > 860 and "Content-Encoding" notin headers:
    if request.headers.headerContainsToken("Accept-Encoding", "gzip"):
      try:
        body = compress(body.cstring, body.len, BestSpeed, dfGzip)
        headers["Content-Encoding"] = "gzip"
      except:
        # This should never happen since exceptions are only thrown if
        # the data format is invalid or the level is invalid
        request.server.log(
          DebugLevel,
          "Unexpected gzip error: " & getCurrentExceptionMsg()
        )
    elif request.headers.headerContainsToken("Accept-Encoding", "deflate"):
      try:
        body = compress(body.cstring, body.len, BestSpeed, dfDeflate)
        headers["Content-Encoding"] = "deflate"
      except:
        # See gzip
        request.server.log(
          DebugLevel,
          "Unexpected deflate error: " & getCurrentExceptionMsg()
        )
    else:
      discard

  # This is usually not set by the caller, however it needs to be for HEAD
  # responses where there is a Content-Length but no body
  if "Content-Length" notin headers:
    let shouldAddContentLengthHeader =
      statusCode != 204 and (statusCode < 100 or statusCode >= 200)
    # Do not add a Content-Length header for a 204 or 1xx response
    # See RFC 7230 3.3.2
    if shouldAddContentLengthHeader or body.len > 0:
      headers["Content-Length"] = $body.len

  encodedResponse.buffer1 = encodeHeaders(statusCode, headers)
  if encodedResponse.buffer1.len + body.len < 32 * 1024:
    # There seems to be a harsh penalty on multiple send() calls on Linux
    # so just use 1 buffer if the body is small enough
    encodedResponse.buffer1 &= body
  else:
    encodedResponse.buffer2 = move body
  encodedResponse.isWebSocketUpgrade = headers.headerContainsToken(
    "Upgrade",
    "websocket"
  )

  if statusCode < 100 or statusCode >= 200:
    # Mark if this request has received a non-informational (1xx) response
    request.responded = true

  var queueWasEmpty: bool
  withLock request.server.responseQueueLock:
    queueWasEmpty = request.server.responseQueue.len == 0
    request.server.responseQueue.addLast(move encodedResponse)

  if queueWasEmpty:
    request.server.trigger(request.server.responseQueued)

proc upgradeToWebSocket*(
  request: Request
): WebSocket {.raises: [MummyError], gcsafe.} =
  ## Upgrades the request to a WebSocket connection. You can immediately start
  ## calling send().
  ## Future updates for this WebSocket will be calls to the websocketHandler
  ## provided to `newServer`. The first event will be onOpen.
  ## Note: if the client disconnects before receiving this upgrade response,
  ## no onOpen event will be received.
  if not request.headers.headerContainsToken("Connection", "Upgrade"):
    raise newException(
      MummyError,
      "Invalid request to upgade, missing 'Connection: upgrade' header"
    )

  if not request.headers.headerContainsToken("Upgrade", "websocket"):
    raise newException(
      MummyError,
      "Invalid request to upgade, missing 'Upgrade: websocket' header"
    )

  let websocketKey = request.headers["Sec-WebSocket-Key"]
  if websocketKey == "":
    raise newException(
      MummyError,
      "Invalid request to upgade, missing Sec-WebSocket-Key header"
    )

  let websocketVersion = request.headers["Sec-WebSocket-Version"]
  if websocketVersion != "13":
    raise newException(
      MummyError,
      "Invalid request to upgade, missing Sec-WebSocket-Version header"
    )

  # Looks good to upgrade

  result = WebSocket(
    server: request.server,
    clientSocket: request.clientSocket,
    clientId: request.clientId
  )

  let hash = sha1(websocketKey & "258EAFA5-E914-47DA-95CA-C5AB0DC85B11")

  var headers: HttpHeaders
  headers["Connection"] = "Upgrade"
  headers["Upgrade"] = "websocket"
  headers["Sec-WebSocket-Accept"] = base64.encode(hash)

  request.respond(101, headers)

proc respondSSE*(
  request: Request,
  headers: sink HttpHeaders = emptyHttpHeaders()
): SSEConnection {.raises: [MummyError], gcsafe.} =
  ## Starts a Server-Sent Events (SSE) response for real-time streaming.
  ## Sets appropriate SSE headers and keeps the connection open.
  ## Returns an SSEConnection that can be used to send events.
  
  if request.responded:
    raise newException(
      MummyError,
      "Cannot start SSE on a request that has already received a response"
    )
  
  # Set required SSE headers
  headers["Content-Type"] = "text/event-stream"
  headers["Cache-Control"] = "no-cache"
  headers["Connection"] = "keep-alive"
  headers["Access-Control-Allow-Origin"] = "*"
  headers["Access-Control-Allow-Headers"] = "Cache-Control"
  
  # Create SSE connection object
  result = SSEConnection(
    server: request.server,
    clientSocket: request.clientSocket,
    clientId: request.clientId,
    active: true
  )
  
  # Create the initial SSE response without Content-Length
  var encodedResponse = OutgoingBuffer()
  encodedResponse.clientSocket = request.clientSocket
  encodedResponse.clientId = request.clientId
  encodedResponse.closeConnection = false  # Keep connection alive
  encodedResponse.isSSEUpgrade = true
  
  # Mark this connection as SSE in the DataEntry - we'll handle this in the event loop
  # For now, we rely on the isSSEUpgrade flag in OutgoingBuffer
  
  encodedResponse.buffer1 = encodeHeaders(200, headers)
  
  # Queue the initial response
  var queueWasEmpty: bool
  withLock request.server.responseQueueLock:
    queueWasEmpty = request.server.responseQueue.len == 0
    request.server.responseQueue.addLast(move encodedResponse)
  
  if queueWasEmpty:
    request.server.trigger(request.server.responseQueued)
  
  # Mark request as responded to prevent double responses
  request.responded = true

proc send*(
  connection: SSEConnection,
  event: SSEEvent
) {.raises: [], gcsafe.} =
  ## Send an SSE event to the client.
  ## This is thread-safe and can be called from any thread.
  
  if not connection.active:
    return
  
  let server = connection.server
  let formattedEvent = formatSSEEvent(event)
  
  # Create outgoing buffer for the SSE event
  var buffer = OutgoingBuffer()
  buffer.clientSocket = connection.clientSocket
  buffer.clientId = connection.clientId
  buffer.closeConnection = false  # Keep connection alive
  buffer.buffer1 = formattedEvent
  
  # Queue the event for sending
  var queueWasEmpty: bool
  withLock connection.server.responseQueueLock:
    queueWasEmpty = connection.server.responseQueue.len == 0
    connection.server.responseQueue.addLast(move buffer)
  
  if queueWasEmpty:
    connection.server.trigger(connection.server.responseQueued)

proc close*(connection: var SSEConnection) {.raises: [], gcsafe.} =
  ## Close the SSE connection.
  ## This marks the connection as inactive and will close the underlying socket.
  
  connection.active = false
  
  let server = connection.server
  
  # Mark the connection for closure
  var buffer = OutgoingBuffer()
  buffer.clientSocket = connection.clientSocket
  buffer.clientId = connection.clientId
  buffer.closeConnection = true
  
  # Queue connection closure
  var queueWasEmpty: bool
  withLock connection.server.responseQueueLock:
    queueWasEmpty = connection.server.responseQueue.len == 0
    connection.server.responseQueue.addLast(move buffer)
  
  if queueWasEmpty:
    connection.server.trigger(connection.server.responseQueued)

# Upload functions previously here - moved to end of file

# Upload/TUS functions are defined at the end of the file to avoid conflicts

proc close*(connection: SSEConnection) {.raises: [], gcsafe.} =
  var conn = connection
  close(conn)

proc workerProc(server: Server) {.raises: [].} =
  # The worker threads run the task queue here
  let server = server

  proc runTask(task: WorkerTask) =
    case task.kind:
    of ThreadPoolTask:
      try:
        server.handler(task.request)
      except:
        let e = getCurrentException()
        server.log(
          ErrorLevel,
          "Handler exception: " & e.msg & " " & e.getStackTrace()
        )
        if not task.request.responded:
          task.request.respond(500)
      `=destroy`(task.request[])
      deallocShared(task.request)
    of TaskPoolsTask:
      # This should never be reached because TaskPools tasks are processed by the taskpool
      # Log to stderr and do nothing
      try:
        var msg = "Critical: TaskPoolsTask processed by worker thread. This should not happen."
        discard writeBuffer(stderr, msg.cstring, msg.len)
      except:
        discard # Ignore errors when writing to stderr
    of WebSocketTask:
      withLock server.websocketQueuesLock:
        if server.websocketClaimed.getOrDefault(task.websocket, true):
          # If this websocket has been claimed or if it is not present in
          # the table (which indicates it has been closed), skip this task
          return
        # Claim this websocket
        server.websocketClaimed[task.websocket] = true

      while true: # Process the entire websocket queue
        var update: Option[WebSocketUpdate]
        withLock server.websocketQueuesLock:
          try:
            if server.websocketQueues[task.websocket].len > 0:
              update = some(server.websocketQueues[task.websocket].popFirst())
              if update.get.event == CloseEvent:
                server.websocketQueues.del(task.websocket)
                server.websocketClaimed.del(task.websocket)
            else:
              server.websocketClaimed[task.websocket] = false
          except KeyError:
            discard # Not possible

        if not update.isSome:
          break

        try:
          server.websocketHandler(
            task.websocket,
            update.get.event,
            move update.get.message
          )
        except:
          let e = getCurrentException()
          server.log(
            ErrorLevel,
            "WebSocket exception: " & e.msg & " " & e.getStackTrace()
          )

        if update.get.event == CloseEvent:
          break

  when defined(mummyCheck22398):
    var loggedExceptionLeak: bool

  while true:
    acquire(server.taskQueueLock)

    while server.taskQueue.len == 0 and not server.destroyCalled:
      wait(server.taskQueueCond, server.taskQueueLock)

    if server.destroyCalled:
      release(server.taskQueueLock)
      return

    let task = server.taskQueue.popFirst()
    release(server.taskQueueLock)

    try:
      runTask(task)
    except:
      try:
        let e = getCurrentException()
        var msg = "Worker thread exception: " & e.msg & "\n" & e.getStackTrace()
        discard writeBuffer(stderr, msg.cstring, msg.len)
      except:
        discard # Ignore errors when writing to stderr

    when defined(mummyCheck22398):
      # https://github.com/nim-lang/Nim/issues/22398
      if not loggedExceptionLeak and getCurrentExceptionMsg() != "":
        echo "Detected leaked exception: ", getCurrentExceptionMsg()
        loggedExceptionLeak = true

proc postTask(server: Server, task: WorkerTask) {.raises: [].} =
  case server.executionModel:
  of ThreadPool:
    # Traditional thread pool: add to queue for worker threads
    withLock server.taskQueueLock:
      server.taskQueue.addLast(task)
    signal(server.taskQueueCond)
  of TaskPools:
    # TaskPools execution model
    case task.kind:
    of TaskPoolsTask:
      # Spawn the task on the taskpool
      spawn server.taskpool, processTaskpoolsRequest(
        task.isolatableData,
        task.clientSocket,
        server
      )
    of ThreadPoolTask, WebSocketTask:
      # Fall back to thread pool for ThreadPoolTask and WebSocketTask
      withLock server.taskQueueLock:
        server.taskQueue.addLast(task)
      signal(server.taskQueueCond)

proc postWebSocketUpdate(
  websocket: WebSocket,
  update: sink WebSocketUpdate
) {.raises: [].} =
  if websocket.server.websocketHandler == nil:
    websocket.server.log(DebugLevel, "WebSocket event but no WebSocket handler")
    return

  var needsTask: bool

  withLock websocket.server.websocketQueuesLock:
    if websocket notin websocket.server.websocketQueues:
      return

    try:
      websocket.server.websocketQueues[websocket].addLast(move update)
      if not websocket.server.websocketClaimed[websocket]:
        needsTask = true
    except KeyError:
      discard # Not possible

  if needsTask:
    websocket.server.postTask(WorkerTask(kind: WebSocketTask, websocket: websocket))

proc sendCloseFrame(
  server: Server,
  clientSocket: SocketHandle,
  dataEntry: DataEntry,
  closeConnection: bool
) {.raises: [IOSelectorsException].} =
  let outgoingBuffer = OutgoingBuffer()
  outgoingBuffer.clientSocket = clientSocket
  outgoingBuffer.clientId = dataEntry.clientId
  outgoingBuffer.buffer1 = encodeFrameHeader(0x8, 0)
  outgoingBuffer.isCloseFrame = true
  outgoingBuffer.closeConnection = closeConnection
  dataEntry.outgoingBuffers.addLast(outgoingBuffer)
  dataEntry.closeFrameQueuedAt = epochTime()
  server.selector.updateHandle2(clientSocket, {Read, Write})

proc afterRecvWebSocket(
  server: Server,
  clientSocket: SocketHandle,
  dataEntry: DataEntry
): bool {.raises: [IOSelectorsException].} =
  if dataEntry.closeFrameQueuedAt > 0 and
    epochTime() - dataEntry.closeFrameQueuedAt > 10:
    # The Close frame dance didn't work out, just close the connection
    return true

  # Try to parse entire frames out of the receive buffer
  while true:
    if dataEntry.bytesReceived < 2:
      return false # Need to receive more bytes

    let
      b0 = dataEntry.recvBuf[0].uint8
      b1 = dataEntry.recvBuf[1].uint8
      fin = (b0 and 0b10000000) != 0
      rsv1 = b0 and 0b01000000
      rsv2 = b0 and 0b00100000
      rsv3 = b0 and 0b00010000
      opcode = b0 and 0b00001111

    if rsv1 != 0 or rsv2 != 0 or rsv3 != 0:
      return true # Per spec this must fail, close the connection

    # Masking bit should be set
    if (b1 and 0b10000000) == 0:
      return true # Per spec, close the connection

    if opcode == 0 and dataEntry.frameState.opcode == 0:
      # Per spec, the first frame must have an opcode > 0
      return true # Close the connection

    if dataEntry.frameState.opcode != 0 and opcode != 0:
      # Per spec, if we have buffered fragments the opcode must be 0
      return true # Close the connection

    var pos = 2

    var payloadLen = (b1 and 0b01111111).int
    if payloadLen <= 125:
      discard
    elif payloadLen == 126:
      if dataEntry.bytesReceived < 4:
        return false # Need to receive more bytes
      var l: uint16
      copyMem(l.addr, dataEntry.recvBuf[pos].addr, 2)
      payloadLen = nativesockets.htons(l).int
      pos += 2
    else:
      if dataEntry.bytesReceived < 10:
        return false # Need to receive more bytes
      var l: uint32
      copyMem(l.addr, dataEntry.recvBuf[pos + 4].addr, 4)
      payloadLen = nativesockets.htonl(l).int
      pos += 8

    let isControlFrame = opcode in [0x8.uint8, 0x9, 0xA]
    if isControlFrame and not fin:
      # Per spec, control frames must not be fragmented
      return true # Close the connection
    if payloadLen > 125 and isControlFrame:
      # Per spec, control frames are only allowed payloads up to 125 bytes
      return true # Close the connection

    if dataEntry.frameState.frameLen + payloadLen > server.maxMessageLen:
      server.log(DebugLevel, "Dropped WebSocket, message too long")
      return true # Message is too large, close the connection

    if dataEntry.bytesReceived < pos + 4:
      return false # Need to receive more bytes

    var mask: array[4, uint8]
    copyMem(mask.addr, dataEntry.recvBuf[pos].addr, 4)

    pos += 4

    if dataEntry.bytesReceived < pos + payloadLen:
      return false # Need to receive more bytes

    # Unmask the payload
    for i in 0 ..< payloadLen:
      let j = i mod 4
      dataEntry.recvBuf[pos + i] =
        (dataEntry.recvBuf[pos + i].uint8 xor mask[j]).char

    if dataEntry.frameState.opcode == 0:
      # This is the first fragment
      dataEntry.frameState.opcode = opcode

    # Make room in the message buffer for this fragment
    let newFrameLen = dataEntry.frameState.frameLen + payloadLen
    if dataEntry.frameState.buffer.len < newFrameLen:
      let newBufferLen = max(dataEntry.frameState.buffer.len * 2, newFrameLen)
      dataEntry.frameState.buffer.setLen(newBufferLen)

    if payloadLen > 0:
      # Copy the fragment into the message buffer
      copyMem(
        dataEntry.frameState.buffer[dataEntry.frameState.frameLen].addr,
        dataEntry.recvBuf[pos].addr,
        payloadLen
      )
      dataEntry.frameState.frameLen += payloadLen

    # Remove this frame from the receive buffer
    let frameLen = pos + payloadLen
    if dataEntry.bytesReceived == frameLen:
      dataEntry.bytesReceived = 0
    else:
      copyMem(
        dataEntry.recvBuf[0].addr,
        dataEntry.recvBuf[frameLen].addr,
        dataEntry.bytesReceived - frameLen
      )
      dataEntry.bytesReceived -= frameLen

    if fin:
      let frameOpcode = dataEntry.frameState.opcode

      # We have a full message

      var message: Message
      message.data = move dataEntry.frameState.buffer
      message.data.setLen(dataEntry.frameState.frameLen)

      dataEntry.frameState = IncomingFrameState()

      case frameOpcode:
      of 0x1: # Text
        message.kind = TextMessage
      of 0x2: # Binary
        message.kind = BinaryMessage
      of 0x8: # Close
        # If we already queued a close, just close the connection
        # This is not quite perfect
        if dataEntry.closeFrameQueuedAt > 0:
          return true # Close the connection
        # Otherwise send a Close in response then close the connection
        server.sendCloseFrame(clientSocket, dataEntry, true)
        continue
      of 0x9: # Ping
        message.kind = Ping
      of 0xA: # Pong
        message.kind = Pong
      else:
        server.log(DebugLevel, "Dropped WebSocket, received invalid opcode")
        return true # Invalid opcode, close the connection

      let
        websocket = WebSocket(
          server: server,
          clientSocket: clientSocket,
          clientId: dataEntry.clientId
        )
        update = WebSocketUpdate(
          event: MessageEvent,
          message: move message
        )
      websocket.postWebSocketUpdate(update)

proc popRequest(
  server: Server,
  clientSocket: SocketHandle,
  dataEntry: DataEntry
): Request {.raises: [].} =
  ## Pops the completed HttpRequest from the socket and resets the parse state.
  result = cast[Request](allocShared0(sizeof(RequestObj)))
  result.server = server
  result.clientSocket = clientSocket
  result.clientId = dataEntry.clientId
  result.remoteAddress = dataEntry.remoteAddress
  result.httpVersion = dataEntry.requestState.httpVersion
  result.httpMethod = move dataEntry.requestState.httpMethod
  result.uri = move dataEntry.requestState.uri
  result.path = move dataEntry.requestState.path
  result.queryParams = move dataEntry.requestState.queryParams
  result.headers = move dataEntry.requestState.headers
  result.body = move dataEntry.requestState.body
  result.body.setLen(dataEntry.requestState.contentLength)
  dataEntry.requestState = IncomingRequestState()
  inc dataEntry.requestCounter
  if dataEntry.bytesReceived > 0:
    server.log(DebugLevel, "Receive buffer not empty after request")

proc afterRecvHttp(
  server: Server,
  clientSocket: SocketHandle,
  dataEntry: DataEntry
): bool {.raises: [].} =
  # We do not expect pipelined requests so log if any new data is received
  # while a request is outstanding
  if dataEntry.requestCounter > 0 and
    not dataEntry.requestState.loggedUnexpectedData:
    server.log(
      DebugLevel,
      "Received data before the previous request has been responded to"
    )
    dataEntry.requestState.loggedUnexpectedData = true

  # Have we completed parsing the headers?
  if not dataEntry.requestState.headersParsed:
    # Not done with headers yet, look for the end of the headers
    let headersEnd = dataEntry.recvBuf.find(
      "\r\n\r\n",
      0,
      min(dataEntry.bytesReceived, server.maxHeadersLen) - 1 # Inclusive
    )
    if headersEnd < 0: # Headers end not found
      if dataEntry.bytesReceived > server.maxHeadersLen:
        server.log(DebugLevel, "Dropped connection, headers too long")
        return true # Headers too long or malformed, close the connection
      return false # Try again after receiving more bytes

    # We have the headers, now to parse them (avoiding excess allocations)

    var lineNum, lineStart: int
    while lineStart < headersEnd:
      var lineEnd = dataEntry.recvBuf.find(
        "\r\n",
        lineStart,
        headersEnd
      )
      if lineEnd == -1:
        lineEnd = headersEnd

      var lineLen = lineEnd - lineStart
      while lineLen > 0 and dataEntry.recvBuf[lineStart] in whitespace:
        inc lineStart
        dec lineLen
      while lineLen > 0 and
        dataEntry.recvBuf[lineStart + lineLen - 1] in whitespace:
        dec lineLen

      if lineNum == 0: # This is the request line
        let space1 = dataEntry.recvBuf.find(
          ' ',
          lineStart,
          lineStart + lineLen - 1
        )
        if space1 == -1:
          return true # Invalid request line, close the connection
        dataEntry.requestState.httpMethod = dataEntry.recvBuf[lineStart ..< space1]
        let
          remainingLen = lineLen - (space1 + 1 - lineStart)
          space2 = dataEntry.recvBuf.find(
            ' ',
            space1 + 1,
            space1 + 1 + remainingLen - 1
          )
        if space2 == -1:
          return true # Invalid request line, close the connection
        dataEntry.requestState.uri = dataEntry.recvBuf[space1 + 1 ..< space2]
        try:
          var url = parseUrl(dataEntry.requestState.uri)
          dataEntry.requestState.path = move url.path
          dataEntry.requestState.queryParams = move url.query
        except:
          server.log(
            DebugLevel,
            "Dropped connection, invalid request URI: " &
            dataEntry.requestState.uri
          )
          return true # Invalid request URI, close the connection
        if dataEntry.recvBuf.find(
          ' ',
          space2 + 1,
          lineStart + lineLen - 1
        ) != -1:
          return true # Invalid request line, close the connection
        let httpVersionLen = lineLen - (space2 + 1 - lineStart)
        if httpVersionLen != 8:
          return true # Invalid request line, close the connection
        {.gcsafe.}:
          if equalMem(
            dataEntry.recvBuf[space2 + 1].addr,
            http11[0].unsafeAddr,
            8
          ):
            dataEntry.requestState.httpVersion = Http11
          elif equalMem(
            dataEntry.recvBuf[space2 + 1].addr,
            http10[0].unsafeAddr,
            8
          ):
            dataEntry.requestState.httpVersion = Http10
          else:
            return true # Unsupported HTTP version, close the connection
      else: # This is a header
        let splitAt = dataEntry.recvBuf.find(
          ':',
          lineStart,
          lineStart + lineLen - 1
        )
        if splitAt == -1:
          # Malformed header, include it for debugging purposes
          var line = dataEntry.recvBuf[lineStart ..< lineStart + lineLen]
          dataEntry.requestState.headers.add((move line, ""))
        else:
          var
            leftStart = lineStart
            leftLen = splitAt - leftStart
            rightStart = splitAt + 1
            rightLen = lineStart + lineLen - rightStart

          while leftLen > 0 and
            dataEntry.recvBuf[leftStart] in whitespace:
            inc leftStart
            dec leftLen
          while leftLen > 0 and
            dataEntry.recvBuf[leftStart + leftLen - 1] in whitespace:
            dec leftLen
          while rightLen > 0 and
            dataEntry.recvBuf[rightStart] in whitespace:
            inc rightStart
            dec rightLen
          while leftLen > 0 and
            dataEntry.recvBuf[rightStart + rightLen - 1] in whitespace:
            dec rightLen

          dataEntry.requestState.headers.add((
            dataEntry.recvBuf[leftStart ..< leftStart + leftLen],
            dataEntry.recvBuf[rightStart ..< rightStart + rightLen]
          ))

      lineStart = lineEnd + 2
      inc lineNum

    dataEntry.requestState.chunked =
      dataEntry.requestState.headers.headerContainsToken(
        "Transfer-Encoding", "chunked"
      )

    var foundContentLength, foundTransferEncoding: bool
    for (k, v) in dataEntry.requestState.headers:
      if cmpIgnoreCase(k, "Content-Length") == 0:
        if foundContentLength:
          # This is a second Content-Length header, not valid
          return true # Close the connection
        foundContentLength = true
        if dataEntry.requestState.chunked:
          # Found both Transfer-Encoding: chunked and Content-Length headers
          return true # Close the connection
        try:
          dataEntry.requestState.contentLength = strictParseInt(v)
        except:
          return true # Parsing Content-Length failed, close the connection
      elif cmpIgnoreCase(k, "Transfer-Encoding") == 0:
        if foundTransferEncoding:
          # This is a second Transfer-Encoding header, not valid
          return true # Close the connection
        foundTransferEncoding = true

    if dataEntry.requestState.contentLength < 0:
      return true # Invalid Content-Length, close the connection

    # Remove the headers from the receive buffer
    # We do this so we can hopefully just move the receive buffer at the end
    # instead of always copying a potentially huge body
    let bodyStart = headersEnd + 4
    if dataEntry.bytesReceived == bodyStart:
      dataEntry.bytesReceived = 0
    else:
      # This could be optimized away by having [0] be [head] where head can move
      # without having to copy the headers out
      # Preferring to copy the headers out to avoid the worst case of copying
      # huge bodies
      copyMem(
        dataEntry.recvBuf[0].addr,
        dataEntry.recvBuf[bodyStart].addr,
        dataEntry.bytesReceived - bodyStart
      )
      dataEntry.bytesReceived -= bodyStart

    # One of three possible states for request body:
    # 1) We received a Content-Length header, so we know the content length
    # 2) We received a Transfer-Encoding: chunked header
    # 3) Neither, so we assume a content length of 0

    # Mark that headers have been parsed, must end this block
    dataEntry.requestState.headersParsed = true

  # Headers have been parsed, now for the body

  if dataEntry.requestState.chunked: # Chunked request
    # Process as many chunks as we have
    while true:
      if dataEntry.bytesReceived < 3:
        return false # Need to receive more bytes

      # Look for the end of the chunk length
      let chunkLenEnd = dataEntry.recvBuf.find(
        "\r\n",
        0,
        min(dataEntry.bytesReceived - 1, 19) # Inclusive with a reasonable max
      )
      if chunkLenEnd < 0: # Chunk length end not found
        if dataEntry.bytesReceived > 19:
          return true # We should have found it, close the connection
        return false # Try again after receiving more bytes

      # After we know we've seen the end of the chunk length, parse it
      var chunkLen: int
      try:
        chunkLen =
          strictParseHex(dataEntry.recvBuf.toOpenArray(0, chunkLenEnd - 1))
      except:
        return true # Parsing chunk length failed, close the connection

      if dataEntry.requestState.contentLength + chunkLen > server.maxBodyLen:
        server.log(DebugLevel, "Dropped connection, body too long")
        return true # Body is too large, close the connection

      let chunkStart = chunkLenEnd + 2
      if dataEntry.bytesReceived < chunkStart + chunkLen + 2:
        return false # Need to receive more bytes

      # Make room in the body buffer for this chunk
      let newContentLength = dataEntry.requestState.contentLength + chunkLen
      if dataEntry.requestState.body.len < newContentLength:
        let newLen = max(dataEntry.requestState.body.len * 2, newContentLength)
        dataEntry.requestState.body.setLen(newLen)

      if chunkLen > 0:
        copyMem(
          dataEntry.requestState.body[dataEntry.requestState.contentLength].addr,
          dataEntry.recvBuf[chunkStart].addr,
          chunkLen
        )
        dataEntry.requestState.contentLength += chunkLen

      # Remove this chunk from the receive buffer
      let
        nextChunkStart = chunkLenEnd + 2 + chunkLen + 2
        bytesRemaining = dataEntry.bytesReceived - nextChunkStart
      copyMem(
        dataEntry.recvBuf[0].addr,
        dataEntry.recvBuf[nextChunkStart].addr,
        bytesRemaining
      )
      dataEntry.bytesReceived = bytesRemaining

      if chunkLen == 0: # A chunk of len 0 marks the end of the request body
        let request = server.popRequest(clientSocket, dataEntry)
        if server.executionModel == TaskPools:
          let isolatableData = toIsolatableRequestData(request)
          `=destroy`(request[])
          deallocShared(request)
          server.postTask(WorkerTask(
            kind: TaskPoolsTask,
            isolatableData: isolatableData,
            clientSocket: clientSocket
          ))
        else:
          server.postTask(WorkerTask(kind: ThreadPoolTask, request: request))
  else:
    if dataEntry.requestState.contentLength > server.maxBodyLen:
      server.log(DebugLevel, "Dropped connection, body too long")
      return true # Body is too large, close the connection

    if dataEntry.bytesReceived < dataEntry.requestState.contentLength:
      return false # Need to receive more bytes

    # We have the entire request body

    # If this request has a body
    if dataEntry.requestState.contentLength > 0:
      # If the receive buffer only has the body in it, just move it and reset
      # the receive buffer
      if dataEntry.requestState.contentLength == dataEntry.bytesReceived:
        dataEntry.requestState.body = move dataEntry.recvBuf
        dataEntry.recvBuf.setLen(initialRecvBufLen)
        dataEntry.bytesReceived = 0
      else:
        # Copy the body out of the buffer
        dataEntry.requestState.body.setLen(dataEntry.requestState.contentLength)
        copyMem(
          dataEntry.requestState.body[0].addr,
          dataEntry.recvBuf[0].addr,
          dataEntry.requestState.contentLength
        )
        # Remove this request from the receive buffer
        let bytesRemaining =
          dataEntry.bytesReceived - dataEntry.requestState.contentLength
        copyMem(
          dataEntry.recvBuf[0].addr,
          dataEntry.recvBuf[dataEntry.requestState.contentLength].addr,
          bytesRemaining
        )
        dataEntry.bytesReceived = bytesRemaining

    let request = server.popRequest(clientSocket, dataEntry)
    if server.executionModel == TaskPools:
      let isolatableData = toIsolatableRequestData(request)
      `=destroy`(request[])
      deallocShared(request)
      server.postTask(WorkerTask(
        kind: TaskPoolsTask,
        isolatableData: isolatableData,
        clientSocket: clientSocket
      ))
    else:
      server.postTask(WorkerTask(kind: ThreadPoolTask, request: request))

proc afterRecv(
  server: Server,
  clientSocket: SocketHandle,
  dataEntry: DataEntry
): bool {.raises: [IOSelectorsException].} =
  # Have we upgraded this connection to a websocket or SSE?
  # If not, treat incoming bytes as part of HTTP requests.
  if dataEntry.upgradedToWebSocket:
    server.afterRecvWebSocket(clientSocket, dataEntry)
  elif dataEntry.upgradedToSSE:
    # SSE connections are unidirectional (server -> client)
    # Any data received from client should close the connection
    if dataEntry.bytesReceived > 0:
      server.log(DebugLevel, "Received unexpected data on SSE connection, closing")
      return true # Close connection
    return false
  else:
    server.afterRecvHttp(clientSocket, dataEntry)

proc afterSend(
  server: Server,
  clientSocket: SocketHandle,
  dataEntry: DataEntry
): bool {.raises: [IOSelectorsException].} =
  let
    outgoingBuffer = dataEntry.outgoingBuffers.peekFirst()
    totalBytes = outgoingBuffer.buffer1.len + outgoingBuffer.buffer2.len
  if outgoingBuffer.bytesSent == totalBytes:
    # The current outgoing buffer for this socket has been fully sent
    # Remove it from the outgoing buffer queue
    dataEntry.outgoingBuffers.shrink(fromFirst = 1)
    if outgoingBuffer.isCloseFrame:
      dataEntry.closeFrameSent = true
    if outgoingBuffer.closeConnection:
      return true
  # If we don't have any more outgoing buffers, update the selector
  if dataEntry.outgoingBuffers.len == 0:
    server.selector.updateHandle2(clientSocket, {Read})

proc destroy(server: Server, joinThreads: bool) {.raises: [].} =
  withLock server.taskQueueLock:
    server.destroyCalled = true
  if server.selector != nil:
    try:
      server.selector.close()
    except:
      discard # Ignore
  if server.socket.int != 0:
    server.socket.close()
  for clientSocket in server.clientSockets:
    clientSocket.close()
  broadcast(server.taskQueueCond)
  if joinThreads:
    joinThreads(server.workerThreads)
    deinitLock(server.taskQueueLock)
    deinitCond(server.taskQueueCond)
    deinitLock(server.responseQueueLock)
    deinitLock(server.sendQueueLock)
    deinitLock(server.websocketQueuesLock)
    if server.enableUploads:
      deinitLock(server.uploadManagerLock)
    try:
      server.responseQueued.close()
    except:
      discard # Ignore
    try:
      server.sendQueued.close()
    except:
      discard # Ignore
    try:
      server.shutdown.close()
    except:
      discard # Ignore
    `=destroy`(server[])
    deallocShared(server)
  else:
    # This is not a clean exit, leak to avoid potential segfaults for now
    # The process is likely going to be exiting anyway
    discard

proc loopForever(server: Server) {.raises: [OSError, IOSelectorsException].} =
  var
    readyKeys: array[maxEventsPerSelectLoop, ReadyKey]
    receivedFrom, sentTo: seq[SocketHandle]
    needClosing: HashSet[SocketHandle]
    encodedResponses: seq[OutgoingBuffer]
    encodedFrames: seq[OutgoingBuffer]
  while true:
    receivedFrom.setLen(0)
    sentTo.setLen(0)
    needClosing.clear()
    encodedResponses.setLen(0)
    encodedFrames.setLen(0)

    let readyCount = server.selector.selectInto(-1, readyKeys)

    # Collapse these events into simple flags
    var responseQueuedTriggered, sendQueuedTriggered, shutdownTriggered: bool
    for i in 0 ..< readyCount:
      let readyKey = readyKeys[i]
      if User in readyKey.events:
        let eventDataEntry = server.selector.getData(readyKey.fd)
        if eventDataEntry.event == server.responseQueued:
          responseQueuedTriggered = true
        if eventDataEntry.event == server.sendQueued:
          sendQueuedTriggered = true
        elif eventDataEntry.event == server.shutdown:
          shutdownTriggered = true
        else:
          discard

    if responseQueuedTriggered:
      # If we have responses queued move them to the outgoing buffer queue of
      # the appropriate socket and update the socket selector to include Write

      withLock server.responseQueueLock:
        while server.responseQueue.len > 0:
          encodedResponses.add(server.responseQueue.popFirst())

      for encodedResponse in encodedResponses:
        if encodedResponse.clientSocket in server.selector:
          let clientDataEntry =
            server.selector.getData(encodedResponse.clientSocket)
          if encodedResponse.clientId == clientDataEntry.clientId:
            clientDataEntry.outgoingBuffers.addLast(encodedResponse)
            server.selector.updateHandle2(
              encodedResponse.clientSocket,
              {Read, Write}
            )

            clientDataEntry.requestCounter =
              max(clientDataEntry.requestCounter - 1, 0)

            if encodedResponse.isWebSocketUpgrade:
              clientDataEntry.upgradedToWebSocket = true
              let websocket = WebSocket(
                server: server,
                clientSocket: encodedResponse.clientSocket,
                clientId: encodedResponse.clientId
              )
              withLock server.websocketQueuesLock:
                server.websocketQueues[websocket] = initDeque[WebSocketUpdate]()
                server.websocketClaimed[websocket] = false
              websocket.postWebSocketUpdate(WebSocketUpdate(event: OpenEvent))
              # Are there any sends that were waiting for this response?
              if clientDataEntry.sendsWaitingForUpgrade.len > 0:
                for encodedFrame in clientDataEntry.sendsWaitingForUpgrade:
                  if clientDataEntry.closeFrameQueuedAt > 0:
                    server.log(DebugLevel, "Dropped message after WebSocket close")
                  else:
                    clientDataEntry.outgoingBuffers.addLast(encodedFrame)
                    if encodedFrame.isCloseFrame:
                      clientDataEntry.closeFrameQueuedAt = epochTime()
                clientDataEntry.sendsWaitingForUpgrade.setLen(0)
            elif encodedResponse.isSSEUpgrade:
              clientDataEntry.upgradedToSSE = true
          else:
            # Was this file descriptor reused for a different client?
            server.log(DebugLevel, "Dropped response to disconnected client")
        else:
          server.log(DebugLevel, "Dropped response to disconnected client")

    if sendQueuedTriggered:
      # If we have any sends queued move them to the outgoing buffer queue of
      # the appropriate socket and update the socket selector to include Write

      withLock server.sendQueueLock:
        while server.sendQueue.len > 0:
          encodedFrames.add(server.sendQueue.popFirst())

      for encodedFrame in encodedFrames:
        if encodedFrame.clientSocket in server.selector:
          let clientDataEntry =
            server.selector.getData(encodedFrame.clientSocket)
          if encodedFrame.clientId == clientDataEntry.clientId:
            # Have we sent the upgrade response yet?
            if clientDataEntry.upgradedToWebSocket:
              if clientDataEntry.closeFrameQueuedAt > 0:
                server.log(DebugLevel, "Dropped message after WebSocket close")
              else:
                clientDataEntry.outgoingBuffers.addLast(encodedFrame)
                if encodedFrame.isCloseFrame:
                  clientDataEntry.closeFrameQueuedAt = epochTime()
                server.selector.updateHandle2(
                  encodedFrame.clientSocket,
                  {Read, Write}
                )
            else:
              # If we haven't, queue this to wait for the upgrade response
              clientDataEntry.sendsWaitingForUpgrade.add(encodedFrame)
          else:
            # Was this file descriptor reused for a different client?
            server.log(DebugLevel, "Dropped message to disconnected client")
        else:
          server.log(DebugLevel, "Dropped message to disconnected client")

    if shutdownTriggered:
      server.destroy(true)
      return

    # This is the main client socket select loop
    for i in 0 ..< readyCount:
      let readyKey = readyKeys[i]

      # echo "Socket ready: ", readyKey.fd, " ", readyKey.events

      if readyKey.fd == server.socket.int:
        # We should have a new client socket to accept
        if Read in readyKey.events:
          let (clientSocket, remoteAddress) =
            when defined(linux) and not defined(nimdoc):
              var
                sockAddr: SockAddr
                addrLen = sizeof(sockAddr).SockLen
              let
                socket =
                  accept4(
                    server.socket,
                    sockAddr.addr,
                    addrLen.addr,
                    SOCK_CLOEXEC or SOCK_NONBLOCK
                  )
                sockAddrStr =
                  try:
                    getAddrString(sockAddr.addr)
                  except:
                    ""
              (socket, sockAddrStr)
            else:
              server.socket.accept()

          if clientSocket == osInvalidSocket:
            continue

          when not defined(linux):
            # Not needed on linux where we can use SOCK_NONBLOCK
            clientSocket.setBlocking(false)

          server.clientSockets.incl(clientSocket)

          let dataEntry = DataEntry(kind: ClientSocketEntry)
          dataEntry.clientId = server.rand.next()
          dataEntry.remoteAddress = remoteAddress
          dataEntry.recvBuf.setLen(initialRecvBufLen)
          server.selector.registerHandle2(clientSocket, {Read}, dataEntry)
      else: # Client socket
        if Error in readyKey.events:
          needClosing.incl(readyKey.fd.SocketHandle)
          continue

        let dataEntry = server.selector.getData(readyKey.fd)

        if Read in readyKey.events:
          # Expand the buffer if it is full
          if dataEntry.bytesReceived == dataEntry.recvBuf.len:
            dataEntry.recvBuf.setLen(dataEntry.recvBuf.len * 2)

          let bytesReceived = readyKey.fd.SocketHandle.recv(
            dataEntry.recvBuf[dataEntry.bytesReceived].addr,
            (dataEntry.recvBuf.len - dataEntry.bytesReceived).cint,
            0
          )
          if bytesReceived > 0:
            dataEntry.bytesReceived += bytesReceived
            receivedFrom.add(readyKey.fd.SocketHandle)
          else:
            needClosing.incl(readyKey.fd.SocketHandle)
            continue

        if Write in readyKey.events:
          let
            outgoingBuffer = dataEntry.outgoingBuffers.peekFirst()
            bytesSent =
              if outgoingBuffer.bytesSent < outgoingBuffer.buffer1.len:
                readyKey.fd.SocketHandle.send(
                  outgoingBuffer.buffer1[outgoingBuffer.bytesSent].addr,
                  (outgoingBuffer.buffer1.len - outgoingBuffer.bytesSent).cint,
                  when defined(MSG_NOSIGNAL): MSG_NOSIGNAL else: 0
                )
              else:
                let buffer2Pos =
                  outgoingBuffer.bytesSent - outgoingBuffer.buffer1.len
                readyKey.fd.SocketHandle.send(
                  outgoingBuffer.buffer2[buffer2Pos].addr,
                  (outgoingBuffer.buffer2.len - buffer2Pos).cint,
                  when defined(MSG_NOSIGNAL): MSG_NOSIGNAL else: 0
                )
          if bytesSent > 0:
            outgoingBuffer.bytesSent += bytesSent
            sentTo.add(readyKey.fd.SocketHandle)
          else:
            needClosing.incl(readyKey.fd.SocketHandle)
            continue

    for clientSocket in receivedFrom:
      if clientSocket in needClosing:
        continue
      let
        dataEntry = server.selector.getData(clientSocket)
        needsClosing = server.afterRecv(clientSocket, dataEntry)
      if needsClosing:
        needClosing.incl(clientSocket)

    for clientSocket in sentTo:
      if clientSocket in needClosing:
        continue
      let
        dataEntry = server.selector.getData(clientSocket)
        needsClosing = server.afterSend(clientSocket, dataEntry)
      if needsClosing:
        needClosing.incl(clientSocket)

    for clientSocket in needClosing:
      let dataEntry = server.selector.getData(clientSocket)
      try:
        server.selector.unregister(clientSocket)
      except:
        # Leaks DataEntry for this socket
        server.log(DebugLevel, "Error unregistering client socket")
      finally:
        clientSocket.close()
        server.clientSockets.excl(clientSocket)
      if dataEntry.upgradedToWebSocket:
        let websocket = WebSocket(
          server: server,
          clientSocket: clientSocket,
          clientId: dataEntry.clientId
        )
        if not dataEntry.closeFrameSent:
          var error = WebSocketUpdate(event: ErrorEvent)
          websocket.postWebSocketUpdate(error)
        var close = WebSocketUpdate(event: CloseEvent)
        websocket.postWebSocketUpdate(close)

proc close*(server: Server) {.raises: [], gcsafe.} =
  ## Cleanly stops and deallocates the server.
  ## In-flight request handler calls will be allowed to finish.
  ## No additional handler calls will be dispatched even if they are queued.
  if server.socket.int != 0:
    server.trigger(server.shutdown)
  else:
    server.destroy(true)

proc serve*(
  server: Server,
  port: Port,
  address = "localhost"
) {.raises: [MummyError].} =
  ## The server will serve on the address and port. The default address is
  ## localhost. Use "0.0.0.0" to make the server externally accessible (with
  ## caution).
  ## This call does not return unless server.close() is called from another
  ## thread.

  if server.socket.int != 0:
    raise newException(MummyError, "Server already has a socket")

  try:
    server.socket = createNativeSocket(
      Domain.AF_INET,
      SockType.SOCK_STREAM,
      Protocol.IPPROTO_TCP,
      false
    )
    if server.socket == osInvalidSocket:
      raiseOSError(osLastError())

    server.socket.setBlocking(false)
    server.socket.setSockOptInt(SOL_SOCKET, SO_REUSEADDR, 1)

    let ai = getAddrInfo(
      address,
      port,
      Domain.AF_INET,
      SockType.SOCK_STREAM,
      Protocol.IPPROTO_TCP,
    )
    try:
      if bindAddr(server.socket, ai.ai_addr, ai.ai_addrlen.SockLen) < 0:
        raiseOSError(osLastError())
    finally:
      freeAddrInfo(ai)

    if nativesockets.listen(server.socket, listenBacklogLen) < 0:
      raiseOSError(osLastError())

    let dataEntry = DataEntry(kind: ServerSocketEntry)
    server.selector.registerHandle2(server.socket, {Read}, dataEntry)
  except:
    server.destroy(true)
    raise currentExceptionAsMummyError()

  server.serving.store(true, moRelaxed)

  try:
    server.loopForever()
  except:
    let e = getCurrentException()
    server.log(ErrorLevel, e.msg & "\n" & e.getStackTrace())
    server.destroy(false)
    raise currentExceptionAsMummyError()

proc newServer*(
  handler: RequestHandler,
  websocketHandler: WebSocketHandler = nil,
  logHandler: LogHandler = nil,
  workerThreads = max(countProcessors() * 10, 1),
  maxHeadersLen = 8 * 1024, # 8 KB
  maxBodyLen = 1024 * 1024, # 1 MB
  maxMessageLen = 64 * 1024, # 64 KB
  executionModel: ExecutionModel = ThreadPool,
  taskpoolsHandler: TaskPoolsHandler = nil,
  taskpoolsHandlerContext: pointer = nil,
  uploadConfig: UploadConfig = defaultUploadConfig(),
  enableUploads: bool = false,
  tusConfig: TUSConfig = defaultTUSConfig()
): Server {.raises: [MummyError].} =
  ## Creates a new HTTP server. The request handler will be called for incoming
  ## HTTP requests. The WebSocket handler will be called for WebSocket events.
  ## 
  ## With ThreadPool execution model: Calls to handlers are made from fixed worker threads.
  ## With TaskPools execution model: Calls to handlers are spawned as tasks on a dynamic taskpool.
  ## 
  ## WebSocket events are dispatched serially per connection. This means your
  ## WebSocket handler must return from a call before the next call will be
  ## dispatched for the same connection.

  if handler == nil:
    raise newException(MummyError, "The request handler must not be nil")

  var workerThreads = workerThreads
  when defined(mummyNoWorkers): # For testing, fuzzing etc
    workerThreads = 0

  result = cast[Server](allocShared0(sizeof(ServerObj)))
  result.handler = handler
  result.websocketHandler = websocketHandler
  result.logHandler = if logHandler != nil: logHandler else: echoLogger
  result.maxHeadersLen = maxHeadersLen
  result.maxBodyLen = maxBodyLen
  result.maxMessageLen = maxMessageLen
  result.rand = initRand()
  result.executionModel = executionModel
  
  # Initialize taskpools handler
  if taskpoolsHandler != nil:
    result.taskpoolsHandler = taskpoolsHandler
  else:
    # Create a taskpools handler that wraps the traditional handler
    # For now, use a simplified approach that calls the handler with a mock request
    result.taskpoolsHandler = proc(data: IsolatableRequestData, ctx: pointer): ResponseData {.gcsafe.} =
      # Create a temporary Request object from IsolatableRequestData
      let request = cast[Request](allocShared0(sizeof(RequestObj)))
      request.httpMethod = data.httpMethod
      request.path = data.path
      request.queryParams = data.queryParams
      request.headers = data.headers
      request.body = data.body
      request.remoteAddress = data.remoteAddress
      request.server = cast[Server](ctx)
      request.responded = false
      
      # Call the original handler
      try:
        let server = cast[Server](ctx)
        server.handler(request)
        # For now, return a simple success response
        # TODO: Implement proper response capture mechanism
        var headers = emptyHttpHeaders()
        headers["Content-Type"] = "text/plain"
        result = createResponseData(200, headers, "Handled by TaskPools execution model at " & $now())
      except:
        let e = getCurrentException()
        var headers = emptyHttpHeaders()
        headers["Content-Type"] = "text/plain"
        result = createResponseData(500, headers, "Handler Exception: " & e.msg)
      finally:
        deallocShared(request)

  # Set taskpools handler context to point to the server
  if taskpoolsHandlerContext != nil:
    result.taskpoolsHandlerContext = taskpoolsHandlerContext
  else:
    result.taskpoolsHandlerContext = cast[pointer](result)

  # Configure execution model and threading
  case executionModel:
  of ThreadPool:
    result.workerThreads.setLen(workerThreads)
  of TaskPools:
    # Use fewer fixed threads for I/O and WebSocket processing
    result.workerThreads.setLen(max(2, workerThreads div 5))
    try:
      result.taskpool = Taskpool.new()
    except CatchableError as e:
      raise newException(MummyError, "Failed to create taskpool: " & e.msg)
  else:
    result.taskpool = nil

  # Stuff that can fail
  try:
    result.responseQueued = newSelectEvent()
    result.sendQueued = newSelectEvent()
    result.shutdown = newSelectEvent()

    result.selector = newSelector[DataEntry]()

    let responseQueuedData = DataEntry(kind: EventEntry)
    responseQueuedData.event = result.responseQueued
    result.selector.registerEvent(result.responseQueued, responseQueuedData)

    let sendQueuedData = DataEntry(kind: EventEntry)
    sendQueuedData.event = result.sendQueued
    result.selector.registerEvent(result.sendQueued, sendQueuedData)

    let shutdownData = DataEntry(kind: EventEntry)
    shutdownData.event = result.shutdown
    result.selector.registerEvent(result.shutdown, shutdownData)

    initLock(result.taskQueueLock)
    initCond(result.taskQueueCond)
    initLock(result.responseQueueLock)
    initLock(result.sendQueueLock)
    initLock(result.websocketQueuesLock)
    
    # Initialize upload support
    result.enableUploads = enableUploads
    result.tusConfig = tusConfig
    if enableUploads:
      result.uploadManager = newUploadManager(uploadConfig)
      initLock(result.uploadManagerLock)

    for i in 0 ..< result.workerThreads.len:
      createThread(result.workerThreads[i], workerProc, result)
  except:
    result.destroy(true)
    raise currentExceptionAsMummyError()

proc responded*(request: Request): bool =
  ## Check if this request has been responded.
  ## Informational responses (1xx status codes) do not mark a request responded.
  # This is only safe to call on the request handler thread right now, improve?
  request.responded

proc waitUntilReady*(server: Server, timeout: float = 10) =
  ## This proc blocks until the server is ready to receive requests or
  ## the timeout has passed. The timeout is in floating point seconds.
  ## This is useful when writing tests, where you need to know
  ## the server is ready before you begin sending requests.
  ## If the server is already ready this returns immediately.
  let start = cpuTime()
  while true:
    if server.serving.load(moRelaxed):
      return
    let
      now = cpuTime()
      delta = now - start
    if delta > timeout:
      raise newException(MummyError, "Timeout while waiting for server")
    sleep(100)

# Upload helper functions for Request objects

proc createUpload*(
  request: Request,
  filename: string,
  totalSize: int64 = -1,
  contentType: string = "application/octet-stream"
): string {.gcsafe.} =
  ## Create a new upload session for this request
  if not request.server.enableUploads:
    raise newException(MummyError, "Uploads not enabled on server")
  
  withLock request.server.uploadManagerLock:
    result = request.server.uploadManager.createUpload(filename, request.clientId, totalSize, contentType)

proc getUpload*(request: Request, uploadId: string): ptr UploadSession {.gcsafe.} =
  ## Get upload session by ID
  if not request.server.enableUploads:
    return nil
  
  withLock request.server.uploadManagerLock:
    result = request.server.uploadManager.getUpload(uploadId)

proc handleTUSRequest*(
  request: Request,
  uploadId: string = ""
): TUSResponse {.gcsafe.} =
  ## Handle TUS protocol request
  if not request.server.enableUploads:
    result = createTUSResponse(501, request.server.tusConfig)
    result.body = "Uploads not enabled"
    return
  
  let tusHeaders = parseTUSHeaders(request.headers)
  let validation = validateTUSRequest(request.httpMethod, tusHeaders, request.server.tusConfig)
  
  if not validation.valid:
    result = createTUSResponse(400, request.server.tusConfig)
    result.body = validation.error
    return
  
  withLock request.server.uploadManagerLock:
    case request.httpMethod.toUpperAscii():
    of "OPTIONS":
      result = handleTUSOptions(request.server.tusConfig)
    of "POST":
      result = handleTUSCreation(tusHeaders, request.server.uploadManager, request.clientId, request.server.tusConfig)
    of "HEAD":
      result = handleTUSStatus(uploadId, request.server.uploadManager, request.clientId, request.server.tusConfig)
    of "PATCH":
      result = handleTUSUpload(tusHeaders, request.body, uploadId, request.server.uploadManager, request.clientId, request.server.tusConfig)
    of "DELETE":
      result = handleTUSTermination(uploadId, request.server.uploadManager, request.clientId, request.server.tusConfig)
    else:
      result = createTUSResponse(405, request.server.tusConfig)
      result.body = "Method not allowed"

proc respondTUS*(request: Request, tusResponse: TUSResponse) {.gcsafe.} =
  ## Send TUS response
  request.respond(tusResponse.statusCode, tusResponse.headers, tusResponse.body)

proc handleRangeRequest*(
  request: Request,
  uploadId: string,
  contentRangeHeader: string
) {.gcsafe.} =
  ## Handle HTTP Range request for uploads
  if not request.server.enableUploads:
    request.respond(501, emptyHttpHeaders(), "Uploads not enabled")
    return
  
  try:
    let (rangeStart, rangeEnd, total) = parseUploadRange(contentRangeHeader)
    
    withLock request.server.uploadManagerLock:
      let upload = request.server.uploadManager.getUpload(uploadId)
      if upload == nil:
        request.respond(404, emptyHttpHeaders(), "Upload not found")
        return
      
      # Verify client ownership
      let clientUploads = request.server.uploadManager.sessionsByClient.getOrDefault(request.clientId, @[])
      if uploadId notin clientUploads:
        request.respond(403, emptyHttpHeaders(), "Upload not owned by client")
        return
      
      # Ensure upload is ready for writing
      if upload[].status == UploadPending:
        upload[].openForWriting()
      elif upload[].status != UploadInProgress:
        request.respond(410, emptyHttpHeaders(), "Upload no longer active")
        return
      
      # Write range data
      if request.body.len > 0:
        upload[].writeRangeChunk(request.body.toOpenArrayByte(0, request.body.len - 1), rangeStart, rangeEnd)
      
      # Check if upload is complete
      if total > 0 and upload[].bytesReceived >= total:
        upload[].completeUpload()
      
      var headers: HttpHeaders
      headers["Content-Range"] = fmt"bytes {rangeStart}-{rangeEnd}/{total}"
      request.respond(204, headers, "")
      
  except ranges.RangeError as e:
    request.respond(400, emptyHttpHeaders(), "Invalid range: " & e.msg)
  except UploadError as e:
    request.respond(500, emptyHttpHeaders(), "Upload error: " & e.msg)

proc getUploadStats*(server: Server): tuple[total: int, active: int, completed: int, failed: int] {.gcsafe.} =
  ## Get upload statistics
  if not server.enableUploads:
    return (0, 0, 0, 0)
  
  withLock server.uploadManagerLock:
    result = server.uploadManager.getUploadStats()

# TUS helper functions are exported directly from the tus module
