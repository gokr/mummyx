import httpclient, mummy, zippy

proc handler(request: Request) =
  echo request
  doAssert request.uri == request.path
  case request.uri:
  of "/":
    if request.httpMethod == "GET":
      var headers: mummy.HttpHeaders
      headers["Content-Type"] = "text/plain"
      request.respond(200, headers, "Hello, World!")
    else:
      request.respond(405)
  of "/compressed":
    if request.httpMethod == "GET":
      var headers: mummy.HttpHeaders
      headers["Content-Type"] = "text/plain"
      var body: string
      for i in 0 ..< 100:
        body &= "abcdefghijklmnopqrstuvwxyz"
      request.respond(200, headers, body)
    else:
      request.respond(405)
  of "/raise":
    if request.httpMethod == "GET":
      raise newException(ValueError, "Expected /raise exception")
    else:
      request.respond(405)
  else:
    request.respond(404)
  doAssert request.responded == true

let server = newServer(handler)

var serverThread: Thread[void]
proc serverProc() {.gcsafe.} =
  {.cast(gcsafe).}:
    server.serve(Port(8081))

createThread(serverThread, serverProc)
server.waitUntilReady()

# Run tests
block:
  let client = newHttpClient()
  doAssert client.getContent("http://localhost:8081/") == "Hello, World!"

block:
  let client = newHttpClient()
  doAssert client.post("http://localhost:8081/", "").status == "405"

block:
  let client = newHttpClient()
  client.headers = newHttpHeaders({"Accept-Encoding": "gzip"})
  let response = client.request("http://localhost:8081/compressed")
  doAssert response.headers["Content-Encoding"] == "gzip"
  discard uncompress(response.body, dfGzip)

block:
  let client = newHttpClient()
  client.headers = newHttpHeaders({"Accept-Encoding": "deflate"})
  let response = client.request("http://localhost:8081/compressed")
  doAssert response.headers["Content-Encoding"] == "deflate"
  discard uncompress(response.body, dfDeflate)

block:
  let client = newHttpClient()
  doAssert client.get("http://localhost:8081/raise").status == "500"

echo "Done, all HTTP tests passed"
# Note: Avoiding server.close() due to segfault in cleanup
