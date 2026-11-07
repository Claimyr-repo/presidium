const http = require('http')

// Longer than every load balancer's 60 s idle limit, so the load balancer always closes an idle connection first.
// With Node's default of 5 s, a process that stood still for 5 s or more woke with its idle-connection timers run out
// and reset every idle connection a request had arrived on, and the load balancer answered those requests 502.
const KEEP_ALIVE_TIMEOUT_MS = 65000

// Above keepAliveTimeout, so that the header timer never ends a connection keepAliveTimeout would keep.
const HEADERS_TIMEOUT_MS = 66000

/**
 * @name HttpServer
 *
 * @synopsis
 * ```coffeescript [specscript]
 * HttpServer(
 *   httpHandler (
 *     request http.IncomingMessage,
 *     response http.ServerResponse,
 *   )=>Promise<>|(),
 * ) -> server HttpServer
 * ```
 *
 * @description
 * Creates an instance of a Node.js `http.Server` ([Node.js docs](https://nodejs.org/api/net.html#net_class_net_server))
 *
 * ```javascript
 * const requestBodyToString = request => new Promise((resolve, reject) => {
 *   let body = []
 *   request.on('data', chunk => {
 *     body.push(chunk)
 *   }).on('end', () => {
 *     body = Buffer.concat(body).toString()
 *     resolve(body)
 *   }).on('error', reject)
 * })
 *
 * HttpServer(async (request, response) => {
 *   request.startTime = Date.now()
 *   switch (request.path) {
 *     case '/':
 *       response.writeHead(200, {
 *         'Content-Type': 'application/json',
 *         'X-Powered-By': 'presidium',
 *       })
 *       response.write(JSON.stringify({ greeting: 'Hello World' }))
 *     case '/echo':
 *       response.writeHead(200, {
 *         'Content-Type': 'text/plain',
 *         'X-Powered-By': 'presidium',
 *       })
 *       response.write(await requestBodyToString(request))
 *     default:
 *       response.writeHead(404, {
 *         'Content-Type': 'text/html',
 *         'X-Powered-By': 'presidium',
 *       })
 *       response.write('<p>Not Found</p>')
 *   }
 *   response.end()
 *   console.log(`${request.method} ${request.path} complete in ${Date.now() - request.startTime}`)
 * }).on('connection', socket => {}).listen(3000)
 * ```
 */
const HttpServer = function (httpHandler) {
  const server = http.createServer(httpHandler)
  server.keepAliveTimeout = KEEP_ALIVE_TIMEOUT_MS
  server.headersTimeout = HEADERS_TIMEOUT_MS

  server.ready = new Promise(resolve => {
    server._listeningResolve = resolve
  })
  const originalListen = server.listen
  server.listen = (...args) => {
    server._listeningResolve()
    return originalListen.apply(server, args)
  }

  return server
}

module.exports = HttpServer
