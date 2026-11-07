const Test = require('thunk-test')
const assert = require('assert')
const childProcess = require('child_process')
const net = require('net')
const path = require('path')
const HttpServer = require('./HttpServer')
const WebSocket = require('./WebSocket')
const WebSocketServer = require('./WebSocketServer')

// Claimyr's change to HttpServer: an idle connection outlives the load balancer's 60 s, so a server that stands still
// for 5 s or more no longer resets the connections a request reached meanwhile (the load balancer's 502, 2026-11-03).
// The server runs in a child, because the stand-still must stop only the server, as it did in production; a control
// on Node's default keepAliveTimeout shows the test can tell the two apart.

const STALL_MS = 6000

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms))

const request = url => `OPTIONS ${url} HTTP/1.1\r\nHost: localhost\r\nConnection: keep-alive\r\n\r\n`

// startServer(keepAliveTimeout number|null) -> Promise<{ port: number, child: ChildProcess }>
const startServer = keepAliveTimeout => new Promise((resolve, reject) => {
  const child = childProcess.spawn(process.execPath, ['-e', `
    const HttpServer = require(${JSON.stringify(path.join(__dirname, 'HttpServer'))})
    const server = new HttpServer((request, response) => {
      if (request.url == '/stall') {
        const until = Date.now() + ${STALL_MS}
        while (Date.now() < until) {
          // the process does nothing else, as when it stood still in production
        }
      }
      response.end('ok')
    })
    ${keepAliveTimeout == null ? '' : `server.keepAliveTimeout = ${keepAliveTimeout}`}
    server.listen(0, '127.0.0.1', () => console.log(server.address().port))
  `], { stdio: ['ignore', 'pipe', 'inherit'] })
  child.on('error', reject)
  child.stdout.once('data', data => resolve({ port: Number(String(data)), child }))
})

// connect(port number) -> socket with `answers` counting responses and `ended` its end, if any
const connect = port => {
  const socket = net.connect(port, '127.0.0.1')
  socket.answers = 0
  socket.ended = null
  socket.on('data', data => {
    socket.answers += String(data).split('HTTP/1.1 ').length - 1
  })
  socket.on('error', error => {
    socket.ended = error.code
  })
  socket.on('close', () => {
    socket.ended ??= 'closed'
  })
  return socket
}

const until = async function (condition) {
  const deadline = Date.now() + STALL_MS + 5000
  while (!condition() && Date.now() < deadline) {
    await sleep(20)
  }
}

// requestDuringStall(keepAliveTimeout number|null) -> Promise<{ answers: number, ended: string|null }>
//   what becomes of a request sent on an idle connection while the server stands still
const requestDuringStall = async function (keepAliveTimeout) {
  const { port, child } = await startServer(keepAliveTimeout)
  try {
    const idle = connect(port)
    const stalled = connect(port)
    idle.write(request('/user/page-view'))
    await until(() => idle.answers == 1)
    assert.strictEqual(idle.answers, 1)
    stalled.write(request('/stall'))
    await sleep(1000)
    idle.write(request('/user/page-view'))
    await until(() => idle.answers == 2 || idle.ended != null)
    const { answers, ended } = idle
    idle.destroy()
    stalled.destroy()
    return { answers, ended }
  } finally {
    child.kill()
  }
}

// webSocketAnswersAfter(idleMs number) -> Promise<string> the echo of a message sent after a WebSocket sat idle: live
//   call audio rides WebSocketServer, which is built on HttpServer
const webSocketAnswersAfter = async function (idleMs) {
  const server = new WebSocketServer(socket => {
    socket.on('message', message => socket.send(String(message)))
  })
  await new Promise(resolve => server.listen(7363, '127.0.0.1', resolve))
  const socket = new WebSocket('ws://127.0.0.1:7363')
  await new Promise((resolve, reject) => socket.once('open', resolve).once('error', reject))
  await sleep(idleMs)
  socket.send('still here')
  const answer = await new Promise((resolve, reject) => {
    socket.once('message', message => resolve(String(message))).once('close', () => reject(new Error('closed')))
  })
  socket.close()
  server.close()
  return answer
}

const test = new Test('ClaimyrHttpServer', async function () {
  const server = new HttpServer(() => {})
  assert.strictEqual(server.keepAliveTimeout, 65000)
  assert.strictEqual(server.headersTimeout, 66000)

  const [withNodeDefault, withHttpServer, webSocketAnswer] = await Promise.all([
    requestDuringStall(5000),
    requestDuringStall(null),
    webSocketAnswersAfter(5500),
  ])
  assert.deepStrictEqual(withNodeDefault, { answers: 1, ended: 'ECONNRESET' })
  assert.deepStrictEqual(withHttpServer, { answers: 2, ended: null })
  assert.strictEqual(webSocketAnswer, 'still here')
}).case()

if (process.argv[1] == __filename) {
  test()
}

module.exports = test
