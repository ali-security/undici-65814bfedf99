'use strict'

const assert = require('node:assert')
const { once } = require('node:events')
const { createServer } = require('node:net')
const { test } = require('node:test')
const { Client, errors } = require('..')

function readBody (body) {
  return new Promise((resolve, reject) => {
    let data = ''
    body.setEncoding('latin1')
    body.on('data', chunk => { data += chunk })
    body.on('end', () => resolve(data))
    body.on('error', reject)
  })
}

test('should not reuse an idle socket with buffered unsolicited response bytes', async (t) => {
  let responses = 0

  const server = createServer((socket) => {
    socket.on('data', () => {
      if (responses++ === 0) {
        socket.write(
          'HTTP/1.1 200 OK\r\n' +
          'Connection: keep-alive\r\n' +
          'Keep-Alive: timeout=300\r\n' +
          'Content-Length: 9\r\n' +
          '\r\n' +
          '/request1' +
          'HTTP/1.1 200 OK\r\n' +
          'Poison-Free-Socket: true\r\n' +
          'Connection: keep-alive\r\n' +
          'Keep-Alive: timeout=300\r\n' +
          'Content-Length: 0\r\n' +
          '\r\n'
        )
      } else {
        socket.end(
          'HTTP/1.1 200 OK\r\n' +
          'Connection: close\r\n' +
          'Content-Length: 9\r\n' +
          '\r\n' +
          '/request2'
        )
      }
    })
  })
  t.after(() => server.close())

  server.listen(0, '127.0.0.1')
  await once(server, 'listening')

  const client = new Client(`http://127.0.0.1:${server.address().port}`, {
    keepAliveTimeout: 300e3
  })
  t.after(() => client.close())

  // The unsolicited response must cause the poisoned socket to be discarded
  // before request 2 is dispatched.
  const disconnected = once(client, 'disconnect')

  const response1 = await client.request({ path: '/request1', method: 'GET' })
  assert.strictEqual(await readBody(response1.body), '/request1')

  await disconnected

  const response2 = await client.request({ path: '/request2', method: 'GET' })
  assert.strictEqual(response2.headers['poison-free-socket'], undefined)
  assert.strictEqual(await readBody(response2.body), '/request2')
})

// Synchronously blocks the event loop so that bytes already written by the
// peer are delivered to the client socket's kernel buffer before the client
// gets a chance to read them.
function blockEventLoop (ms) {
  Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, ms)
}

const RESPONSE_1 =
  'HTTP/1.1 200 OK\r\n' +
  'Connection: keep-alive\r\n' +
  'Keep-Alive: timeout=300\r\n' +
  'Content-Length: 9\r\n' +
  '\r\n' +
  '/request1'

const POISON =
  'HTTP/1.1 200 OK\r\n' +
  'Poison-Free-Socket: true\r\n' +
  'Connection: keep-alive\r\n' +
  'Keep-Alive: timeout=300\r\n' +
  'Content-Length: 0\r\n' +
  '\r\n'

const RESPONSE_2 =
  'HTTP/1.1 200 OK\r\n' +
  'Connection: close\r\n' +
  'Content-Length: 9\r\n' +
  '\r\n' +
  '/request2'

test('should not attribute an unsolicited response to a queued request', async (t) => {
  let connections = 0

  const server = createServer((socket) => {
    const connection = ++connections
    socket.on('error', () => {})
    socket.once('data', () => {
      if (connection === 1) {
        // Response 1 immediately followed by an unsolicited response, while
        // request 2 is still queued on the client and has not been written.
        socket.write(RESPONSE_1 + POISON)
      } else {
        socket.end(RESPONSE_2)
      }
    })
  })
  t.after(() => server.close())

  server.listen(0, '127.0.0.1')
  await once(server, 'listening')

  const client = new Client(`http://127.0.0.1:${server.address().port}`, {
    pipelining: 1,
    keepAliveTimeout: 300e3
  })
  t.after(() => client.close())

  const disconnected = once(client, 'disconnect')

  const request1 = client.request({ path: '/request1', method: 'GET' })
  const request2 = client.request({ path: '/request2', method: 'GET' })

  const response1 = await request1
  assert.strictEqual(response1.statusCode, 200)

  const [, , err] = await disconnected
  assert.ok(err instanceof errors.SocketError, `expected SocketError, got ${err && err.name}: ${err && err.message}`)
  assert.strictEqual(err.message, 'bad response')

  const response2 = await request2
  assert.strictEqual(response2.headers['poison-free-socket'], undefined)
  assert.strictEqual(await readBody(response2.body), '/request2')
  assert.strictEqual(await readBody(response1.body), '/request1')
  assert.strictEqual(connections, 2)
})

test('should read pending unsolicited bytes before writing a request on an idle keep-alive socket', async (t) => {
  let connections = 0
  let firstSocket = null

  const server = createServer((socket) => {
    const connection = ++connections
    let requests = 0
    socket.on('error', () => {})
    if (connection === 1) {
      firstSocket = socket
    }
    socket.on('data', () => {
      if (connection === 1 && requests++ === 0) {
        socket.write(RESPONSE_1)
      } else {
        socket.end(RESPONSE_2)
      }
    })
  })
  t.after(() => server.close())

  server.listen(0, '127.0.0.1')
  await once(server, 'listening')

  const client = new Client(`http://127.0.0.1:${server.address().port}`, {
    pipelining: 1,
    keepAliveTimeout: 300e3
  })
  t.after(() => client.close())

  const response1 = await client.request({ path: '/request1', method: 'GET' })
  assert.strictEqual(await readBody(response1.body), '/request1')

  const disconnected = once(client, 'disconnect')

  // Let the keep-alive socket go idle, then have the server push an
  // unsolicited response that is already sitting in the client's socket
  // buffer (but not yet read) at the moment request 2 is dispatched.
  const request2 = new Promise((resolve, reject) => {
    setTimeout(() => {
      firstSocket.write(POISON)
      blockEventLoop(100)
      client.request({ path: '/request2', method: 'GET' }).then(resolve, reject)
    }, 50)
  })

  const response2 = await request2
  assert.strictEqual(response2.headers['poison-free-socket'], undefined)
  assert.strictEqual(await readBody(response2.body), '/request2')

  const [, , err] = await disconnected
  assert.ok(err instanceof errors.SocketError, `expected SocketError, got ${err && err.name}: ${err && err.message}`)
  assert.strictEqual(err.message, 'bad response')
  assert.strictEqual(connections, 2)
})
