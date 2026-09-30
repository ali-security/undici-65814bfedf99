'use strict'

const assert = require('node:assert')
const { test, after } = require('node:test')
const { once } = require('node:events')
const { WebSocketServer } = require('ws')
const {
  Agent,
  BalancedPool,
  Client,
  Dispatcher,
  EnvHttpProxyAgent,
  Pool,
  ProxyAgent,
  WebSocket
} = require('../..')
const { ByteParser } = require('../../lib/web/websocket/receiver')
const { states, sentCloseFrameState } = require('../../lib/web/websocket/constants')
const {
  kController,
  kReadyState,
  kResponse,
  kSentClose,
  kWebSocketURL
} = require('../../lib/web/websocket/symbols')
const diagnosticsChannel = require('node:diagnostics_channel')

test('Fragmented frame with a ping frame in the middle of it', () => {
  const server = new WebSocketServer({ port: 0 })

  server.on('connection', (ws) => {
    const socket = ws._socket

    socket.write(Buffer.from([0x01, 0x03, 0x48, 0x65, 0x6c])) // Text frame "Hel"
    socket.write(Buffer.from([0x89, 0x05, 0x48, 0x65, 0x6c, 0x6c, 0x6f])) // ping "Hello"
    socket.write(Buffer.from([0x80, 0x02, 0x6c, 0x6f])) // Text frame "lo"
  })

  after(() => {
    for (const client of server.clients) {
      client.close()
    }

    server.close()
  })

  const ws = new WebSocket(`ws://localhost:${server.address().port}`)

  diagnosticsChannel.channel('undici:websocket:ping').subscribe(
    ({ payload }) => assert.deepStrictEqual(payload, Buffer.from('Hello'))
  )

  return new Promise((resolve) => {
    ws.addEventListener('message', ({ data }) => {
      assert.strictEqual(data, 'Hello')

      ws.close()
      resolve()
    })
  })
})

// The default maximum number of fragments per message (see DispatcherBase)
const kDefaultMaxFragments = 131072

// Without a fragment limit these connections are never failed, so bound each
// test instead of hanging until the whole file times out.
const timeout = 5000

function noop () {}

function closeServer (server) {
  for (const client of server.clients) {
    client.terminate()
  }

  return new Promise((resolve) => server.close(() => resolve()))
}

test('Too many fragments (uncompressed)', { timeout }, async (t) => {
  const agent = new Agent({
    webSocket: {
      maxFragments: 3
    }
  })

  const server = new WebSocketServer({ port: 0 })

  t.after(async () => {
    await closeServer(server)
    await agent.close()
  })

  await once(server, 'listening')

  const serverClosed = new Promise((resolve) => {
    server.on('connection', (ws) => {
      ws.on('error', noop)
      ws.on('close', (code, reason) => resolve({ code, reason: reason.toString() }))

      const fragment = Buffer.from('a')
      const options = { fin: false }

      ws.send(fragment, options)
      ws.send(fragment, options)
      ws.send(fragment, options)
      ws.send(fragment, options)
    })
  })

  const client = new WebSocket(`ws://127.0.0.1:${server.address().port}`, {
    dispatcher: agent
  })

  let messageReceived = false
  client.addEventListener('message', () => {
    messageReceived = true
  })

  const errored = once(client, 'error')
  const [closeEvent] = await once(client, 'close')
  const [errorEvent] = await errored
  const { code, reason } = await serverClosed

  assert.strictEqual(errorEvent.message, 'Too many message fragments')
  assert.strictEqual(closeEvent.code, 1006)
  assert.strictEqual(code, 1008)
  assert.strictEqual(reason, 'Too many message fragments')
  assert.strictEqual(messageReceived, false)
})

test('Too many fragments (compressed)', { timeout }, async (t) => {
  const agent = new Agent({
    webSocket: {
      maxFragments: 3
    }
  })

  const server = new WebSocketServer({
    perMessageDeflate: { threshold: 0 },
    port: 0
  })

  t.after(async () => {
    await closeServer(server)
    await agent.close()
  })

  await once(server, 'listening')

  const serverClosed = new Promise((resolve) => {
    server.on('connection', (ws) => {
      ws.on('error', noop)
      ws.on('close', (code, reason) => resolve({ code, reason: reason.toString() }))

      const fragment = Buffer.from('a')
      const options = { fin: false }

      ws.send(fragment, options)
      ws.send(fragment, options)
      ws.send(fragment, options)
      ws.send(fragment, options)
    })
  })

  const client = new WebSocket(`ws://127.0.0.1:${server.address().port}`, {
    dispatcher: agent
  })

  let messageReceived = false
  client.addEventListener('message', () => {
    messageReceived = true
  })

  const errored = once(client, 'error')
  const [closeEvent] = await once(client, 'close')
  const [errorEvent] = await errored
  const { code, reason } = await serverClosed

  assert.strictEqual(errorEvent.message, 'Too many message fragments')
  assert.strictEqual(closeEvent.code, 1006)
  assert.strictEqual(code, 1008)
  assert.strictEqual(reason, 'Too many message fragments')
  assert.strictEqual(messageReceived, false)
})

test('Empty first fragment followed by non-empty continuation delivers the message', { timeout }, async (t) => {
  // RFC 6455 §5.4 allows zero-byte fragments. A conforming server that opens
  // a fragmented message with an empty frame must be honored: the parser must
  // recognize the in-progress fragmented message when the continuation arrives.
  const server = new WebSocketServer({ port: 0 })

  t.after(() => closeServer(server))

  await once(server, 'listening')

  server.on('connection', (ws) => {
    ws.on('error', noop)
    ws.send('', { fin: false })
    ws.send('hello', { fin: true })
  })

  const client = new WebSocket(`ws://127.0.0.1:${server.address().port}`)

  const [{ data }] = await once(client, 'message')
  assert.strictEqual(data, 'hello')

  client.close()
  await once(client, 'close')
})

test('Too many empty fragments triggers close 1008', { timeout }, async (t) => {
  const agent = new Agent({
    webSocket: {
      maxFragments: 3
    }
  })

  const server = new WebSocketServer({ port: 0 })

  t.after(async () => {
    await closeServer(server)
    await agent.close()
  })

  await once(server, 'listening')

  const serverClosed = new Promise((resolve) => {
    server.on('connection', (ws) => {
      ws.on('error', noop)
      ws.on('close', (code, reason) => resolve({ code, reason: reason.toString() }))

      const fragment = ''
      const options = { fin: false }

      ws.send(fragment, options) // Text frame fin=0, len=0
      ws.send(fragment, options) // Continuation fin=0, len=0
      ws.send(fragment, options) // Continuation fin=0, len=0
      ws.send(fragment, options) // Continuation fin=0, len=0
    })
  })

  const client = new WebSocket(`ws://127.0.0.1:${server.address().port}`, {
    dispatcher: agent
  })

  const errored = once(client, 'error')
  const [closeEvent] = await once(client, 'close')
  const [errorEvent] = await errored
  const { code, reason } = await serverClosed

  assert.strictEqual(errorEvent.message, 'Too many message fragments')
  assert.strictEqual(closeEvent.code, 1006)
  assert.strictEqual(code, 1008)
  assert.strictEqual(reason, 'Too many message fragments')
})

/**
 * Writes a flood of empty fragments that exceeds the default fragment limit:
 * an opening text frame (fin=0, len=0) followed by kDefaultMaxFragments empty
 * continuation frames (fin=0, len=0, i.e. the bytes 0x00 0x00).
 * @param {WebSocketServer} server
 */
function floodEmptyFragmentsOnConnection (server) {
  server.on('connection', (ws) => {
    ws.on('error', noop)
    ws._socket.on('error', noop)

    ws._socket.write(Buffer.concat([
      Buffer.from([0x01, 0x00]),
      Buffer.alloc(2 * kDefaultMaxFragments)
    ]))
  })
}

test('Default fragment limit applies with the global dispatcher', { timeout }, async (t) => {
  const server = new WebSocketServer({ port: 0 })

  t.after(() => closeServer(server))

  await once(server, 'listening')

  floodEmptyFragmentsOnConnection(server)

  const client = new WebSocket(`ws://127.0.0.1:${server.address().port}`)

  let messageReceived = false
  client.addEventListener('message', () => {
    messageReceived = true
  })

  const errored = once(client, 'error')
  const [closeEvent] = await once(client, 'close')
  const [errorEvent] = await errored

  assert.strictEqual(errorEvent.message, 'Too many message fragments')
  assert.strictEqual(closeEvent.code, 1006)
  assert.strictEqual(messageReceived, false)
})

test('Default fragment limit applies to dispatchers without webSocketOptions', { timeout }, async (t) => {
  class WrappingDispatcher extends Dispatcher {
    constructor (agent) {
      super()
      this.agent = agent
    }

    dispatch (opts, handler) {
      return this.agent.dispatch(opts, handler)
    }

    close (...args) {
      return this.agent.close(...args)
    }

    destroy (...args) {
      return this.agent.destroy(...args)
    }
  }

  const agent = new Agent()
  const dispatcher = new WrappingDispatcher(agent)
  assert.strictEqual(dispatcher.webSocketOptions, undefined)

  const server = new WebSocketServer({ port: 0 })

  t.after(async () => {
    await closeServer(server)
    await agent.close()
  })

  await once(server, 'listening')

  floodEmptyFragmentsOnConnection(server)

  const client = new WebSocket(`ws://127.0.0.1:${server.address().port}`, {
    dispatcher
  })

  const errored = once(client, 'error')
  const [closeEvent] = await once(client, 'close')
  const [errorEvent] = await errored

  assert.strictEqual(errorEvent.message, 'Too many message fragments')
  assert.strictEqual(closeEvent.code, 1006)
})

test('Dispatchers forward the webSocket options', async (t) => {
  const origin = 'http://127.0.0.1:1'
  const webSocket = { maxFragments: 5 }

  const dispatchers = {
    agent: new Agent({ webSocket }),
    pool: new Pool(origin, { webSocket }),
    balancedPool: new BalancedPool([origin], { webSocket }),
    client: new Client(origin, { webSocket }),
    defaultAgent: new Agent(),
    defaultPool: new Pool(origin),
    defaultClient: new Client(origin),
    disabledAgent: new Agent({ webSocket: { maxFragments: 0 } }),
    proxyAgent: new ProxyAgent(origin),
    envHttpProxyAgent: new EnvHttpProxyAgent()
  }

  t.after(() => Promise.all(Object.values(dispatchers).map((d) => d.close())))

  assert.deepStrictEqual(dispatchers.agent.webSocketOptions, { maxFragments: 5 })
  assert.deepStrictEqual(dispatchers.pool.webSocketOptions, { maxFragments: 5 })
  assert.deepStrictEqual(dispatchers.balancedPool.webSocketOptions, { maxFragments: 5 })
  assert.deepStrictEqual(dispatchers.client.webSocketOptions, { maxFragments: 5 })
  assert.deepStrictEqual(dispatchers.defaultAgent.webSocketOptions, { maxFragments: kDefaultMaxFragments })
  assert.deepStrictEqual(dispatchers.defaultPool.webSocketOptions, { maxFragments: kDefaultMaxFragments })
  assert.deepStrictEqual(dispatchers.defaultClient.webSocketOptions, { maxFragments: kDefaultMaxFragments })
  assert.deepStrictEqual(dispatchers.disabledAgent.webSocketOptions, { maxFragments: 0 })
  assert.deepStrictEqual(dispatchers.proxyAgent.webSocketOptions, { maxFragments: kDefaultMaxFragments })
  assert.deepStrictEqual(dispatchers.envHttpProxyAgent.webSocketOptions, { maxFragments: kDefaultMaxFragments })
})

/**
 * Creates the minimal state of an open WebSocket needed to drive a ByteParser.
 */
function createOpenWebSocket () {
  const ws = new EventTarget()
  const calls = { abort: 0, destroy: 0, written: [], messages: [], errors: [] }

  ws[kReadyState] = states.OPEN
  ws[kSentClose] = sentCloseFrameState.NOT_SENT
  ws[kWebSocketURL] = new URL('ws://localhost')
  ws[kController] = {
    abort: () => {
      calls.abort += 1
    }
  }
  ws[kResponse] = {
    socket: {
      destroyed: false,
      write (data) {
        calls.written.push(data)
      },
      destroy () {
        calls.destroy += 1
        this.destroyed = true
      }
    }
  }

  ws.addEventListener('message', ({ data }) => calls.messages.push(data))
  ws.addEventListener('error', ({ message }) => calls.errors.push(message))

  return { ws, calls }
}

/**
 * Decodes a masked close frame written by the client.
 * @param {Buffer} frame
 */
function decodeCloseFrame (frame) {
  assert.strictEqual(frame[0], 0x88)
  const length = frame[1] & 0x7F
  const mask = frame.subarray(2, 6)
  const payload = Buffer.alloc(length)
  for (let i = 0; i < length; i++) {
    payload[i] = frame[6 + i] ^ mask[i & 3]
  }
  return { code: payload.readUInt16BE(0), reason: payload.subarray(2).toString() }
}

function writeFrames (options, frames) {
  const { ws, calls } = createOpenWebSocket()
  const parser = new ByteParser(ws, null, options)

  parser.write(Buffer.concat(frames.map((frame) => Buffer.from(frame))))

  return new Promise((resolve) => {
    setImmediate(() => {
      parser.destroy()
      resolve(calls)
    })
  })
}

function assertFailedWithTooManyFragments (calls) {
  assert.strictEqual(calls.abort, 1)
  assert.strictEqual(calls.destroy, 1)
  assert.deepStrictEqual(calls.messages, [])
  assert.deepStrictEqual(calls.errors, ['Too many message fragments'])
  assert.strictEqual(calls.written.length, 1)
  assert.deepStrictEqual(decodeCloseFrame(calls.written[0]), {
    code: 1008,
    reason: 'Too many message fragments'
  })
}

test('ByteParser fails the connection when a message exceeds maxFragments', async () => {
  const calls = await writeFrames({ maxFragments: 3 }, [
    [0x01, 0x01, 0x61], // Text frame fin=0 "a"
    [0x00, 0x01, 0x61], // Continuation fin=0 "a"
    [0x00, 0x01, 0x61], // Continuation fin=0 "a"
    [0x00, 0x01, 0x61], // Continuation fin=0 "a"
    [0x80, 0x01, 0x61] // Continuation fin=1 "a"
  ])

  assertFailedWithTooManyFragments(calls)
})

test('ByteParser counts zero-length fragments towards maxFragments', async () => {
  const calls = await writeFrames({ maxFragments: 3 }, [
    [0x01, 0x00], // Text frame fin=0, len=0
    [0x00, 0x00], // Continuation fin=0, len=0
    [0x00, 0x00], // Continuation fin=0, len=0
    [0x00, 0x00], // Continuation fin=0, len=0
    [0x80, 0x00] // Continuation fin=1, len=0
  ])

  assertFailedWithTooManyFragments(calls)
})

test('ByteParser delivers a message with exactly maxFragments fragments', async () => {
  const calls = await writeFrames({ maxFragments: 3 }, [
    [0x01, 0x01, 0x61], // Text frame fin=0 "a"
    [0x00, 0x01, 0x62], // Continuation fin=0 "b"
    [0x80, 0x01, 0x63], // Continuation fin=1 "c"
    [0x01, 0x00], // Text frame fin=0, len=0
    [0x00, 0x02, 0x64, 0x65], // Continuation fin=0 "de"
    [0x80, 0x00] // Continuation fin=1, len=0
  ])

  assert.strictEqual(calls.abort, 0)
  assert.deepStrictEqual(calls.errors, [])
  assert.deepStrictEqual(calls.messages, ['abc', 'de'])
})

test('ByteParser accepts an empty first fragment followed by a continuation', async () => {
  const calls = await writeFrames({ maxFragments: 3 }, [
    [0x01, 0x00], // Text frame fin=0, len=0
    [0x80, 0x05, 0x68, 0x65, 0x6c, 0x6c, 0x6f] // Continuation fin=1 "hello"
  ])

  assert.strictEqual(calls.abort, 0)
  assert.deepStrictEqual(calls.errors, [])
  assert.deepStrictEqual(calls.messages, ['hello'])
})

test('ByteParser does not limit fragments when maxFragments is 0', async () => {
  const frames = [[0x01, 0x01, 0x61]]
  for (let i = 0; i < 9; i++) {
    frames.push([0x00, 0x01, 0x61])
  }
  frames.push([0x80, 0x00])

  const calls = await writeFrames({ maxFragments: 0 }, frames)

  assert.strictEqual(calls.abort, 0)
  assert.deepStrictEqual(calls.errors, [])
  assert.deepStrictEqual(calls.messages, ['a'.repeat(10)])
})
