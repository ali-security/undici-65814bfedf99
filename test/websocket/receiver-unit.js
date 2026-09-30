'use strict'

const { test } = require('node:test')
const assert = require('node:assert')
const { ByteParser } = require('../../lib/web/websocket/receiver')
const { states } = require('../../lib/web/websocket/constants')
const { kController, kResponse, kReadyState } = require('../../lib/web/websocket/symbols')

const invalidFrame = Buffer.from([0x82, 0x7F, 0x00, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01])

test('ByteParser rejects 64-bit payload lengths with a non-zero upper word', (t) => {
  const calls = {
    abort: 0,
    destroy: 0
  }

  const ws = new EventTarget()
  ws[kController] = {
    abort: () => {
      calls.abort += 1
    }
  }
  ws[kResponse] = {
    socket: {
      destroyed: false,
      destroy: () => {
        calls.destroy += 1
      }
    }
  }

  const parser = new ByteParser(ws)

  parser.write(invalidFrame)

  return new Promise((resolve) => {
    setImmediate(() => {
      assert.strictEqual(calls.abort, 1)
      assert.strictEqual(calls.destroy, 1)
      parser.destroy()
      resolve()
    })
  })
})

function createHandler (calls) {
  return {
    [kReadyState]: states.CONNECTING,
    [kController]: {
      abort: () => {
        calls.abort += 1
      }
    },
    [kResponse]: null,
    dispatchEvent: () => {},
    onSocketClose: () => {},
    closeState: new Set()
  }
}

function parseHeader (header) {
  const calls = { abort: 0 }
  const parser = new ByteParser(createHandler(calls))

  parser.write(header)

  return new Promise((resolve) => {
    setImmediate(() => {
      parser.destroy()
      resolve(calls)
    })
  })
}

test('ByteParser rejects a 64-bit payload length whose upper word would be mis-shifted into a small length', async () => {
  // upper = 1, lower = 0 (2^32 bytes) was previously computed as (1 << 8) + 0 = 256
  const calls = await parseHeader(Buffer.from([0x82, 0x7F, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00]))
  assert.strictEqual(calls.abort, 1)
})

test('ByteParser rejects a 64-bit payload length with a zero upper word and lower word > 2^31-1', async () => {
  const calls = await parseHeader(Buffer.from([0x82, 0x7F, 0x00, 0x00, 0x00, 0x00, 0x80, 0x00, 0x00, 0x00]))
  assert.strictEqual(calls.abort, 1)
})

test('ByteParser accepts a valid 64-bit payload length', async () => {
  // upper = 0, lower = 65536
  const calls = await parseHeader(Buffer.from([0x82, 0x7F, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00]))
  assert.strictEqual(calls.abort, 0)
})
