/* globals describe, it */
import { deepStrictEqual as same } from 'assert'
import { create } from '../src/cid-set.js'
import * as codec from '@ipld/dag-cbor'
import { sha256 as hasher } from 'multiformats/hashes/sha2'
import { CID } from 'multiformats'
import { nocache } from '../src/cache.js'
import { bf, enc32 } from '../src/utils.js'

const MAX_UINT32 = 4294967295
const chunker = bf(3)
const threshold = Math.floor(MAX_UINT32 / 3)

const cid = CID.parse('zdj7Wd8AMwqnhJGQCbFxBVodGSBG84TM7Hs1rcJuQMwTyfEDS')
const baseBytes = cid.bytes.slice()

const mkcid = (sort, num) => {
  const bytes = baseBytes.slice()
  bytes[bytes.byteLength - 5] = sort
  num = enc32(num)
  let offset = bytes.byteLength - 4
  let i = 0
  while (i < 4) {
    bytes[offset+i] = num[i]
    i++
  }
  return CID.decode(bytes)
}

const mkcids = list => {
  let i = 0
  const ret = []
  for (const num of list) {
    const cid = mkcid(i, num)
    ret.push(cid)
    i++
  }
  return ret
}

const cache = nocache

const storage = () => {
  const blocks = {}
  const put = block => {
    blocks[block.cid.toString()] = block
  }
  const get = async cid => {
    const block = blocks[cid.toString()]
    if (!block) throw new Error('Not found')
    return block
  }
  return { get, put, blocks }
}

const opts = { cache, chunker, codec, hasher }

const verify = (check, node) => {
  same(check.isLeaf, node.isLeaf)
  same(check.isBranch, node.isBranch)
  same(check.entries, node.entryList.entries.length)
  same(check.closed, node.closed)
}

describe('cid set', () => {
  it('basic create', async () => {
    const { get, put } = storage()
    const list = mkcids([ threshold + 1, threshold - 2, threshold + 2 ])
    const checks = [
      { isLeaf: true, entries: 2, closed: true },
      { isLeaf: true, entries: 1, closed: false },
      { isBranch: true, entries: 2, closed: false }
    ]
    let root
    for await (const node of create({ get, list, ...opts })) {
      const address = await node.address
      same(address.asCID, address)
      verify(checks.shift(), node)
      await put(await node.block)
      root = node
    }
    root = await root.getNode(await root.address)
    for (const cid of list) {
      same(await root.get(cid), cid)
    }
  })
})
