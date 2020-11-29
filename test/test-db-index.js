/* globals describe, it */
import { deepStrictEqual as same } from 'assert'
import { create } from '../src/db-index.js'
import * as codec from '@ipld/dag-cbor'
import { sha256 as hasher } from 'multiformats/hashes/sha2'
import { nocache } from '../src/cache.js'
import { bf, simpleCompare as compare } from '../src/utils.js'
import { CID } from 'multiformats'

const chunker = bf(3)

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

const createList = entries => entries.map(([key, value]) => ({ key, value }))
const cid = CID.parse('zdj7Wd8AMwqnhJGQCbFxBVodGSBG84TM7Hs1rcJuQMwTyfEDS')

const list = createList([
  [['a', 0], cid],
  [['b', 1], cid],
  [['b', 2], cid],
  [['c', 3], cid],
  [['c', 4], cid],
  [['d', 5], cid],
  [['f', 6], cid],
  [['h', 7], cid],
  [['zz', 9], cid]
])

describe('map', () => {
  it('basic create', async () => {
    const { get, put } = storage()
    const checks = [
      [true, undefined, 1, true],
      [true, undefined, 1, true],
      [true, undefined, 3, true],
      [true, undefined, 3, true],
      [true, undefined, 1, false],
      [undefined, true, 1, true],
      [undefined, true, 2, true],
      [undefined, true, 2, true],
      [undefined, true, 1, true],
      [undefined, true, 2, false],
      [undefined, true, 1, true],
      [undefined, true, 1, true],
      [undefined, true, 2, false]
    ].map(([isLeaf, isBranch, entries, closed]) => ({ isLeaf, isBranch, entries, closed }))
    let root
    for await (const node of create({ get, compare, list, ...opts })) {
      const address = await node.address
      same(address.asCID, address)
      verify(checks.shift(), node)
      await put(await node.block)
      root = node
    }
    root = await root.getNode(await root.address)
    for (const { key } of list) {
      const expected = list.map(entry => {
        if (entry.key[0] !== key[0]) return null
        return { rowid: entry.key[1], row: entry.value }
      }).filter(x => x)
      const result = await root.get(key[0])
      same(result, expected)
    }
  })
  /*
  it('getEntries & getMany', async () => {
    const { get, put } = storage()
    let root
    for await (const node of create({ get, compare, list, ...opts })) {
      const address = await node.address
      await put(await node.block)
      root = node
    }
    const entries = await root.getEntries(['a', 'zz'])
    same(entries.length, 2)
    const [ a, zz ] = entries
    same(a.key, 'a')
    same(a.value, 1)
    same(zz.key, 'zz')
    same(zz.value, 2)
    const values = await root.getMany(['a', 'zz'])
    same(values, [1, 2])
  })
  it('getRangeEntries', async () => {
    const { get, put } = storage()
    let root
    for await (const node of create({ get, compare, list, ...opts })) {
      const address = await node.address
      await put(await node.block)
      root = node
    }
    const verify = (entries, start, end) => {
      const keys = entries.map(entry => entry.key)
      const comp = list.slice(start, end).map(({ key }) => key)
      same(keys, comp)
    }
    let entries = await root.getRangeEntries('b', 'z')
    verify(entries, 1, 9)
    entries = await root.getRangeEntries('', 'zzz')
    verify(entries)
    entries = await root.getRangeEntries('a', 'zz')
    verify(entries)
    entries = await root.getRangeEntries('a', 'c')
    verify(entries, 0, 4)
  })
  it('getAllEntries', async () => {
    const { get, put } = storage()
    let root
    for await (const node of create({ get, compare, list, ...opts })) {
      const address = await node.address
      await put(await node.block)
      root = node
    }
    const verify = (entries, start, end) => {
      const keys = entries.map(entry => entry.key)
      const comp = list.slice(start, end).map(({ key }) => key)
      same(keys, comp)
    }
    let entries = await root.getAllEntries()
    verify(entries)
  })
  */
})
