/* globals describe, it */
import { deepStrictEqual as same } from 'assert'
import { create, load } from '../src/db-index.js'
import * as codec from '@ipld/dag-cbor'
import { sha256 as hasher } from 'multiformats/hashes/sha2'
import { nocache } from '../src/cache.js'
import { bf } from '../src/utils.js'
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

const stringDocIdList = createList([
  [['a', 't0'], cid],
  [['b', 't1'], cid],
  [['b', 't2'], cid],
  [['c', 't3'], cid],
  [['c', 't4'], cid],
  [['d', 't5'], cid],
  [['f', 't6'], cid],
  [['h', 't7'], cid],
  [['zz', 't9'], cid]
])

describe('db index', () => {
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
    let leaf
    for await (const node of create({ get, list, ...opts })) {
      if (node.isLeaf) leaf = node
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
        return { id: entry.key[1], row: entry.value }
      }).filter(x => x)
      const { result } = await root.get(key[0])
      same(result, expected)
    }
    const cid = await leaf.address
    root = await load({ cid, get, ...opts })
    let { result: [result] } = await leaf.get('zz')
    same(result.id, 9)
    const { result: results } = await leaf.range('z', 'zzzzz')
    same(results.length, 1)
    result = results[0]
    same(result.id, 'zz')
    same(result.key, 9)
  })
  it('string create', async () => {
    const { get, put } = storage()
    const checks = [
      [true, undefined, 1, true],
      [true, undefined, 2, true],
      [true, undefined, 2, true],
      [true, undefined, 4, true],
      [undefined, true, 3, true],
      [undefined, true, 1, true],
      [undefined, true, 1, true],
      [undefined, true, 1, true],
      [undefined, true, 2, false]
    ].map(([isLeaf, isBranch, entries, closed]) => ({ isLeaf, isBranch, entries, closed }))
    let root
    let leaf
    for await (const node of create({ get, list: stringDocIdList, ...opts })) {
      if (node.isLeaf) leaf = node
      const address = await node.address
      same(address.asCID, address)
      verify(checks.shift(), node)
      await put(await node.block)
      root = node
    }
    root = await root.getNode(await root.address)
    for (const { key } of stringDocIdList) {
      const expected = stringDocIdList.map(entry => {
        if (entry.key[0] !== key[0]) return null
        return { id: entry.key[1], row: entry.value }
      }).filter(x => x)
      const { result } = await root.get(key[0])
      same(result.id, expected.id)
      same(result, expected)
    }
    const cid = await leaf.address
    root = await load({ cid, get, ...opts })
    let { result: [result] } = await leaf.get('zz')
    same(result.id, 't9')
    const { result: results } = await leaf.range('z', 'zzzzz')
    same(results.length, 1)
    result = results[0]
    same(result.id, 'zz')
    same(result.key, 't9')
  })
  it('range', async () => {
    const { get, put } = storage()
    let root
    for await (const node of create({ get, list, ...opts })) {
      await put(await node.block)
      root = node
    }
    const verify = (entries, start, end) => {
      const comp = list.slice(start, end).map(entry => {
        const [id, key] = entry.key
        return { id, key, row: entry.value }
      })
      same(entries, comp)
    }
    const range = async (...args) => (await root.range(...args)).result
    let entries = await range('b', 'z')
    verify(entries, 1, 8)
    entries = await range('', 'zzz')
    verify(entries)
    entries = await range('a', 'zz')
    verify(entries)
    entries = await range('a', 'c')
    verify(entries, 0, 5)
  })
  it('range errors', async () => {
    const { get, put } = storage()
    const badDocIdList = createList([
      [['a', 't0'], cid],
      [['b', NaN], cid],
      [['b', 't2'], cid],
      [['c', 't3'], cid],
      [['c', 't4'], cid],
      [['d', 't5'], cid],
      [['f', 't6'], cid],
      [['h', 't7'], cid],
      [['zz', 't9'], cid]
    ])

    try {
      for await (const node of create({ get, list: badDocIdList, ...opts })) {
        await put(await node.block)
      }
    } catch (err) {
      same(err.message, 'ref may not be Infinity or NaN')
    }
  })
  it('getAllEntries', async () => {
    const { get, put } = storage()
    let root
    for await (const node of create({ get, list, ...opts })) {
      await put(await node.block)
      root = node
    }
    const verify = (entries, start, end) => {
      const keys = entries.map(entry => entry.key)
      const comp = list.slice(start, end).map(({ key }) => key)
      same(keys, comp)
    }
    const { result: entries } = await root.getAllEntries()
    verify(entries)
  })
  it('bulk', async () => {
    const { get, put } = storage()
    let base
    let leaf
    for await (const node of create({ get, list, ...opts })) {
      if (node.isLeaf) leaf = node
      await put(await node.block)
      base = node
    }
    const value = cid
    let bulk = [{ key: ['a', 40], value }, { key: ['z', 41], value }, { key: ['b', 2], del: true }]
    const { root, blocks } = await base.bulk(bulk)
    await Promise.all(blocks.map(b => put(b)))
    const ids = results => results.map(({ id }) => id)
    const getval = async k => (await root.get(k)).result
    same(ids(await getval('a')), [0, 40])
    same(ids(await getval('b')), [1])
    same(ids(await getval('z')), [41])

    const getrange = async (start, end) => (await root.range(start, end)).result
    const gotrange = await getrange('a', 'z')
    console.log('gotrange', gotrange)
    const { id, key } = gotrange[0]
    same({ id, key }, 'ok')

    bulk = [{ key: ['zz', 42], value }, { key: ['zz', 9], del: true }]
    const { root: newRoot, blocks: newBlocks } = await leaf.bulk(bulk)
    await Promise.all(newBlocks.map(b => put(b)))
    same(ids((await newRoot.get('zz')).result), [42])
  })
})
