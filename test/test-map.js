/* globals describe, it */
import { deepStrictEqual as same } from 'assert'
import { create, load } from '../src/map.js'
import * as codec from '@ipld/dag-cbor'
import { sha256 as hasher } from 'multiformats/hashes/sha2'
import { nocache, global as globalCache } from '../src/cache.js'
import { bf, simpleCompare as compare } from '../src/utils.js'

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

const list = createList([
  ['a', 1],
  ['b', 1],
  ['bb', 2],
  ['c', 1],
  ['cc', 2],
  ['d', 1],
  ['ff', 2],
  ['h', 1],
  ['z', 1],
  ['zz', 2]
])

describe('map', () => {
  it('basic create', async () => {
    const { get, put } = storage()
    const checks = [
      [true, undefined, 1, true],
      [true, undefined, 3, true],
      [true, undefined, 1, true],
      [true, undefined, 2, true],
      [true, undefined, 2, true],
      [true, undefined, 1, false],
      [undefined, true, 5, true],
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
    const cid = await root.address
    root = await load({ cid, get, compare, ...opts })
    for (const { key } of list) {
      same(await root.get(key), key.length)
    }
  })
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
    const [a, zz] = entries
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
    verify(entries, 1, 8)
    entries = await root.getRangeEntries('', 'zzz')
    verify(entries)
    entries = await root.getRangeEntries('a', 'zz')
    verify(entries, 0, 9)
    entries = await root.getRangeEntries('a', 'c')
    verify(entries, 0, 3)
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
    const entries = await root.getAllEntries()
    verify(entries)
  })
  it('bulk insert 2', async () => {
    const { get, put } = storage()
    let last
    for await (const node of create({ get, compare, list, ...opts })) {
      const address = await node.address
      await put(await node.block)
      last = node
    }
    const verify = (entries, start, end) => {
      const keys = entries.map(entry => entry.key)
      const comp = list.slice(start, end).map(({ key }) => key)
      same(keys, comp)
    }
    const entries = await last.getAllEntries()
    verify(entries)
    const bulk = [{ key: 'dd', value: 2 }, { key: 'd', value: -1 }]
    const { blocks, root, previous } = await last.bulk(bulk)
    await Promise.all(blocks.map(block => put(block)))
    same(await root.get('dd'), 2)
    same(await root.get('d'), -1)
    const expected = [
      ['a', 1],
      ['b', 1],
      ['bb', 2],
      ['c', 1],
      ['cc', 2],
      ['ff', 2],
      ['h', 1],
      ['z', 1],
      ['zz', 2]
    ]
    for (const [key, value] of expected) {
      same(await root.get(key), value)
    }
  })
  it('bulk insert 100 update 1*100', async () => {
    const { get, put } = storage()
    let last
    const list = []
    let i = 0
    let expected = []
    while (i < 100) {
      list.push({ key: i.toString(), value: true })
      expected.push(i.toString())
      i++
    }
    expected = expected.sort()
    for await (const node of create({ get, compare, list, ...opts, cache: globalCache })) {
      await globalCache.set(node)
      const address = await node.address
      await put(await node.block)
      last = node
    }
    i = -1
    const verify = (entries, start, end) => {
      let count = 0
      const _expected = [...expected]
      for (const { key, value } of entries) {
        same(value, i.toString() !== key)
        same(_expected.shift(), key)
        count++
      }
      same(count, 100)
    }
    const entries = await last.getAllEntries()
    verify(entries)
    const base = last
    i++
    while (i < 100) {
      const bulk = [{ key: i.toString(), value: false }]
      const { blocks, root, previous } = await base.bulk(bulk)
      await Promise.all(blocks.map(block => put(block)))
      verify(await root.getAllEntries())
      i++
    }
  })
  it('bulk insert 100 delete 1*100', async () => {
    const { get, put } = storage()
    let last
    const list = []
    let i = 0
    let expected = []
    while (i < 100) {
      list.push({ key: i.toString(), value: true })
      expected.push(i.toString())
      i++
    }
    expected = expected.sort()
    for await (const node of create({ get, compare, list, ...opts, cache: globalCache })) {
      await globalCache.set(node)
      const address = await node.address
      await put(await node.block)
      last = node
    }
    i = -1
    const verify = (entries, start, end) => {
      let count = 0
      const _expected = [...expected]
      for (const { key, value } of entries) {
        same(value, true)
        let exp = _expected.shift()
        if (exp === i.toString()) exp = _expected.shift()
        same(exp, key)
        count++
      }
      same(count, i === -1 ? 100 : 99)
    }
    const entries = await last.getAllEntries()
    verify(entries)
    const base = last
    i++
    while (i < 100) {
      const bulk = [{ key: i.toString(), del: true }]
      const { blocks, root, previous } = await base.bulk(bulk)
      await Promise.all(blocks.map(block => put(block)))
      verify(await root.getAllEntries())
      i++
    }
  })
  it('brute ranges', async () => {
    const { get, put } = storage()
    let last
    const list = []
    let i = 0
    let expected = []
    while (i < 100) {
      list.push({ key: i.toString(), value: true })
      expected.push(i.toString())
      i++
    }
    expected = expected.sort()
    for await (const node of create({ get, compare, list, ...opts, cache: globalCache })) {
      await globalCache.set(node)
      const address = await node.address
      await put(await node.block)
      last = node
    }
    const front = [...expected]
    const back = [...expected]
    while (front.length) {
      const entries = await last.getRangeEntries(front[0], front[front.length - 1] + '999')
      same(entries.map(({ key }) => key), front)
      front.shift()
    }
    while (front.length) {
      const entries = await last.getRangeEntries(back[0], back[back.length - 1] + '.')
      same(entries.map(({ key }) => key), back)
      back.pop()
    }

    let entries = await last.getRangeEntries('9999999', '9999999999999999')
    same(entries, [])
    entries = await last.getRangeEntries('.', '.')
  })
  it('getEntry', async () => {
    const { get, put } = storage()
    let root
    let leaf
    for await (const node of create({ get, compare, list, ...opts })) {
      const address = await node.address
      if (node.isLeaf) leaf = node
      await put(await node.block)
      root = node
    }
    let threw = true
    try {
      await root.getEntry('.')
      threw = false
    } catch (e) {
      if (e.message !== 'Not found') throw e
    }
    same(threw, true)
    try {
      await leaf.getEntry('.')
      threw = false
    } catch (e) {
      if (e.message !== 'Not found') throw e
    }
    same(threw, true)
  })
  it('leaf', async () => {
    const { get, put } = storage()
    let root
    let leaf
    const chunker = bf(1000)
    for await (const node of create({ get, compare, list, ...opts, chunker })) {
      const address = await node.address
      if (!node.isLeaf) throw new Error('Not leaf')
      await put(await node.block)
      root = node
    }
    let result = await root.get('c')
    same(result, 1)
    result = await root.getMany(['c', 'cc', 'd'])
    same(result, [1, 2, 1])
    const bulk = [{ key: 'aaa', value: 3 }]
    const { blocks, root: rr, previous } = await root.bulk(bulk)
    await Promise.all(blocks.map(block => put(block)))
    same(await rr.get('aaa'), 3)
  })
})
