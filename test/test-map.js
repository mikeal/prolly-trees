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
      same((await root.get(key)).result, key.length)
    }
  })
  it('getEntries & getMany', async () => {
    const { get, put } = storage()
    let root
    for await (const node of create({ get, compare, list, ...opts })) {
      await put(await node.block)
      root = node
    }
    const { result: entries, cids } = await root.getEntries(['a', 'zz'])
    same((await cids.all()).size, 5)
    same(entries.length, 2)
    const [a, zz] = entries
    same(a.key, 'a')
    same(a.value, 1)
    same(zz.key, 'zz')
    same(zz.value, 2)
    const { result: values } = await root.getMany(['a', 'zz'])
    same(values, [1, 2])
  })
  it('getRangeEntries', async () => {
    const { get, put } = storage()
    let root
    for await (const node of create({ get, compare, list, ...opts })) {
      await put(await node.block)
      root = node
    }
    const verify = (entries, start, end) => {
      const keys = entries.map(entry => entry.key)
      const comp = list.slice(start, end).map(({ key }) => key)
      same(keys, comp)
    }
    const range = async (...args) => (await root.getRangeEntries(...args)).result
    let entries = await range('b', 'z')
    verify(entries, 1, 8)
    entries = await range('', 'zzz')
    verify(entries)
    entries = await range('a', 'zz')
    verify(entries, 0, 9)
    entries = await range('a', 'c')
    verify(entries, 0, 3)
  })
  let getCount = 0
  const createFaultyStorage = (faultCount) => {
    const { get, put } = storage()
    const faultyGet = async (cid) => {
      getCount++
      if (getCount === faultCount) {
        throw new Error('Faulty CID encountered')
      }
      return await get(cid)
    }
    return { get: faultyGet, put }
  }
  it('getAllEntries', async () => {
    const { get, put } = createFaultyStorage(8)

    let root
    for await (const node of create({ get, compare, list, ...opts })) {
      await put(await node.block)
      root = node
    }

    try {
      await root.getAllEntries()
      throw new Error('getAllEntries should have thrown an error')
    } catch (err) {
      same(err.message, 'Faulty CID encountered', 'Unexpected error thrown')
    }
  })
  it('bulk insert 2', async () => {
    const { get, put } = storage()
    let last
    for await (const node of create({ get, compare, list, ...opts })) {
      await put(await node.block)
      last = node
    }
    const verify = (entries, start, end) => {
      const keys = entries.map(entry => entry.key)
      const comp = list.slice(start, end).map(({ key }) => key)
      same(keys, comp)
    }
    const { result: entries } = await last.getAllEntries()
    verify(entries)
    const bulk = [
      { key: 'dd', value: 2 },
      { key: 'd', value: -1 }
    ]
    const { blocks, root } = await last.bulk(bulk)
    await Promise.all(blocks.map(block => put(block)))
    const _get = async (k) => (await root.get(k)).result
    same(await _get('dd'), 2)
    same(await _get('d'), -1)
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
      same(await _get(key), value)
    }
  })
  it('bulk insert unsorted', async () => {
    const { get, put } = storage()
    let last
    for await (const node of create({ get, compare, list, ...opts })) {
      await put(await node.block)
      last = node
    }
    const verify = (entries, start, end) => {
      const keys = entries.map(entry => entry.key)
      const comp = list.slice(start, end).map(({ key }) => key)
      same(keys, comp)
    }
    const { result: entries } = await last.getAllEntries()
    verify(entries)

    // Unsorted bulk array
    const bulk = [
      { key: 'dd', value: 2 },
      { key: 'd', value: -1 }
    ].sort(() => Math.random() - 0.5)

    const { blocks, root } = await last.bulk(bulk, { sorted: false }) // Do not set the sorted option
    await Promise.all(blocks.map(block => put(block)))
    const _get = async (k) => (await root.get(k)).result
    same(await _get('dd'), 2)
    same(await _get('d'), -1)
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
      same(await _get(key), value)
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
    const { result: entries } = await last.getAllEntries()
    verify(entries)
    const base = last
    i++
    while (i < 100) {
      const bulk = [{ key: i.toString(), value: false }]
      const { blocks, root } = await base.bulk(bulk)
      await Promise.all(blocks.map(block => put(block)))
      const { result } = await root.getAllEntries()
      verify(result)
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
    const { result: entries } = await last.getAllEntries()
    verify(entries)
    const base = last
    i++
    while (i < 100) {
      const bulk = [{ key: i.toString(), del: true }]
      const { blocks, root, previous } = await base.bulk(bulk)
      same(previous.length, 1)
      const [{ key, value }] = previous
      same(key, i.toString())
      same(value, true)
      await Promise.all(blocks.map(block => put(block)))
      const { result } = await root.getAllEntries()
      verify(result)
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
      await put(await node.block)
      last = node
    }
    const front = [...expected]
    const back = [...expected]
    while (front.length) {
      const { result: entries } = await last.getRangeEntries(front[0], front[front.length - 1] + '999')
      same(entries.map(({ key }) => key), front)
      front.shift()
    }
    while (front.length) {
      const { result: entries } = await last.getRangeEntries(back[0], back[back.length - 1] + '.')
      same(entries.map(({ key }) => key), back)
      back.pop()
    }

    let { result: entries } = await last.getRangeEntries('9999999', '9999999999999999')
    same(entries, [])
    entries = (await last.getRangeEntries('.', '.')).result
  })
  it('getEntry', async () => {
    const { get, put } = storage()
    let root
    let leaf
    for await (const node of create({ get, compare, list, ...opts })) {
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
    try {
      await root.getEntry('missing')
      threw = false
    } catch (e) {
      if (e.message !== 'Not found') throw e
    }
    same(threw, true)
  })
  it('leaf', async () => {
    const { get, put } = storage()
    let root
    const chunker = bf(1000)
    for await (const node of create({ get, compare, list, ...opts, chunker })) {
      if (!node.isLeaf) throw new Error('Not leaf')
      await put(await node.block)
      root = node
    }
    const res = await root.get('c')
    same(res.result, 1)
    const { result } = await root.getMany(['c', 'cc', 'd'])
    same(result, [1, 2, 1])
    const bulk = [{ key: 'aaa', value: 3 }]
    const { blocks, root: rr } = await root.bulk(bulk)
    await Promise.all(blocks.map(block => put(block)))
    same((await rr.get('aaa')).result, 3)
  })
  it('load non-existent key', async () => {
    const { get, put } = storage()
    let root
    for await (const node of create({ get, compare, list, ...opts })) {
      await put(await node.block)
      root = node
    }

    try {
      await root.get('non_existent_key')
      throw new Error('Should not reach this line')
    } catch (error) {
      same(error.message, 'Not found')
    }
  })
  it('delete multiple entries', async () => {
    const { get, put } = storage()
    let root

    for await (const node of create({ get, compare, list, ...opts })) {
      await put(await node.block)
      root = node
    }

    const keysToDelete = ['b', 'cc', 'z']
    const bulk = keysToDelete.map(key => ({ key, del: true }))
    const { blocks, root: updatedRoot, previous } = await root.bulk(bulk)
    await Promise.all(blocks.map(block => put(block)))

    // Verify the deleted keys and their values in the 'previous' result
    const deletedKeys = previous.map(({ key }) => key)
    const deletedValues = previous.map(({ value }) => value)
    same(deletedKeys.sort(), keysToDelete.sort())
    same(deletedValues, [1, 2, 1])

    // Verify the deleted keys are not present in the updated tree
    for (const key of keysToDelete) {
      let threw = false
      try {
        await updatedRoot.get(key)
      } catch (e) {
        if (e.message === 'Not found') threw = true
      }
      same(threw, true)
    }

    // Get the remaining keys after deletion and sort them
    const { result: entries } = await updatedRoot.getEntries(['a', 'zz'])
    const remainingKeys = entries.map(({ key }) => key)

    remainingKeys.sort()

    // Verify the remaining keys are as expected after deletion, also sort the expected keys
    same(remainingKeys.sort(compare), ['a', 'zz'].sort(compare))
  })

  it('delete multiple entries within range', async () => {
    const updatedList = [
      { key: 'a', value: 1 },
      { key: 'b', value: 1 },
      { key: 'bb', value: 2 },
      { key: 'c', value: 1 },
      { key: 'cc', value: 2 },
      { key: 'd', value: -1 },
      { key: 'ff', value: 2 },
      { key: 'h', value: 1 },
      { key: 'z', value: 1 },
      { key: 'zz', value: 2 }
    ]

    const { get, put } = storage()
    let root

    for await (const node of create({ get, compare, list: updatedList, ...opts })) {
      await put(await node.block)
      root = node
    }

    const keysToDelete = ['b', 'cc', 'd', 'z']
    const bulk = keysToDelete.map(key => ({ key, del: true }))
    const { blocks, root: updatedRoot, previous } = await root.bulk(bulk)
    await Promise.all(blocks.map(block => put(block)))

    previous.sort((a, b) => compare(a.key, b.key))

    // Verify the deleted keys and their values in the 'previous' result
    const deletedKeys = previous.map(({ key }) => key)
    const deletedValues = previous.map(({ value }) => value)
    same(deletedKeys.sort(), keysToDelete.sort())
    same(deletedValues, [1, 2, -1, 1])

    // Verify the deleted keys are not present in the updated tree
    for (const key of keysToDelete) {
      let threw = false
      try {
        await updatedRoot.get(key)
      } catch (e) {
        if (e.message === 'Not found') threw = true
      }
      same(threw, true)
    }

    // Get the remaining keys after deletion and sort them
    const { result: entries } = await updatedRoot.getAllEntries()
    const remainingKeys = entries.map(({ key }) => key)

    remainingKeys.sort(compare)

    // Verify the remaining keys are as expected after deletion, also sort the expected keys
    same(remainingKeys, ['a', 'bb', 'c', 'ff', 'h', 'zz'])
  })
  it('delete multiple entries with some outside leaf range', async () => {
    const { get, put } = storage()
    let root

    for await (const node of create({ get, compare, list, ...opts })) {
      await put(await node.block)
      root = node
    }

    // Include keys that are outside the range of the leaf's entryList
    const keysToDelete = ['b', 'cc', 'z', 'xxx']
    const bulk = keysToDelete.map(key => ({ key, del: true }))
    const { blocks, root: updatedRoot, previous } = await root.bulk(bulk)
    await Promise.all(blocks.map(block => put(block)))

    // Verify the deleted keys and their values in the 'previous' result
    const deletedKeys = previous.map(({ key }) => key)
    const deletedValues = previous.map(({ value }) => value)
    same(deletedKeys.sort(), ['b', 'cc', 'z'].sort())
    same(deletedValues, [1, 2, 1])

    // Verify the deleted keys are not present in the updated tree
    for (const key of keysToDelete) {
      let threw = false
      try {
        await updatedRoot.get(key)
      } catch (e) {
        if (e.message === 'Not found') threw = true
      }
      same(threw, true)
    }

    // Get the remaining keys after deletion and sort them
    const { result: entries } = await updatedRoot.getEntries(['a', 'zz'])
    const remainingKeys = entries.map(({ key }) => key)

    remainingKeys.sort()

    // Verify the remaining keys are as expected after deletion, also sort the expected keys
    same(remainingKeys.sort(compare), ['a', 'zz'].sort(compare))
  })

  it('big map', async () => {
    const { get, put } = storage()
    let mapRoot
    // let leaf
    for await (const node of create({ get, compare, list, ...opts })) {
      // if (node.isLeaf) leaf = node
      await put(await node.block)
      mapRoot = node
    }
    const { result } = await mapRoot.get('c').catch((e) => {
      same(e.message, 'Failed at key: c')
    })
    same(result, 1)

    const { blocks: blockX, root: rootX } = await mapRoot.bulk([{ key: 'ok', value: 200 }])
    await Promise.all(blockX.map((block) => put(block)))
    mapRoot = rootX

    const { result: result2 } = await mapRoot.get('ok').catch((e) => {
      same(e.message, 'Failed at key: ok')
    })
    same(result2, 200)

    const prefixes = ['b', 'A', '0', '']

    for (const prefix of prefixes) {
      for (let index = 10; index > 0; index--) {
        const key = prefix + index.toString()
        const bulk = [{ key, value: index }]
        const { blocks, root } = await mapRoot.bulk(bulk)
        await Promise.all(blocks.map((block) => put(block)))
        mapRoot = root
        const { result: result3 } = await mapRoot.get(key).catch((e) => {
          throw Error("Couldn't find key: " + key)
          // same(e.message, 'Failed at key: ' + key)
        })
        same(result3, index)
      }
    }
  })
  it('passing, slow. deterministic fuzzer', async () => {
    const { get, put } = storage()
    let mapRoot
    // let leaf
    for await (const node of create({ get, compare, list, ...opts })) {
      // if (node.isLeaf) leaf = node
      await put(await node.block)
      mapRoot = node
    }
    const { result } = await mapRoot.get('c').catch((e) => {
      same(e.message, 'Failed at key: c')
    })
    same(result, 1)

    for (let i = 0; i < 500; i++) {
      const randFun = mulberry32(i)
      for (let rowCount = 0; rowCount < 500; rowCount++) {
        const key = 'a-' + randFun()
        const value = `${i}-${rowCount}-${key}`
        const bulk = [{ key, value }]
        const { blocks, root } = await mapRoot.bulk(bulk)
        await Promise.all(blocks.map((block) => put(block)))
        mapRoot = root
        const { result: result3 } = await mapRoot.get(key).catch((e) => {
          same(e.message, `Failed at key: ${key} : ${value}`)
        })
        same(result3, value)
      }
    }
  }).timeout(60 * 1000)
})

function mulberry32 (a) {
  return function () {
    let t = (a += 0x6d2b79f5)
    t = Math.imul(t ^ (t >>> 15), t | 1)
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61)
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296
  }
}
