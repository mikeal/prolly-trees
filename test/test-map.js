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
  const put = (block) => {
    blocks[block.cid.toString()] = block
  }
  const get = async (cid) => {
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

const createList = (entries) => entries.map(([key, value]) => ({ key, value }))

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
      const keys = entries.map((entry) => entry.key)
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
  it('getAllEntries', async () => {
    const { get, put } = storage()
    let root
    for await (const node of create({ get, compare, list, ...opts })) {
      await put(await node.block)
      root = node
    }
    const verify = (entries, start, end) => {
      const keys = entries.map((entry) => entry.key)
      const comp = list.slice(start, end).map(({ key }) => key)
      same(keys, comp)
    }
    const { result: entries } = await root.getAllEntries()
    verify(entries)
  })
  it('bulk insert 2', async () => {
    const { get, put } = storage()
    let last
    for await (const node of create({ get, compare, list, ...opts })) {
      await put(await node.block)
      last = node
    }
    const verify = (entries, start, end) => {
      const keys = entries.map((entry) => entry.key)
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
    await Promise.all(blocks.map((block) => put(block)))
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
      await Promise.all(blocks.map((block) => put(block)))
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
      await Promise.all(blocks.map((block) => put(block)))
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
      same(
        entries.map(({ key }) => key),
        front
      )
      front.shift()
    }
    while (front.length) {
      const { result: entries } = await last.getRangeEntries(back[0], back[back.length - 1] + '.')
      same(
        entries.map(({ key }) => key),
        back
      )
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
    await Promise.all(blocks.map((block) => put(block)))
    same((await rr.get('aaa')).result, 3)
  })
  it('basic numeric string key', async () => {
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

    const errors = []
    const limit = 500
    for (let rowCount = 33; rowCount < limit; rowCount++) {
      const key = String.fromCharCode(rowCount)
      const value = `${rowCount}-${key}`
      // console.log('writing', key, value)
      const bulk = [{ key, value }]
      const { blocks, root } = await mapRoot.bulk(bulk)
      // await Promise.all(blocks.map((block) => await put(block)))
      for (const bl of blocks) {
        await put(bl)
      }

      mapRoot = root
      await mapRoot
        .get(key)
        .then(() => {
          // console.log('got', key, value)
        })
        .catch((e) => {
          errors.push({ key, value, rowCount })
        })
    }
    console.log('ok keys', limit - errors.length)
    console.log('unhandled keys', errors.length)
    // anything with charcode less than 97, eg before lowercase a, will fail
    console.log(
      'unhandled keys',
      errors.map(({ key, rowCount }) => key)
    )
    same(errors.length, 0)
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
  it('insert causes chunker to return true for non-empty bulk', async () => {
    const { get, put } = storage()
    let root

    for await (const node of create({ get, compare, list, ...opts })) {
      await put(await node.block)
      root = node
    }

    // Custom chunker function to force a split on a non-empty bulk
    const chunker = async (node, context) => {
      if (context.height === 1 && node.bulk.length > 0) {
        return true
      }
      return false
    }

    // Insert a new key-value pair that will cause the chunker to return true
    const keyToInsert = 'x'
    const valueToInsert = 42
    const { blocks, root: updatedRoot, previous } = await root.bulk([{ key: keyToInsert, value: valueToInsert }], { chunker })
    await Promise.all(blocks.map(block => put(block)))

    // Verify the previous result is empty, as it's a new insertion
    same(previous, [])

    // Verify the new key-value pair is present in the updated tree
    const { result } = await updatedRoot.get(keyToInsert)
    same(result, valueToInsert)

    // Verify the remaining keys are still present in the updated tree
    for (const { key, value } of list) {
      const { result } = await updatedRoot.get(key)
      same(result, value)
    }
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
  it('inserts with custom chunker that triggers branch for uninitialized node', async () => {
    const { get, put } = storage()
    let root

    for await (const node of create({ get, compare, list, ...opts })) {
      await put(await node.block)
      root = node
    }

    // Custom chunker that inserts new entries with keys smaller than existing entries
    const customChunker = async (entry, distance) => {
      return compare(entry.key, 'a') < 0
    }

    const keysToInsert = ['-1', '-2', '-3']
    const bulk = keysToInsert.map(key => ({ key, value: -1 }))

    const { blocks, root: updatedRoot } = await root.bulk(bulk, { ...opts, chunker: customChunker })
    await Promise.all(blocks.map(block => put(block)))

    // Verify the inserted keys and their values
    for (const key of keysToInsert) {
      const value = await updatedRoot.get(key)
      same(value, -1)
    }

    // Get the existing keys after insertion and sort them
    const { result: entries } = await updatedRoot.getEntries(['-3', 'zz'])
    const resultingKeys = entries.map(({ key }) => key)

    resultingKeys.sort(compare)

    // Verify the resulting keys are as expected after insertion, also sort the expected keys
    const expectedKeys = ['a', 'b', 'cc', 'ff', 'h', 'z', '-1', '-2', '-3'].sort(compare)
    same(resultingKeys, expectedKeys)
  })

  it('insert multiple entries within range causing chunker function to return true', async () => {
    const initialList = [
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

    // Create initial tree
    for await (const node of create({ get, compare, list: initialList, ...opts })) {
      await put(await node.block)
      root = node
    }

    // Inserts that fall within the range of the tree
    const bulkInserts = [
      { key: 'aa', value: 3 },
      { key: 'ca', value: 4 },
      { key: 'da', value: 5 }
    ]

    // Custom chunker function that returns true for a non-empty bulk
    const customChunker = async (entry, distance) => {
      return distance > 0
    }

    // Perform bulk insert
    const { blocks, root: newRoot } = await root.bulk(bulkInserts, { ...opts, chunker: customChunker })

    // Save the new blocks
    await Promise.all(blocks.map((block) => put(block)))

    // Assert the new entries have been inserted correctly
    const _get = async (k) => (await newRoot.get(k)).result
    same(await _get('aa'), 3)
    same(await _get('ca'), 4)
    same(await _get('da'), 5)

    // Assert the original entries remain unchanged
    const expected = [
      ['a', 1],
      ['aa', 3],
      ['b', 1],
      ['bb', 2],
      ['c', 1],
      ['ca', 4],
      ['cc', 2],
      ['d', -1],
      ['da', 5],
      ['ff', 2],
      ['h', 1],
      ['z', 1],
      ['zz', 2]
    ]

    for (const [key, value] of expected) {
      same(await _get(key), value)
    }

    // Assert the tree's structure remains valid
    const { result: allEntries } = await newRoot.getAllEntries()
    const actualKeys = allEntries.map((entry) => entry.key)
    const expectedKeys = expected.map(([key]) => key)
    same(actualKeys, expectedKeys)
  })
  it('inserts with custom chunker that triggers uncovered branches', async () => {
    const { get, put } = storage()
    let root

    for await (const node of create({ get, compare, list, ...opts })) {
      await put(await node.block)
      root = node
    }

    // Custom chunker function that triggers specific branches
    const customChunker = async (entry, distance) => {
      return distance > 1
    }

    // Bulk inserts that will trigger the branches
    const bulkInserts = [
      { key: 'a', value: 10 },
      { key: 'c', value: 20 },
      { key: 'h', value: 30 },
      { key: 'z', value: 40 }
    ]

    // Perform bulk insert
    const { blocks, root: newRoot } = await root.bulk(bulkInserts, { ...opts, chunker: customChunker })

    // Save the new blocks
    await Promise.all(blocks.map((block) => put(block)))

    // Assert the new entries have been inserted correctly
    const _get = async (k) => (await newRoot.get(k)).result
    same(await _get('a'), 10)
    same(await _get('c'), 20)
    same(await _get('h'), 30)
    same(await _get('z'), 40)

    // Assert the other entries remain unchanged
    const expected = [
      ['a', 10],
      ['b', 1],
      ['bb', 2],
      ['c', 20],
      ['cc', 2],
      ['d', 1],
      ['ff', 2],
      ['h', 30],
      ['z', 40],
      ['zz', 2]
    ]

    for (const [key, value] of expected) {
      same(await _get(key), value)
    }

    // Assert the tree's structure remains valid
    const { result: allEntries } = await newRoot.getAllEntries()
    const actualKeys = allEntries.map((entry) => entry.key)
    const expectedKeys = expected.map(([key]) => key)
    same(actualKeys, expectedKeys)
  })

  it.skip('big map', async () => {
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
          same(e.message, 'Failed at key: ' + key)
        })
        same(result3, index)
      }
    }
  })
  it.skip('deterministic fuzzer', async () => {
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

    for (let i = 0; i < 100; i++) {
      const randFun = mulberry32(i)
      for (let rowCount = 0; rowCount < 100; rowCount++) {
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
