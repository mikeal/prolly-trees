/* globals describe, it */
import { deepStrictEqual as same } from 'assert'
import { create } from '../src/map.js'
import * as codec from '@ipld/dag-cbor'
import { sha256 as hasher } from 'multiformats/hashes/sha2'
import { nocache } from '../src/cache.js'
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
    if (!block) {
      throw new Error('Not found')
    }
    return block
  }
  return { get, put, blocks }
}

const opts = { cache, chunker, codec, hasher }

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

describe('map first-leaf', () => {
  it('minimal test case with leftmost key insertion', async () => {
    const { get, put } = storage()
    let mapRoot

    // Create the initial map with one entry
    const initialList = [{ key: 'original', value: 1 }]
    for await (const node of create({ get, compare, list: initialList, ...opts })) {
      await put(await node.block)
      mapRoot = node
    }

    // Verify the initial entry
    const { result: initialResult } = await mapRoot.get('original')
    same(initialResult, 1)

    // Insert a new leftmost key
    const bulk = [{ key: 'before', value: 2 }]
    const { blocks, root } = await mapRoot.bulk(bulk)
    for (const block of blocks) {
      await put(block)
    }

    mapRoot = root

    // Verify the new leftmost key
    const { result: newResult } = await mapRoot.get('before')
    same(newResult, 2)

    // Verify that the original entry still exists and has the correct value
    const { result: updatedResult } = await mapRoot.get('original')
    same(updatedResult, 1)
  })
  it('minimal test case with leftmost key insertion and custom always-split chunker', async () => {
    const { get, put } = storage()
    let mapRoot

    // Create the initial map with one entry
    const initialList = [{ key: 'original', value: 1 }]
    for await (const node of create({ get, compare, list: initialList, ...opts })) {
      await put(await node.block)
      mapRoot = node
    }

    // Verify the initial entry
    const { result: initialResult } = await mapRoot.get('original')
    same(initialResult, 1)

    // Custom chunker that always returns true for splitting
    const alwaysSplitChunker = async (entry, distance) => {
      return true
    }

    // Insert a new leftmost key
    const bulk = [{ key: 'before', value: 2 }]
    const { blocks, root } = await mapRoot.bulk(bulk, { ...opts, chunker: alwaysSplitChunker })
    for (const block of blocks) {
      await put(block)
    }

    mapRoot = root

    // Verify the new leftmost key
    const { result: newResult } = await mapRoot.get('before')
    same(newResult, 2)

    // Verify that the original entry still exists and has the correct value
    const { result: updatedResult } = await mapRoot.get('original')
    same(updatedResult, 1)
  })

  it('basic numeric string key with specific keys and no loops inline', async () => {
    const { get, put } = storage()
    let mapRoot

    const list = createList([
      ['b', 1],
      ['bb', 2],
      ['c', 1],
      ['cc', 2]
    ])

    for await (const node of create({ get, compare, list, ...opts })) {
      await put(await node.block)
      mapRoot = node
    }

    // Test getting a key that exists
    const { result } = await mapRoot.get('c')
    same(result, 1)

    // Test getting a key that does not exist
    await mapRoot
      .get('a')
      .then(() => {
        throw new Error('Key should not exist')
      })
      .catch((e) => {
        same(e.message, 'Not found')
      })

    const everything0 = await mapRoot.getAllEntries()
    same(everything0.result.length, 4)
    // Test bulk insert with a key that does not exist
    const key2 = 'a'
    const value2 = `32-${key2}`
    const bulk2 = [{ key: key2, value: value2 }]
    const { blocks: blocks2, root: root2 } = await mapRoot.bulk(bulk2)
    for (const block of blocks2) {
      await put(block)
    }

    await put(root2.block)
    const address = await root2.address
    const gotRoot = await get(address)
    mapRoot = root2
    same(gotRoot.value, root2.block.value)
    const everything1 = await mapRoot.getAllEntries()
    same(everything1.result.length, 1)

    await mapRoot
      .get(key2)
      .then((val) => {
        same(val.result, value2)
      })
      .catch((e) => {
        throw e
      })
  })

  it('minimal nonreproduction of duplicate CID issue', async () => {
    const { get, put } = storage()
    let mapRoot

    const list = createList([
      ['b', 1],
      ['c', 1]
      // ['d', 1] // if this is removed, the test passes
    ])

    for await (const node of create({ get, compare, list, ...opts })) {
      await put(await node.block)
      mapRoot = node
    }

    // Test bulk insert with a key that does not exist
    const key2 = 'a'
    const value2 = `32-${key2}`
    const bulk2 = [{ key: key2, value: value2 }]
    const { blocks: blocks2, root: root2 } = await mapRoot.bulk(bulk2)
    for (const block of blocks2) {
      await put(block)
    }

    await put(root2.block)
    const address = await root2.address
    const gotRoot = await get(address)
    same(gotRoot.value, root2.block.value)
    same(root2.entryList.entries.length, 2)
    mapRoot = root2

    const everything1 = await mapRoot.getAllEntries()
    same(everything1.result.length, 3)
    await mapRoot
      .get(key2)
      .then((val) => {
        same(val.result, value2)
      })
      .catch((e) => {
        throw e
      })
  })
  it('minimal reproduction of duplicate CID issue', async () => {
    const { get, put } = storage()
    let mapRoot

    const list = createList([
      ['b', 1],
      ['c', 1],
      ['d', 1] // if this is removed, the test passes
    ])

    for await (const node of create({ get, compare, list, ...opts })) {
      await put(await node.block)
      mapRoot = node
    }

    // Test bulk insert with a key that does not exist
    const key2 = 'a'
    const value2 = `32-${key2}`
    const bulk2 = [{ key: key2, value: value2 }]
    const { blocks: blocks2, root: root2 } = await mapRoot.bulk(bulk2)
    for (const block of blocks2) {
      await put(block)
    }

    await put(root2.block)
    const address = await root2.address
    const gotRoot = await get(address)
    same(gotRoot.value, root2.block.value)
    same(root2.entryList.entries.length, 1)
    mapRoot = root2

    const everything1 = await mapRoot.getAllEntries()

    same(everything1.result.length, 1)
    await mapRoot
      .get(key2)
      .then((val) => {
        same(val.result, value2)
      })
      .catch((e) => {
        throw e
      })
  })
  it('minimal failing test case', async () => {
    const { get, put } = storage()
    let mapRoot

    for await (const node of create({ get, compare, list, ...opts })) {
      await put(await node.block)
      mapRoot = node
    }

    const limit = 100
    const errors = []

    for (let rowCount = 90; rowCount < limit; rowCount++) {
      const key = String.fromCharCode(rowCount)
      const value = `${rowCount}-${key}`
      const bulk = [{ key, value }]

      const { blocks, root } = await mapRoot.bulk(bulk)
      for (const block of blocks) {
        await put(block)
      }

      mapRoot = root
      try {
        await mapRoot.get(key)
      } catch (e) {
        errors.push({ key, value, rowCount })
      }
    }
    same(errors.length, 0)
  })

  it('basic numeric string key', async () => {
    const { get, put } = storage()
    let mapRoot
    for await (const node of create({ get, compare, list, ...opts })) {
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
      const bulk = [{ key, value }]
      const { blocks, root } = await mapRoot.bulk(bulk)
      for (const bl of blocks) {
        await put(bl)
      }

      mapRoot = root
      await mapRoot
        .get(key)
        .then(() => {})
        .catch((e) => {
          errors.push({ key, value, rowCount })
        })
    }
    same(errors.length, 0)
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
    const {
      blocks,
      root: updatedRoot,
      previous
    } = await root.bulk([{ key: keyToInsert, value: valueToInsert }], { chunker })
    await Promise.all(blocks.map((block) => put(block)))

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
  it('inserts with uninitialized node', async () => {
    const { get, put } = storage()
    let root

    for await (const node of create({ get, compare, list, ...opts })) {
      await put(await node.block)
      root = node
    }
    const keysToInsert = ['1', '2', '3']
    const bulk = keysToInsert.map((key) => ({ key, value: parseInt(key) }))

    const { blocks, root: updatedRoot } = await root.bulk(bulk, { ...opts, chunker })
    await Promise.all(blocks.map((block) => put(block)))
    // Verify the inserted keys and their values
    for (const key of keysToInsert) {
      const value = await updatedRoot.get(key)
      same(value.result, parseInt(key))
    }

    // Get the existing keys after insertion and sort them
    const { result: entries } = await updatedRoot.getAllEntries()
    const resultingKeys = entries.map(({ key }) => key)

    resultingKeys.sort(compare)

    // Verify the resulting keys are as expected after insertion, also sort the expected keys
    const expectedKeys = ['a', 'b', 'cc', 'c', 'bb', 'd', 'ff', 'h', 'z', 'zz', '1', '2', '3'].sort(compare)
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
})
