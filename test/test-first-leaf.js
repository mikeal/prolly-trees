import { encode as multiformatEncode } from 'multiformats/block'
import { encodeBlocks } from '../src/first-leaf.js'

/* globals describe, it */
import { deepStrictEqual as same } from 'assert'
import { create } from '../src/map.js'
import * as codec from '@ipld/dag-cbor'
import { sha256 as hasher } from 'multiformats/hashes/sha2'
import { nocache } from '../src/cache.js'
import { bf, simpleCompare as compare } from '../src/utils.js'
// import { EntryList } from '../src/base.js'
const chunker = bf(3)

const cache = nocache

const storage = () => {
  const blocks = {}
  const put = (block) => {
    console.log('Storing block with CID:', block?.cid?.toString())

    blocks[block.cid.toString()] = block
  }
  const get = async (cid) => {
    console.log('Retrieving block with CID:', cid.toString())

    const block = blocks[cid.toString()]
    if (!block) throw new Error('Not found')
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

async function mfEncode () {
  console.log('MapLeaf.encode', this.entryList?.startKey, this.block?.value)
  if (this.block) return this.block

  const value = await this.encodeNode()
  const opts = { codec: this.codec, hasher: this.hasher, value }

  console.log('encode options:', opts.value)

  if (!opts.codec || !opts.hasher) {
    console.trace('Missing codec or hasher')
  }

  this.block = await multiformatEncode(opts)
  console.log('this.encode done', await this.block.cid)

  return this.block
}

async function encodeNodeWithoutCircularReference (that, node) {
  return (await encodeBlocks('testCallId', that, [node]))[0]
}

describe('map', () => {
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
    console.log(
      'everything0',
      everything0.cids,
      everything0.result.map(({ key, value }) => ({ key, value }))
    )

    // Test bulk insert with a key that does not exist
    const key2 = 'a'
    const value2 = `32-${key2}`
    const bulk2 = [{ key: key2, value: value2 }]
    const { blocks: blocks2, root: root2 } = await mapRoot.bulk(bulk2)
    for (const block of blocks2) {
      console.log('putting', block.value, block.cid.toString())
      await put(block)
    }

    await put(root2.block)
    const address = await root2.address
    const gotRoot = await get(address)
    console.log('root2', root2.entryList.entries.length, root2.entryList.startKey, gotRoot.value, address)
    mapRoot = root2
    console.log('Root CID before getAllEntries:', (await mapRoot.address).toString())

    const everything1 = await mapRoot.getAllEntries()
    console.log(
      'everything1',
      everything1.cids,
      everything1.result.map(({ key, value }) => ({ key, value }))
    )

    await mapRoot
      .get(key2)
      .then((val) => {
        same(val, value2)
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
      console.log('putting', block.value, block.cid.toString())
      await put(block)
    }

    await put(root2.block)
    const address = await root2.address
    const gotRoot = await get(address)
    console.log('root2', root2.entryList.entries.length, root2.entryList.startKey, gotRoot.value, address)
    mapRoot = root2
    console.log('Root CID before getAllEntries:', (await mapRoot.address).toString())

    const everything1 = await mapRoot.getAllEntries()
    console.log(
      'everything1',
      everything1.cids,
      everything1.result.map(({ key, value }) => ({ key, value }))
    )
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
      console.log('putting', block, block.value, block?.cid?.toString())
      await put(block)
    }

    await put(root2.block)
    const address = await root2.address
    const gotRoot = await get(address)
    console.log('root2', root2.entryList.entries.length, root2.entryList.startKey, gotRoot.value, address)
    mapRoot = root2
    console.log('Root CID before getAllEntries:', (await mapRoot.address).toString())

    const everything1 = await mapRoot.getAllEntries()
    console.log(
      'everything1',
      everything1.cids,
      everything1.result.map(({ key, value }) => ({ key, value }))
    )
    await mapRoot
      .get(key2)
      .then((val) => {
        same(val.result, value2)
      })
      .catch((e) => {
        throw e
      })
  })

  it('test first-left encodeNodeWithoutCircularReference', async () => {
    const that = {
      codec: { encode: (e) => e + '1' },
      encode (e) {
        return e.value + '2'
      }
    }
    const node = {
      block: { value: 'ok' }
    }
    const output = await encodeNodeWithoutCircularReference(that, node)
    same(output, 'ok12')
  })
  it('test encodeNodeWithoutCircularReference with complex node', async () => {
    const that = {
      codec: { encode: (e) => JSON.stringify(e) },
      encode (e) {
        return e.value + '2'
      }
    }
    const node = {
      block: {
        value: {
          key: 'exampleKey',
          data: 'exampleData'
        }
      }
    }
    const output = await encodeNodeWithoutCircularReference(that, node)
    same(output, JSON.stringify({ key: 'exampleKey', data: 'exampleData' }) + '2')
  })
  it('test encodeNodeWithoutCircularReference with base64 encoding and buffer', async () => {
    const that = {
      codec: { encode: (e) => Buffer.from(JSON.stringify(e)).toString('base64') },
      encode (e) {
        return e.value + '2'
      }
    }
    const node = {
      block: {
        value: Buffer.from('ok')
      }
    }
    const output = await encodeNodeWithoutCircularReference(that, node)
    same(output, Buffer.from(JSON.stringify(Buffer.from('ok'))).toString('base64') + '2')
  })
  it('test encodeNodeWithoutCircularReference with custom encoding and decoding', async () => {
    const customEncode = (obj) => {
      return Object.entries(obj)
        .map(([key, value]) => `${key}:${value}`)
        .join(',')
    }

    const customDecode = (str) => {
      return str
        .split(',')
        .map((entry) => entry.split(':'))
        .reduce((acc, [key, value]) => {
          acc[key] = value
          return acc
        }, {})
    }

    const that = {
      codec: { encode: customEncode },
      encode (e) {
        return customDecode(e.value)
      }
    }
    const node = {
      block: {
        value: {
          key: 'exampleKey',
          data: 'exampleData'
        }
      }
    }
    const output = await encodeNodeWithoutCircularReference(that, node)
    const expectedValue = customEncode({ key: 'exampleKey', data: 'exampleData' })
    const decodedOutput = customDecode(expectedValue)
    same(output, decodedOutput)
  })
  it('test encodeNodeWithoutCircularReference with distinct inputs should produce distinct outputs', async () => {
    const that = {
      codec: { encode: (e) => JSON.stringify(e) },
      encode (e) {
        return e.value + '2'
      }
    }
    const node1 = {
      block: {
        value: {
          key: 'exampleKey1',
          data: 'exampleData1'
        }
      }
    }
    const node2 = {
      block: {
        value: {
          key: 'exampleKey2',
          data: 'exampleData2'
        }
      }
    }
    const output1 = await encodeNodeWithoutCircularReference(that, node1)
    const output2 = await encodeNodeWithoutCircularReference(that, node2)

    if (output1 === output2) {
      throw new Error('encodeNodeWithoutCircularReference should produce distinct outputs for distinct inputs')
    }
  })
  it('test encodeNodeWithoutCircularReference multiformats with distinct inputs should produce distinct outputs', async () => {
    function createThat (entryList, distance, closed) {
      return {
        codec,
        hasher,
        encode: mfEncode.bind({
          codec,
          hasher,
          encodeNode: async () => {
            const mapper = (entry) => [entry.key, entry.address]
            const list = entryList.entries.map(mapper)
            console.log('encodeNode.called', distance, list, Math.random())
            return { branch: [distance, list], closed }
          }
        }),
        distance,
        closed
      }
    }

    const address1 = { key: 'exampleKey1', data: 'exampleData1', block: { value: 'ok1' } }
    const address2 = { key: 'exampleKey2', data: 'exampleData2', block: { value: 'ok2' } }

    const entryList1 = {
      entries: [
        {
          key: 'exampleKey1',
          address: address1
        }
      ]
    }

    const that1 = createThat(entryList1, 0, false)
    const output1 = JSON.parse(JSON.stringify((await encodeNodeWithoutCircularReference(that1, address1)).value))

    const entryList2 = {
      entries: [
        {
          key: 'exampleKey2',
          address: address2
        }
      ]
    }

    const that2 = createThat(entryList2, 0, false)
    const output2 = JSON.parse(JSON.stringify((await encodeNodeWithoutCircularReference(that2, address2)).value))

    console.log('diffz', output1.branch[1], output2.branch[1])

    if (JSON.stringify(output1) === JSON.stringify(output2)) {
      throw new Error('encodeNodeWithoutCircularReference should produce distinct outputs for distinct inputs')
    }
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
    const bulk = keysToInsert.map((key) => ({ key, value: -1 }))

    const { blocks, root: updatedRoot } = await root.bulk(bulk, { ...opts, chunker: customChunker })
    await Promise.all(blocks.map((block) => put(block)))
    // console.log('updatedRoot', updatedRoot)
    // Verify the inserted keys and their values
    for (const key of keysToInsert) {
      const value = await updatedRoot.get(key)
      same(value, -1)
    }

    // Get the existing keys after insertion and sort them
    const { result: entries } = await updatedRoot[0].getEntries(['-3', 'zz'])
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
