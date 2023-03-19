import { encode as multiformatEncode } from 'multiformats/block'
import { CIDCounter } from './utils.js'
import { newInsertsBulker } from './first-leaf.js'

class Entry {
  constructor ({ key, address }, opts = {}) {
    this.key = key
    this.address = address
    this.codec = opts.codec
    this.hasher = opts.hasher
  }

  get isEntry () {
    return true
  }
}

class EntryList {
  constructor ({ entries, closed }) {
    if (typeof closed !== 'boolean') throw new Error('Missing required argument "closed"')
    this.entries = entries
    this.closed = closed
    this.startKey = entries[0].key
  }

  find (key, compare) {
    const { entries } = this
    for (let i = entries.length - 1; i > -1; i--) {
      const entry = entries[i]
      const comp = compare(key, entry.key)
      if (comp > -1) {
        return [i, entry]
      }
    }
    return null
  }

  findMany (keys, compare, sorted = false, strict = false) {
    const { entries } = this
    const results = new Map()
    // Note: object based entries must be sorted
    if (!sorted) {
      keys = keys.sort(compare)
    } else {
      keys = [...keys]
    }
    for (let i = entries.length - 1; i > -1; i--) {
      if (!keys.length) break
      const entry = entries[i]
      const found = []
      while (keys.length) {
        let key = keys[keys.length - 1]
        key = key.key ? key.key : key
        const comp = compare(key, entry.key)
        if (!strict) {
          if (comp > -1) {
            found.push(keys.pop())
          } else {
            break // this is the only branch that hits if the new key sorts before the earliest key in the tree
          }
        } else {
          if (comp === 0) {
            found.push(keys.pop())
          } else if (comp > 0) {
            keys.pop()
          } else {
            break
          }
        }
      }
      if (found.length) {
        results.set(i, [entry, found])
      }
    }
    return results
  }

  findRange (start, end, compare) {
    const { entries } = this
    let last
    let first = 0
    for (let i = entries.length - 1; i > -1; i--) {
      const entry = entries[i]
      const comp = compare(end, entry.key)
      if (comp > 0) {
        last = i
        break
      }
    }
    for (let i = 0; i < entries.length; i++) {
      const entry = entries[i]
      const comp = compare(start, entry.key)
      if (comp === 0) {
        first = i
        break
      } else if (comp < 0) {
        break
      }
      first = i
    }
    return { first, last, entries: entries.slice(first, last + 1) }
  }
}

const stringKey = (key) => (typeof key === 'string' ? key : JSON.stringify(key))

async function sortBulk (bulk, opts) {
  return bulk.sort(({ key: a }, { key: b }) => opts.compare(a, b))
}

/**
 * Processes an array of nodes by calling processBranch for each node,
 * and returns an array of BranchEntryClass instances.
 *
 * @param {*} that - {codec, hasher, getNode, compare, cache, chunker} - The value of the `this` keyword as passed from bulk to processBranch.
 * @param {Object} results - {root, nodes: [{encode()}], blocks: [...], getAllEntries(): Promise, bulk(): Promise, previous: Array<{key, value}>} - An object containing the results of processing the nodes, along with the results of the entire bulk operation.
 * @param {Array} nodes - [{encode()}] - An array of nodes to process.
 * @param {Object} opts - {codec, hasher, getNode, compare, cache, sorted, ...} - An object containing options for processing the nodes.
 * @returns {Array} An array of BranchEntryClass instances.
*/
export async function processBranchEntries (that, results, nodes, opts) {
  // console.log('processBranchEntries', that.value, Object.keys(results), await nodes.map(async (n) => await n.address), Object.keys(opts))
  const entries = await Promise.all(nodes.map(async (node) => {
    const block = await node.encode()
    results.blocks.push(block)
    that.cache.set(node)
    return new opts.BranchEntryClass(node, opts)
  }))

  return entries
}

async function processRoot (that, results, bulk, nodeOptions) {
  const root = results.root
  const distance = root.distance
  const first = root.entryList.startKey
  const inserts = []

  for (const b of bulk) {
    const { key, del } = b
    if (nodeOptions.opts.compare(key, first) < 0) {
      if (!del) inserts.push(b)
    } else {
      break
    }
  }

  if (inserts.length) {
    await newInsertsBulker(that, inserts, nodeOptions, distance, root, results)
  }
}

class Node {
  constructor ({ entryList, chunker, distance, getNode, compare, cache }) {
    this.entryList = entryList
    this.chunker = chunker
    this.distance = distance
    this.getNode = getNode
    this.compare = compare
    this.cache = cache
  }

  get closed () {
    return this.entryList.closed
  }

  get key () {
    return this.entryList.startKey
  }

  async getEntry (key, cids = new CIDCounter()) {
    const result = await this._getEntry(key, cids)
    return { result, cids }
  }

  async _getEntry (key, cids) {
    cids.add(this)
    let node = this
    while (!node.isLeaf) {
      const result = node.entryList.find(key, this.compare)
      if (result === null) throw new Error('Not found')
      const [, entry] = result
      node = await this.getNode(await entry.address)
      cids.add(node)
    }
    const result = node.entryList.find(key, this.compare)
    if (result === null || result[1].key.toString() !== key.toString()) throw new Error('Not found')
    const [, entry] = result
    return entry
  }

  async getAllEntries (cids = new CIDCounter()) {
    const result = await this._getAllEntries(cids)
    return { result, cids }
  }

  _getAllEntries (cids) {
    cids.add(this)
    if (this.isLeaf) {
      return this.entryList.entries
    } else {
      const { entries } = this.entryList
      const mapper = async (entry) => this.getNode(await entry.address).then((node) => node._getAllEntries(cids)).catch(async (err) => {
        console.log('did not find', await entry.address)
        throw err
      })
      return Promise.all(entries.map(mapper)).then((results) => results.flat())
    }
  }

  async getEntries (keys, sorted = false, cids = new CIDCounter()) {
    const result = await this._getEntries(keys, sorted, cids)
    return { result, cids }
  }

  async _getEntries (keys, sorted, cids) {
    cids.add(this)
    if (!sorted) keys = keys.sort(this.compare)
    const results = this.entryList.findMany(keys, this.compare, true, this.isLeaf)
    if (this.isLeaf) {
      return [...results.values()].map(([entry]) => entry)
    }
    let entries = []
    for (const [entry, keys] of [...results.values()].reverse()) {
      const p = this.getNode(await entry.address)
      entries.push(p.then((node) => node._getEntries(keys.reverse(), true, cids)))
    }
    entries = await Promise.all(entries)
    return entries.flat()
  }

  async getRangeEntries (start, end, cids = new CIDCounter()) {
    const result = await this._getRangeEntries(start, end, cids)
    return { result, cids }
  }

  _getRangeEntries (start, end, cids) {
    cids.add(this)
    const { entries } = this.entryList.findRange(start, end, this.compare)
    if (this.isLeaf) {
      return entries.filter((entry) => {
        const s = this.compare(start, entry.key)
        const e = this.compare(end, entry.key)
        if (s <= 0 && e >= 0) return true
        return false
      })
    }

    if (!entries.length) return []
    const thenRange = async (entry) =>
      this.getNode(await entry.address).then((node) => {
        return node._getRangeEntries(start, end, cids)
      })
    const results = [thenRange(entries.shift())]

    if (!entries.length) return results[0]
    const last = thenRange(entries.pop())

    while (entries.length) {
      const thenAll = async (entry) =>
        this.getNode(await entry.address).then(async (node) => {
          return node._getAllEntries(cids)
        })
      results.push(thenAll(entries.shift()))
    }
    results.push(last)
    return Promise.all(results).then((results) => results.flat())
  }

  async transaction (bulk, opts = {}) {
    const { LeafClass, LeafEntryClass, BranchClass, BranchEntryClass } = opts
    opts = {
      codec: this.codec,
      hasher: this.hasher,
      getNode: this.getNode,
      compare: this.compare,
      cache: this.cache,
      ...opts
    }
    const nodeOptions = { chunker: this.chunker, opts }
    if (!opts.sorted) {
      bulk = bulk.sort(({ key: a }, { key: b }) => opts.compare(a, b))
      opts.sorted = true
    }
    const results = this.entryList.findMany(bulk, opts.compare, true, this.isLeaf)
    let entries = []
    if (this.isLeaf) {
      const previous = []
      const changes = {}
      const deletes = new Map()
      for (const { key, del, value } of bulk) {
        const skey = stringKey(key)
        if (del) {
          if (typeof changes[skey] === 'undefined') deletes.set(skey, null)
        } else {
          changes[skey] = { key, value }
          deletes.delete(skey)
        }
      }
      entries = [...this.entryList.entries]
      for (const [i, [entry]] of results) {
        previous.push(entry)
        const skey = stringKey(entry.key)
        if (deletes.has(skey)) {
          deletes.set(skey, i)
        } else {
          // console.log('LeafEntryClass transaction', entry.key.toString(), JSON.stringify(changes[skey]))
          entries[i] = new LeafEntryClass(changes[skey], opts)
          delete changes[skey]
        }
      }
      let count = 0
      for (const [, i] of deletes) {
        entries.splice(i - count++, 1)
      }
      const appends = Object.values(changes).map((obj) => {
        // console.log('LeafEntryClass appends', obj.key.toString(), JSON.stringify(obj))

        return new LeafEntryClass(obj, opts)
      })
      // TODO: there's a faster version of this that only does one iteration
      entries = entries.concat(appends).sort(({ key: a }, { key: b }) => opts.compare(a, b))
      const _opts = { ...nodeOptions, entries, NodeClass: LeafClass, distance: 0 }
      const nodes = await Node.from(_opts)
      return { nodes, previous, blocks: [], distance: 0 }
    } else {
      let distance = 0
      for (const [i, [entry, keys]] of results) {
        const p = this.getNode(await entry.address)
          .then((node) => node.transaction(keys.reverse(), { ...opts, sorted: true }))
          .then((r) => ({ entry, keys, distance, ...r }))
        results.set(i, p)
      }
      entries = [...this.entryList.entries]
      const final = { previous: [], blocks: [] }
      for (const [i, p] of results) {
        const { nodes, previous, blocks, distance: _distance } = await p
        distance = _distance
        entries[i] = nodes
        if (previous.length) final.previous = final.previous.concat(previous)
        if (blocks.length) final.blocks = final.blocks.concat(blocks)
      }
      entries = entries.flat()
      // TODO: rewrite this to use getNode concurrently on merge
      let newEntries = []
      let prepend = null
      for (let entry of entries) {
        if (prepend) {
          if (entry.isEntry) entry = await this.getNode(await entry.address)
          const entries = prepend.entryList.entries.concat(entry.entryList.entries)
          prepend = null
          const NodeClass = distance === 0 ? LeafClass : BranchClass
          const _opts = { ...nodeOptions, entries, NodeClass, distance }
          const nodes = await Node.from(_opts)
          if (!nodes[nodes.length - 1].closed) {
            prepend = nodes.pop()
          }
          if (nodes.length) {
            newEntries = newEntries.concat(nodes)
          }
        } else {
          if (!entry.isEntry && !entry.closed) {
            prepend = entry
          } else {
            newEntries.push(entry)
          }
        }
      }
      if (prepend) {
        newEntries.push(prepend)
      }
      distance++
      const toEntry = async (branch) => {
        if (branch.isEntry) return branch
        const block = await branch.encode()
        final.blocks.push(block)
        this.cache.set(branch)
        return new BranchEntryClass(branch, opts)
      }
      entries = await Promise.all(newEntries.map(toEntry))
      const _opts = { ...nodeOptions, entries, NodeClass: BranchClass, distance }
      return { nodes: await Node.from(_opts), ...final, distance }
    }
  }

  async bulk (bulk, opts = {}, isRoot = true) {
    const {
      BranchClass
      //  BranchEntryClass, LeafClass, LeafEntryClass
    } = opts
    opts = {
      codec: this.codec,
      hasher: this.hasher,
      getNode: this.getNode,
      compare: this.compare,
      cache: this.cache,
      ...opts
    }

    if (!opts.sorted) {
      bulk = await sortBulk(bulk, opts)
      opts.sorted = true
    }

    const nodeOptions = { chunker: this.chunker, opts }

    const results = await this.transaction(bulk, opts)

    while (results.nodes.length > 1) {
      const newDistance = results.nodes[0].distance + 1

      const branchEntries = await processBranchEntries(
        this,
        results,
        results.nodes,
        opts
      )

      const newNodes = await Node.from({
        ...nodeOptions,
        entries: branchEntries,
        NodeClass: BranchClass,
        distance: newDistance
      })

      const encodedBlocks = await Promise.all(newNodes.map(async (node) => {
        return await node.encode()
      }))

      results.nodes = newNodes
      results.blocks.push(...encodedBlocks)
    }

    const [root] = results.nodes
    results.root = root

    if (isRoot) {
      await processRoot(this, results, bulk, nodeOptions)
    }

    return results
  }

  static async from ({ entries, chunker, NodeClass, distance, opts }) {
    // console.log('Node.from entries:', entries.length)

    const parts = []
    let chunk = []
    for (const entry of entries) {
      chunk.push(entry)
      if (!entry.identity) {
        console.log('missing entry.identity', entry, chunker)
        // throw new Error('missing entry.identity')
      }
      if (await chunker(entry, distance)) {
        parts.push(new EntryList({ entries: chunk, closed: true }))
        chunk = []
      }
    }
    if (chunk.length) {
      parts.push(new EntryList({ entries: chunk, closed: false }))
    }

    // console.log('Node.from parts:', parts.length)

    return parts.map((entryList) => new NodeClass({ entryList, chunker, distance, ...opts }))
  }
}

class IPLDNode extends Node {
  constructor ({ codec, hasher, block, ...opts }) {
    // console.log('IPLDNode opts:', JSON.stringify([codec, hasher])) // Add this line
    super(opts)
    this.codec = codec
    this.hasher = hasher
    if (!block) {
      this.block = this.encode()
      this.address = this.block.then((block) => block.cid)
    } else {
      this.block = block
      this.address = block.cid
    }
  }

  async get (key) {
    const { result: entry, cids } = await this.getEntry(key)
    return { result: entry.key, cids }
  }

  async encode () {
    // console.log('MapLeaf-encode', this.entryList?.startKey, this.block?.value)
    if (this.block) return this.block
    // console.log('MapLeaf-encode', await this.address)
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
}

class IPLDBranch extends IPLDNode {
  async encodeNode () {
    const { entries } = this.entryList
    const mapper = async (entry) => [entry.key, await entry.address]
    const list = await Promise.all(entries.map(mapper))
    return { branch: [this.distance, list], closed: this.closed }
  }

  get isBranch () {
    return true
  }
}

class IPLDLeaf extends IPLDNode {
  encodeNode () {
    const list = this.entryList.entries.map((entry) => entry.encodeNode())
    return { leaf: list, closed: this.closed }
  }

  get isLeaf () {
    return true
  }
}

const create = async function * (obj) {
  let { LeafClass, LeafEntryClass, BranchClass, BranchEntryClass, list, chunker, compare, ...opts } = obj
  // console.log('LeafEntryClass create', list)
  list = list.map((value) => new LeafEntryClass(value, opts))
  opts.compare = compare
  let nodes = await Node.from({ entries: list, chunker, NodeClass: LeafClass, distance: 0, opts })
  yield * nodes
  let distance = 1
  while (nodes.length > 1) {
    const mapper = async (node) => new BranchEntryClass({ key: node.key, address: await node.address }, opts)
    const entries = await Promise.all(nodes.map(mapper))
    nodes = await Node.from({ entries, chunker, NodeClass: BranchClass, distance, opts })
    yield * nodes
    distance++
  }
}

export { Node, Entry, EntryList, IPLDNode, IPLDLeaf, IPLDBranch, create }
