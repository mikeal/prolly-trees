import { encode as multiformatEncode } from 'multiformats/block'
import { CIDCounter } from './utils.js'

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
            break // the new key sorts before the leftmost key in the tree
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

const stringKey = key => typeof key === 'string' ? key : JSON.stringify(key)

function sortBulk (bulk, opts) {
  return bulk.sort(({ key: a }, { key: b }) => opts.compare(a, b))
}

async function filterLeftmostInserts (first, bulk, compare) {
  const inserts = []

  for (const b of bulk) {
    const { key, del } = b
    if (compare(key, first) < 0) {
      if (!del) inserts.push(b)
    } else {
      break
    }
  }

  return inserts
}

async function generateNewLeaves (inserts, opts, { chunker, compare }) {
  return await Node.from({
    entries: inserts
      .map((insert) => new opts.LeafEntryClass(insert, opts))
      .sort((a, b) => compare(a.key, b.key)),
    chunker,
    NodeClass: opts.LeafClass,
    distance: 0,
    opts
  })
}

async function generateBranchEntries (that, newLeaves, results, opts) {
  return await Promise.all(
    newLeaves.map(async (node) => {
      const block = await node.encode()
      results.blocks.push(block)
      that.cache.set(node)
      const newBranchEntry = new opts.BranchEntryClass(
        { key: node.key, address: await node.address },
        opts
      )
      return newBranchEntry
    })
  )
}

async function processRoot (that, results, bulk, nodeOptions) {
  const root = results.root
  const opts = nodeOptions.opts
  const distance = root.distance
  const first = root.entryList.startKey
  const inserts = await filterLeftmostInserts(first, bulk, that.compare)
  if (inserts.length) {
    const newLeaves = await generateNewLeaves(inserts, opts, that)
    const branchEntries = await generateBranchEntries(that, newLeaves, results, opts)
    const firstRootEntry = new opts.BranchEntryClass({ key: root.entryList.startKey, address: await root.address }, opts)
    results.blocks.push((await root.encode()))
    that.cache.set(root)
    const newBranchEntries = [firstRootEntry, ...branchEntries].sort(({ key: a }, { key: b }) => opts.compare(a, b))
    let newBranches = await Node.from({
      ...nodeOptions,
      entries: newBranchEntries,
      chunker: that.chunker,
      NodeClass: opts.BranchClass,
      distance: distance + 1
    })
    let allBranches = [...newBranches]
    while (newBranches.length > 1) {
      const newBranchEntries = await Promise.all(newBranches.map(async l =>
        new opts.BranchEntryClass({ key: l.key, address: await l.address }, opts)
      ))
      newBranches = await Node.from({
        ...nodeOptions,
        entries: newBranchEntries.sort(({ key: a }, { key: b }) => opts.compare(a, b)),
        chunker: that.chunker,
        NodeClass: opts.BranchClass,
        distance: distance + 1
      })

      allBranches = [...allBranches, ...newBranches]
    }
    await Promise.all(allBranches.map(async (m) => {
      const block = await m.encode()
      that.cache.set(m)
      results.blocks.push(block)
    }))
    results.root = newBranches[0]
    // results.nodes = [root]
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
    if (result === null ||
      (result[1].key.toString() !== key.toString())) throw new Error('Not found')
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
      const mapper = async (entry) =>
        this.getNode(await entry.address)
          .then((node) => node._getAllEntries(cids))
          .catch(async err => { throw err })
      return Promise.all(entries.map(mapper)).then(results => results.flat())
    }
  }

  async * vis (cids = new Set()) {
    const renderNodeLabel = async (node) => {
      const entries = node.entryList.entries.map((e) => `[${e.key},${JSON.stringify(e.value || '').replace(/"/g, "'")}]`).join(', ')
      if (node.isLeaf) {
        return `Leaf [${entries}]`
      } else {
        return `Branch [${entries}]`
      }
    }

    const visit = async function * (node, parentId, cids) {
      const nodeId = (await node.address)// .toString()
      if (!cids.has(nodeId)) {
        cids.add(nodeId)

        const nodeLabel = await renderNodeLabel(node)
        yield `  node [shape=ellipse fontname="Courier"]; ${nodeId} [label="${nodeLabel}"];`
        yield `  ${parentId} -> ${nodeId};`

        for (const entry of node.entryList.entries) {
          if (entry.address) {
            const entryId = (await entry.address)// .toString()
            try {
              const childNode = await node.getNode(entryId)
              yield * await visit(childNode, nodeId, cids)
            } catch (err) {
              yield `  ${nodeId} -> ${entryId};`
              yield `  node [shape=ellipse fontname="Courier"]; ${entryId} [label="Error: ${err.message}"];`
            }
          }
        }
      }
    }

    yield 'digraph tree {'
    // const rootCid = (await this.address).toString()
    // const rootNode = await this.getNode(rootCid)
    yield '  node [shape=ellipse fontname="Courier"]; root;'

    for await (const line of visit(this, 'root', cids)) {
      yield line
    }

    yield '}'
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
      entries.push(p.then(node => node._getEntries(keys.reverse(), true, cids)))
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
      return entries.filter(entry => {
        const s = this.compare(start, entry.key)
        const e = this.compare(end, entry.key)
        if (s <= 0 && e >= 0) return true
        return false
      })
    }

    if (!entries.length) return []
    const thenRange = async entry =>
      this.getNode(await entry.address).then(node =>
        node._getRangeEntries(start, end, cids))

    const results = [thenRange(entries.shift())]

    if (!entries.length) return results[0]
    const last = thenRange(entries.pop())

    while (entries.length) {
      const thenAll = async (entry) =>
        this.getNode(await entry.address).then(async node =>
          node._getAllEntries(cids))
      results.push(thenAll(entries.shift()))
    }
    results.push(last)
    return Promise.all(results).then(results => results.flat())
  }

  async transaction (bulk, opts = {}) {
    opts = {
      codec: this.codec,
      hasher: this.hasher,
      getNode: this.getNode,
      compare: this.compare,
      cache: this.cache,
      ...opts
    }
    const nodeOptions = { chunker: this.chunker, opts }
    const results = this.entryList.findMany(bulk, opts.compare, true, this.isLeaf)

    if (this.isLeaf) {
      return await this.transactionLeaf(bulk, opts, nodeOptions, results)
    } else {
      return await this.transactionBranch(bulk, opts, nodeOptions, results)
    }
  }

  async transactionLeaf (bulk, opts, nodeOptions, results) {
    const { LeafClass, LeafEntryClass } = opts
    const previous = []
    let entries = []
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
        entries[i] = new LeafEntryClass(changes[skey], opts)
        delete changes[skey]
      }
    }
    let count = 0
    for (const [, i] of deletes) {
      entries.splice(i - count++, 1)
    }
    const appends = Object.values(changes).map(obj => new LeafEntryClass(obj, opts))
    // TODO: there's a faster version of this that only does one iteration

    entries = entries.concat(appends).sort(({ key: a }, { key: b }) => opts.compare(a, b))

    const _opts = { ...nodeOptions, entries, NodeClass: LeafClass, distance: 0 }
    const nodes = await Node.from(_opts)
    // why is blocks empty?
    return { nodes, previous, blocks: [], distance: 0 }
  }

  async transactionBranch (bulk, opts, nodeOptions, results) {
    const { BranchClass, BranchEntryClass } = opts
    let distance = 0
    for (const [i, [entry, keys]] of results) {
      const p = this.getNode(await entry.address)
        .then(node => node.transaction(keys.reverse(), { ...opts, sorted: true }))
        .then(r => ({ entry, keys, distance, ...r }))
      results.set(i, p)
    }
    let entries = [...this.entryList.entries]
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
    const { newEntries, prepend } = await this.handlePrepend(entries, opts, nodeOptions, final, distance)
    if (prepend) {
      newEntries.push(prepend)
    }
    distance++
    const toEntry = async branch => {
      if (branch.isEntry) return branch
      const block = await branch.encode()
      final.blocks.push(block)
      this.cache.set(branch)
      return new BranchEntryClass(branch, opts)
    }
    entries = await Promise.all(newEntries.map(toEntry)) // .sort(({ key: a }, { key: b }) => opts.compare(a, b))
    const _opts = { ...nodeOptions, entries, NodeClass: BranchClass, distance }
    return { nodes: await Node.from(_opts), ...final, distance }
  }

  async handlePrepend (entries, opts, nodeOptions, final, distance) {
    const { BranchClass, LeafClass } = opts
    let newEntries = []
    let prepend = null
    for (const entry of entries) {
      if (prepend) {
        const mergeEntries = await this.mergeFirstLeftEntries(entry, prepend, nodeOptions, final, distance)
        prepend = null
        const NodeClass = !mergeEntries[0].address ? LeafClass : BranchClass
        const _opts = {
          ...nodeOptions,
          entries: mergeEntries.sort(({ key: a }, { key: b }) => opts.compare(a, b)),
          NodeClass,
          distance: distance // TODO: is this right?
        }
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
    return { newEntries, prepend }
  }

  async mergeFirstLeftEntries (entry, prepend, nodeOptions, final, distance) {
    const opts = nodeOptions.opts
    const { LeafClass, BranchClass, BranchEntryClass } = opts
    if (entry.isEntry) { entry = await this.getNode(await entry.address) }
    const es = entry.entryList.entries
    if (es[0].constructor.name === prepend.entryList.entries[0].constructor.name) {
      return prepend.entryList.entries.concat(entry.entryList.entries)
    } else {
      const leftEntry = es.shift()
      /* c8 ignore next */
      if (!leftEntry) throw new Error('unreachable no left entry')
      /* c8 ignore next */
      if (!leftEntry.address) throw new Error('unreachable existing leaf, no leftEntry.address')
      const mergeLeftEntries = await this.mergeFirstLeftEntries(leftEntry, prepend, nodeOptions, final, distance - 1)
      const esf = es.shift()
      if (!esf) {
        return mergeLeftEntries
      }
      if (!esf.address) {
        throw new Error('unreachable existing leaf, no esf.address')
      }
      console.log('getNode esf', await esf.address)
      try {
        const oldFront = await this.getNode(await esf.address)
        if (!oldFront.entryList.entries[0].address) {
          return mergeLeftEntries.concat(oldFront.entryList.entries)
        } else {
          let mergeLeftNodes
          if (mergeLeftEntries[0].address) {
            mergeLeftNodes = await Node.from({
              ...nodeOptions,
              entries: mergeLeftEntries.sort(({ key: a }, { key: b }) => opts.compare(a, b)),
              NodeClass: BranchClass,
              distance
            })
          } else {
            mergeLeftNodes = await Node.from({
              ...nodeOptions,
              entries: mergeLeftEntries.sort(({ key: a }, { key: b }) => opts.compare(a, b)),
              NodeClass: LeafClass,
              distance
            })
          }
          const mergeLeftBranchEntries = await Promise.all(mergeLeftNodes.map(async l => {
            final.blocks.push(await l.encode())
            this.cache.set(l)
            return new BranchEntryClass({ key: l.key, address: await l.address }, opts)
          }))
          const newFirstNodes = await Node.from({
            ...nodeOptions,
            entries: [...oldFront.entryList.entries, ...mergeLeftBranchEntries].sort(({ key: a }, { key: b }) => opts.compare(a, b)),
            NodeClass: BranchClass,
            distance
          })
          await Promise.all(newFirstNodes.map(async l => {
            final.blocks.push(await l.encode())
            this.cache.set(l)
          }))
          const newBranchEntries = await Promise.all(newFirstNodes.map(async (l) => {
            return new BranchEntryClass({ key: l.key, address: await l.address }, opts)
          }))
          return newBranchEntries
        }
      } catch (err) {
        console.log('err', err)
        return mergeLeftEntries
      }
    }
  }

  async bulk (bulk, opts = {}, isRoot = true) {
    const { BranchClass } = opts
    opts = {
      codec: this.codec,
      hasher: this.hasher,
      getNode: this.getNode,
      compare: this.compare,
      cache: this.cache,
      ...opts
    }

    if (!opts.sorted) {
      bulk = sortBulk(bulk, opts)
      opts.sorted = true
    }

    const nodeOptions = { chunker: this.chunker, opts }

    const results = await this.transaction(bulk, opts)
    while (results.nodes.length > 1) {
      const newDistance = results.nodes[0].distance + 1

      const branchEntries = await Promise.all(
        results.nodes.map(async node => {
          const block = await node.encode()
          results.blocks.push(block)
          this.cache.set(node)
          return new opts.BranchEntryClass(node, opts)
        })
      )

      const newNodes = await Node.from({
        ...nodeOptions,
        entries: branchEntries,
        NodeClass: BranchClass,
        distance: newDistance
      })

      await Promise.all(
        newNodes.map(async node => {
          const block = await node.encode()
          this.cache.set(node)
          results.blocks.push(block)
        })
      )

      results.nodes = newNodes
    }

    const [root] = results.nodes
    results.root = root

    if (isRoot) {
      await processRoot(this, results, bulk, nodeOptions)
    }

    return results
  }

  static async from ({ entries, chunker, NodeClass, distance, opts }) {
    /* c8 ignore next */
    if (!entries.every(entry => entry.constructor.name === entries[0].constructor.name)) throw new Error('all entries must be of the same type')
    const parts = []
    let chunk = []
    for (const entry of entries) {
      chunk.push(entry)
      if (await chunker(entry, distance)) {
        parts.push(new EntryList({ entries: chunk, closed: true }))
        chunk = []
      }
    }
    if (chunk.length) {
      parts.push(new EntryList({ entries: chunk, closed: false }))
    }
    // console.log('node.from', JSON.stringify(parts.map(p => p.entries.map(e => [e.constructor.name, e.key]))))
    return parts.map(entryList => new NodeClass({ entryList, chunker, distance, ...opts }))
  }
}

class IPLDNode extends Node {
  constructor ({ codec, hasher, block, ...opts }) {
    super(opts)
    this.codec = codec
    this.hasher = hasher
    if (!block) {
      this.block = this.encode()
      this.address = this.block.then(block => block.cid)
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
    if (this.block) return this.block
    const value = await this.encodeNode()
    const opts = { codec: this.codec, hasher: this.hasher, value }
    this.block = await multiformatEncode(opts)
    return this.block
  }
}

class IPLDBranch extends IPLDNode {
  async encodeNode () {
    const { entries } = this.entryList
    const mapper = async entry => {
      /* c8 ignore next */
      if (!entry.address) throw new Error('entry.address required')
      return [entry.key, await entry.address]
    }
    const list = await Promise.all(entries.map(mapper))
    return { branch: [this.distance, list], closed: this.closed }
  }

  get isBranch () {
    return true
  }
}

class IPLDLeaf extends IPLDNode {
  async encodeNode () {
    const list = await Promise.all(this.entryList.entries.map(async entry => await entry.encodeNode()))
    return { leaf: list, closed: this.closed }
  }

  get isLeaf () {
    return true
  }
}

const create = async function * (obj) {
  let { LeafClass, LeafEntryClass, BranchClass, BranchEntryClass, list, chunker, compare, ...opts } = obj
  list = list.map((value) => new LeafEntryClass(value, opts))
  opts.compare = compare
  let nodes = await Node.from({ entries: list, chunker, NodeClass: LeafClass, distance: 0, opts })
  yield * nodes
  let distance = 1
  while (nodes.length > 1) {
    const mapper = async node => new BranchEntryClass({ key: node.key, address: await node.address }, opts)
    const entries = await Promise.all(nodes.map(mapper))
    nodes = await Node.from({ entries, chunker, NodeClass: BranchClass, distance, opts })
    yield * nodes
    distance++
  }
}

export { Node, Entry, EntryList, IPLDNode, IPLDLeaf, IPLDBranch, create }
