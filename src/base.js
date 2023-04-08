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
            break
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
      results.blocks.push({ block, node })
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
  results.blocks.push({ block: await root.encode(), node: root })

  that.cache.set(root)
  const opts = nodeOptions.opts
  const distance = root.distance
  const first = root.entryList.startKey
  const inserts = await filterLeftmostInserts(first, bulk, that.compare)
  if (inserts.length) {
    const newLeaves = await generateNewLeaves(inserts, opts, that)
    const branchEntries = await generateBranchEntries(that, newLeaves, results, opts)

    const firstRootEntry = new opts.BranchEntryClass({ key: root.entryList.startKey, address: await root.address }, opts)
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
      results.blocks.push({ block, node: m })
    }))
    results.root = newBranches[0]
    results.nodes = [...results.nodes, ...allBranches]
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
      if (node.isLeaf) {
        const entries = node.entryList.entries.map((e) => `[${e.key},${JSON.stringify(e.value).replace(/"/g, "'")}]`).join(', ')
        return `Leaf [${entries}]`
      } else {
        const entries = node.entryList.entries.map((e) => `[${e.key}]`).join(', ')
        return `Branch [${entries}]`
      }
    }
    const shortCid = (cid) => cid.toString().slice(0, 4) + cid.toString().slice(-4)
    const visit = async function * (node, parentId, cids) {
      const nodeId = (await node.address)// .toString()
      if (!cids.has(nodeId)) {
        cids.add(nodeId)
        const nodeLabel = await renderNodeLabel(node)
        yield `  node [shape=ellipse fontname="Courier"]; ${shortCid(nodeId)} [label="${nodeLabel}"];`
        yield `  ${shortCid(parentId)} -> ${shortCid(nodeId)};`

        for (const entry of node.entryList.entries) {
          if (entry.address) {
            const entryId = (await entry.address)// .toString()
            try {
              const childNode = await node.getNode(entryId)
              yield * await visit(childNode, nodeId, cids)
              /* c8 ignore next */
            } catch (err) {
              /* c8 ignore next */
              yield `  ${shortCid(nodeId)} -> ${shortCid(entryId)};`
              /* c8 ignore next */
              yield `  node [shape=ellipse fontname="Courier"]; ${shortCid(entryId)} [label="Error: ${err.message}"];`
              /* c8 ignore next */
            }
          }
        }
      }
    }
    yield 'digraph tree {'
    yield '  node [shape=ellipse fontname="Courier"]; rootnode;'
    for await (const line of visit(this, 'rootnode', cids)) {
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
    // console.log('transaction', bulk.map(({ key }) => key), 'results', JSON.stringify([...results.values()].map((entry) => [entry[0].key, entry[1].map(({ key }) => key)])))
    if (this.isLeaf) {
      const newLeaf = await this.transactionLeaf(bulk, opts, nodeOptions, results)
      if (newLeaf) return newLeaf
      // newLeaf is null if everything in it got deleted, we need to call transactionBranch without 'this', on our parents
      // return await this.transactionBranch(bulk, opts, nodeOptions, results)
    } else {
      return await this.transactionBranch(bulk, opts, nodeOptions, results)
    }
  }

  async transactionLeaf (bulk, opts, nodeOptions, results) {
    const { LeafClass, LeafEntryClass } = opts
    const { entries, previous } = this.processLeafEntries(bulk, results, LeafEntryClass, opts)
    // console.log('transactionLeaf', bulk.map(({ key }) => key), 'entries', JSON.stringify(entries.map(({ key }) => key)))
    if (!entries.length) return { previous, nodes: [], blocks: [], distance: 0 }

    const _opts = { ...nodeOptions, entries, NodeClass: LeafClass, distance: 0 }
    const nodes = await Node.from(_opts)
    // console.log('leaf nodes', JSON.stringify(await Promise.all(nodes.map(async n => [n.constructor.name, (await n.address).toString()]))))
    return {
      nodes,
      previous,
      blocks: await Promise.all(nodes.map(async (n) => {
        const block = await n.encode()
        this.cache.set(n)
        return { block, node: n }
      })),
      distance: 0
    }
  }

  processLeafEntries (bulk, results, LeafEntryClass, opts) {
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
      if (i !== null) entries.splice(i - count++, 1)
    }
    const appends = Object.values(changes).map(obj => new LeafEntryClass(obj, opts))
    // TODO: there's a faster version of this that only does one iteration
    entries = entries.concat(appends).sort(({ key: a }, { key: b }) => opts.compare(a, b))
    return { entries, previous }
  }

  // doesn't use bulk
  async transactionBranch (bulk, opts, nodeOptions, results) {
    const { BranchClass, BranchEntryClass } = opts
    let distance = 0
    for (const [i, [entry, keys]] of results) {
      // console.log('transactionBranch', entry.key, JSON.stringify(keys.map(({ key }) => key)))
      const p = this.getNode(await entry.address)
        .then(node => node.transaction(keys.reverse(), { ...opts, sorted: true }))
        .then(r => ({ entry, keys, distance, ...r }))
      results.set(i, p)
    }
    let entries = [...this.entryList.entries]
    const final = { previous: [], blocks: [], nodes: [] } // type of results is a map not our return value
    for (const [i, p] of results) {
      const { nodes, previous, blocks, distance: _distance } = await p
      // if (nodes.length === 0) {
      //   throw new Error('transactionBranch: nodes.length === 0')
      // }
      distance = _distance
      entries[i] = nodes
      if (previous.length) final.previous = final.previous.concat(previous)
      if (blocks.length) final.blocks = final.blocks.concat(blocks)
      if (nodes.length) final.nodes = final.nodes.concat(nodes)
    }
    entries = entries.flat()
    const newEntries = await this.handlePrepend(entries, opts, nodeOptions, final, distance)

    distance++
    const toEntry = async branch => {
      if (branch.isEntry) return branch
      const block = await branch.encode()
      final.blocks.push({ block, node: branch })
      this.cache.set(branch)
      return new BranchEntryClass(branch, opts)
    }
    // console.log('transactionBranch newEntries', JSON.stringify(newEntries.map((e) => [e.key, e.constructor.name])))
    entries = await Promise.all(newEntries.map(toEntry)) // .sort(({ key: a }, { key: b }) => opts.compare(a, b))
    const _opts = { ...nodeOptions, entries, NodeClass: BranchClass, distance }

    const newNodes = await Node.from(_opts) // stomp on previous nodes
    await Promise.all(newNodes.map(async n => {
      const block = await n.encode()
      final.blocks.push({ block, node: n })
      this.cache.set(n)
    }))
    // final.nodes = final.nodes.concat(newNodes)
    // final.nodes = newNodes.concat(final.nodes)
    // final.nodes = [...newNodes, ...beforePrependNodes]
    final.nodes = newNodes
    return { ...final, distance }
  }

  async handlePrepend (entries, opts, nodeOptions, final, distance) {
    const { BranchClass, LeafClass } = opts
    let newEntries = []
    let prepend = null
    // console.log('handlePrepend', entries.map(e => e.key))
    for (const entry of entries) {
      if (prepend) {
        // console.log('do mergeFirstLeftEntries prepend', prepend.key, prepend.constructor.name, JSON.stringify(prepend.entryList?.entries.map((n) => [n.key, n.constructor.name])))
        // console.log('do mergeFirstLeftEntries entry', entry.key, entry.constructor.name, JSON.stringify((await this.getNodeFirstFromBlocks(final.blocks, await entry.address)).entryList?.entries.map((n) => [n.key, n.constructor.name])))
        const mergeEntries = await this.mergeFirstLeftEntries(entry, prepend, nodeOptions, final, distance)
        prepend = null
        const NodeClass = !mergeEntries[0].address ? LeafClass : BranchClass
        const _opts = {
          ...nodeOptions,
          entries: mergeEntries.sort(({ key: a }, { key: b }) => opts.compare(a, b)),
          NodeClass,
          distance: distance
        }
        const nodes = await Node.from(_opts)
        // console.log('nodes to pop', nodes.map((n) => [n.key, n.constructor.name, JSON.stringify(n.entryList?.entries.map((e) => [e.key, e.constructor.name]))]))
        if (!nodes[nodes.length - 1].closed) {
          prepend = nodes.pop()
        }
        if (nodes.length) {
          newEntries = newEntries.concat(nodes)
        }
      } else {
        if (!entry.isEntry && !entry.closed) {
          // console.log('else new prepend', entry.key, entry.constructor.name)
          prepend = entry
        } else {
          // console.log('else new entry', entry.key, entry.constructor.name)
          newEntries.push(entry)
        }
      }
    }
    // console.log('newEntries after loop', newEntries.map(e => e.key))
    if (prepend) {
      newEntries.push(prepend)
    }
    // console.log('did prepend', newEntries.map(e => e.key))
    return newEntries
  }

  async getNodeFirstFromBlocks (blocks, addr) {
    for (const { block, node } of blocks) {
      if (await block.cid === addr) return node
    }
    return await this.getNode(addr)
  }

  async mergeFirstLeftEntries (entry, prepend, nodeOptions, final, distance) {
    const opts = nodeOptions.opts
    const { LeafClass, BranchClass, BranchEntryClass } = opts
    // console.log('mergeFirstLeftEntries', await entry.address)
    if (entry.isEntry) {
      const addr = await entry.address
      entry = await this.getNodeFirstFromBlocks(final.blocks, addr)
    }
    const es = entry.entryList.entries
    // console.log('entry.entryList.entries', entry.constructor.name, entry.key, es.map(e => [e.key, e.constructor.name]))
    /* c8 ignore next */
    if (!es.length) throw new Error('unreachable no entries')

    const basicMerge = (entries1, entries2) => {
      // console.log('basicMerge', entries1.concat(entries2).map(e => [e.key, e.constructor.name]))
      return entries1.concat(entries2)
    }

    const processNodesAndCreateEntries = async (nodes, final, opts) => {
      return await Promise.all(
        nodes.map(async (l) => {
          final.blocks.push({ block: await l.encode(), node: l })
          this.cache.set(l)
          return new BranchEntryClass(
            { key: l.key, address: await l.address },
            opts
          )
        })
      )
    }

    if (es[0].constructor.name === prepend.entryList.entries[0].constructor.name) {
      return await basicMerge(prepend.entryList.entries, es)
    } else {
      const leftEntry = es.shift()
      /* c8 ignore next */
      if (!leftEntry) throw new Error('unreachable no left entry')
      /* c8 ignore next */
      if (!leftEntry.address) throw new Error('unreachable existing leaf, no leftEntry.address')
      // maybe should be while es.length
      const mergeLeftEntries = await this.mergeFirstLeftEntries(
        leftEntry,
        prepend,
        nodeOptions,
        final,
        distance - 1
      )
      // console.log('Merged entries:', mergeLeftEntries.map(e => [e.key, e.constructor.name]))

      const esf = es.shift()
      // console.log('esf remainder', es.map(e => [e.key, e.constructor.name]))

      if (!esf) {
        return mergeLeftEntries
      }
      /* c8 ignore next */
      if (!esf.address) throw new Error('unreachable existing leaf, no esf.address')

      const oldFront = await this.getNodeFirstFromBlocks(final.blocks, await esf.address)

      if (!oldFront.entryList.entries[0].address) {
        // console.log('basicOldFront', mergeLeftEntries.concat(oldFront.entryList.entries).map(e => [e.key, e.constructor.name]))
        const leftLeafEntries = await basicMerge(mergeLeftEntries, oldFront.entryList.entries)

        const leftLeafNodes = await Node.from({
          ...nodeOptions,
          entries: leftLeafEntries.sort(({ key: a }, { key: b }) =>
            opts.compare(a, b)
          ),
          NodeClass: LeafClass,
          distance
        })

        const leftBranches = await processNodesAndCreateEntries(
          leftLeafNodes,
          final,
          opts
        )

        return await basicMerge(leftBranches, es)
      } else {
        // console.log('es remainder', es.map(e => [e.key, e.constructor.name]))
        // console.log('oldFront.entryList.entries:', oldFront.entryList.entries.map(n => [n.key, n.constructor.name]))
        if (mergeLeftEntries[0].address) {
          // throw new Error('unreachable left branch')
          // console.log('mergeLeftEntries:', mergeLeftEntries.map(n => [n.key, n.constructor.name]))
          return mergeLeftEntries.concat(oldFront.entryList.entries)
        } else {
          const mergeLeftNodes = await Node.from({
            ...nodeOptions,
            entries: mergeLeftEntries.sort(({ key: a }, { key: b }) =>
              opts.compare(a, b)
            ),
            NodeClass: LeafClass,
            distance
          })
          // console.log('mergeLeftNodes:', mergeLeftNodes.map(n => [n.key, n.constructor.name]))

          const mergeLeftBranchEntries = await processNodesAndCreateEntries(
            mergeLeftNodes,
            final,
            opts
          )
          // console.log('newFirstNodes remainder', es.map(e => [e.key, e.constructor.name]))

          const newFirstNodes = await Node.from({
            ...nodeOptions,
            entries: [
              ...oldFront.entryList.entries, // what about es?
              ...mergeLeftBranchEntries,
              ...es
            ].sort(({ key: a }, { key: b }) => opts.compare(a, b)),
            NodeClass: BranchClass,
            distance
          })
          // console.log('newFirstNodes:', newFirstNodes.map(n => [n.key, n.constructor.name]))

          const newBranchEntries = await processNodesAndCreateEntries(
            newFirstNodes,
            final,
            opts
          )
          // console.log('newBranchEntries:', newBranchEntries.map(e => [e.key, e.constructor.name]))

          return newBranchEntries
        }
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
    // console.log('results.nodes', results.nodes.length)
    while (results.nodes.length > 1) {
      const newDistance = results.nodes[0].distance + 1

      const branchEntries = await Promise.all(
        results.nodes.map(async node => {
          const block = await node.encode()
          results.blocks.push({ block, node })
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
          results.blocks.push({ block, node })
        })
      )

      results.nodes = newNodes
    }
    results.root = results.nodes[0]

    if (isRoot && results.root) {
      await processRoot(this, results, bulk, nodeOptions)
    }
    results.blocks = results.blocks.map(({ block }) => block)
    return results
  }

  static async from ({ entries, chunker, NodeClass, distance, opts }) {
    // console.log('node.from entries', entries.length, entries[0].constructor.name, entries.map(e => e.key))
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
