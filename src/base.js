import { encode } from 'multiformats/block'

class Entry {
  constructor ({ key, address }, opts = {}) {
    this.key = key
    this.address = address
    this.codec = opts.codec
    this.hasher = opts.hasher
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

  findMany (keys, compare, sorted = false) {
    const { entries } = this
    const results = new Map()
    if (!sorted) keys = keys.sort(compare)
    else keys = [...keys]
    for (let i = entries.length - 1; i > -1; i--) {
      if (!keys.length) break
      const entry = entries[i]
      const found = []
      while (keys.length) {
        const key = keys[keys.length - 1]
        const comp = compare(key, entry.key)
        if (comp > -1) {
          keys.pop()
          found.push(entry)
        } else {
          break
        }
      }
      if (found.length) {
        results.set(i, found)
      }
    }
    return results
  }

  findRange (start, end, compare) {
    const { entries } = this
    const result = { first: null, last: null }
    for (let i = entries.length - 1; i > -1; i--) {
      const entry = entries[i]
      let comp
      if (result.last === null) {
        comp = compare(end, entry.key)
        if (comp > -1) {
          result.last = i
        }
      }
      if (result.last !== null && result.first === null) {
        comp = compare(start, entry.key)
        if (comp > 0) {
          result.first = i + 1
        }
      }
    }
    if (result.first === null) result.first = 0
    result.entries = entries.slice(result.first, result.last + 1)
    return result
  }
}

class Node {
  constructor ({ entryList, chunker, distance, getNode, compare }) {
    this.entryList = entryList
    this.chunker = chunker
    this.distance = distance
    this.getNode = getNode
    this.compare = compare
  }

  get closed () {
    return this.entryList.closed
  }

  get key () {
    return this.entryList.startKey
  }

  async getEntry (key) {
    let node = this
    while (!node.isLeaf) {
      const result = node.entryList.find(key, this.compare)
      if (result === null) throw new Error('Not found')
      const [, entry] = result
      node = await this.getNode(await entry.address)
    }
    const result = node.entryList.find(key, this.compare)
    if (result === null) throw new Error('Not found')
    const [, entry] = result
    return entry
  }

  static async from ({ entries, chunker, NodeClass, distance, opts }) {
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
    const entry = await this.getEntry(key)
    return entry.key
  }

  async encode () {
    if (this.block) return this.block
    const value = await this.encodeNode()
    const opts = { codec: this.codec, hasher: this.hasher, value }
    this.block = encode(opts)
    return this.block
  }
}

class IPLDBranch extends IPLDNode {
  async encodeNode () {
    const { entries } = this.entryList
    const mapper = async entry => [entry.key, await entry.address]
    const list = await Promise.all(entries.map(mapper))
    return { branch: [this.distance, list], closed: this.closed }
  }

  get isBranch () {
    return true
  }
}

class IPLDLeaf extends IPLDNode {
  encodeNode () {
    const list = this.entryList.entries.map(entry => entry.encodeNode())
    return { leaf: list, closed: this.closed }
  }

  get isLeaf () {
    return true
  }
}

const create = async function * (obj) {
  let {
    LeafClass,
    LeafEntryClass,
    BranchClass,
    BranchEntryClass,
    list,
    chunker,
    compare,
    ...opts
  } = obj
  list = list.map(value => new LeafEntryClass(value, opts))
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
