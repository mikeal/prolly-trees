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
}

const findEntry = (key, node) => {
  const { entries } = node.entryList
  for (let i = entries.length - 1; i > -1; i--) {
    const entry = entries[i]
    const comp = node.compare(key, entry.key)
    if (comp > -1) {
      return entry
    }
  }
  throw new Error('key is out of bounds')
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
      const entry = findEntry(key, node)
      node = await this.getNode(await entry.address)
    }
    return findEntry(key, node)
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
