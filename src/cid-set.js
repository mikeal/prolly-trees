import { Node, Entry, create as baseCreate } from './base.js'
import { encode } from 'multiformats/block'
import { readUInt32LE } from './utils.js'

class CIDEntry extends Entry {
  constructor (cid) {
    super({ address: cid, key: cid })
    this.cid = cid
  }

  encode () {
    return this.cid
  }

  identity () {
    const buffer = this.cid.multihash.bytes
    return readUInt32LE(buffer)
  }
}

class CIDBranchEntry extends Entry {
  constructor (node) {
    super(node)
  }
  async identity () {
    const { multihash: { bytes } } = await this.address
    return readUInt32LE(bytes)
  }
}

class CIDSetNode extends Node {
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

  async encode () {
    if (this.block) return this.block
    const value = await this.encodeNode()
    const opts = { codec: this.codec, hasher: this.hasher, value }
    this.block = encode(opts)
    return this.block
  }
}

class CIDSetBranch extends CIDSetNode {
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

class CIDSetLeaf extends CIDSetNode {
  encodeNode () {
    const list = this.entryList.entries.map(entry => entry.key)
    return { leaf: list, closed: this.closed }
  }

  get isLeaf () {
    return true
  }
}

const createGetNode = (get, cache, chunker, codec, hasher) => {
  const decoder = block => {
    const { value } = block
    const opts = { closed: value.closed, chunker, cache, block, getNode }
    let node
    if (value.leaf) {
      node = new CIDSetLeaf({ entries: value.leaf, ...opts })
    } else if (value.branch) {
      node = new CIDSetBranch({ entries: value.branch, ...opts })
    } else {
      throw new Error('Unknown block data, does not match schema')
    }
    cache.set(block.cid, node)
    return node
  }
  const getNode = cid => {
    if (cache.has(cid)) return cache.get(cid)
    return get(cid).then(block => decoder(block))
  }
  return getNode
}

const create = ({ get, cache, chunker, list, codec, hasher }) => {
  const getNode = createGetNode(get, cache, chunker, codec, hasher)
  const opts = {
    list,
    codec,
    hasher,
    chunker,
    getNode,
    cache,
    LeafNodeClass: CIDSetLeaf,
    LeafEntryClass: CIDEntry,
    BranchNodeClass: CIDSetBranch,
    BranchEntryClass: CIDBranchEntry
  }
  return baseCreate(opts)
}

export { create }
