import { Entry, EntryList, IPLDLeaf, IPLDBranch, create as baseCreate } from './base.js'
import { readUInt32LE } from './utils.js'

class MapEntry extends Entry {
  async identity () {
    const encoded = this.codec.encode(this.encodeNode())
    const hash = await this.hasher.encode(encoded)
    return readUInt32LE(hash)
  }
}

class MapLeafEntry extends MapEntry {
  constructor (node, opts) {
    super(node, opts)
    this.value = node.value
  }

  encodeNode () {
    return [this.key, this.value]
  }
}

class MapBranchEntry extends MapEntry {
  encodeNode () {
    return [this.key, this.address]
  }
}

const getValue = async (node, key) => {
  const entry = await node.getEntry(key)
  return entry.value
}

const getManyValues = async (node, keys) => {
  const entries = await node.getEntries(keys)
  return entries.map(entry => entry.value)
}

class MapLeaf extends IPLDLeaf {
  get (key) {
    return getValue(this, key)
  }
  getMany (keys) {
    return getManyValues(this, keys)
  }
}
class MapBranch extends IPLDBranch {
  get (key) {
    return getValue(this, key)
  }
  getMany (keys) {
    return getManyValues(this, keys)
  }
}

const createGetNode = (get, cache, chunker, codec, hasher, compare, opts) => {
  const LeafClass = opts.LeafClass || MapLeaf
  const LeafEntryClass = opts.LeafEntryClass || MapLeafEntry
  const BranchClass = opts.BranchClass || MapBranch
  const BranchEntryClass = opts.BranchEntryClass || MapBranchEntry

  const entryOpts = { codec, hasher }

  const decoder = block => {
    const { value } = block
    const opts = { chunker, cache, block, getNode, codec, hasher, compare }
    let entries
    let CLS
    if (value.leaf) {
      entries = value.leaf.map(([key, value]) => new LeafEntryClass({ key, value }, entryOpts))
      CLS = LeafClass
    } else if (value.branch) {
      const [distance, _entries] = value.branch
      opts.distance = distance
      entries = _entries.map(([key, address]) => new BranchEntryClass({ key, address }, entryOpts))
      CLS = BranchClass
    } else {
      throw new Error('Unknown block data, does not match schema')
    }
    const entryList = new EntryList({ entries, closed: value.closed })
    const node = new CLS({ entryList, ...opts })
    cache.set(block.cid, node)
    return node
  }
  const getNode = cid => {
    if (cache.has(cid)) return cache.get(cid)
    return get(cid).then(block => decoder(block))
  }
  return getNode
}

const create = ({ get, cache, chunker, list, codec, hasher, sorted, compare, ...opts }) => {
  if (!sorted) list = list.sort(({ key: a }, { key: b }) => compare(a, b))
  const getNode = createGetNode(get, cache, chunker, codec, hasher, compare, opts)
  const _opts = {
    list,
    codec,
    hasher,
    chunker,
    getNode,
    sorted,
    compare,
    cache,
    LeafClass: opts.LeafClass || MapLeaf,
    LeafEntryClass: opts.LeafEntryClass || MapLeafEntry,
    BranchClass: opts.BranchClass || MapBranch,
    BranchEntryClass: opts.BranchEntryClass || MapBranchEntry
  }
  return baseCreate(_opts)
}

export { create, MapLeaf, MapBranch }
