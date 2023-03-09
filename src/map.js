import { Entry, EntryList, IPLDLeaf, IPLDBranch, create as baseCreate } from './base.js'
import { readUInt32LE } from './utils.js'

class MapEntry extends Entry {
  async identity () {
    const encoded = await this.codec.encode(await this.encodeNode())
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
  async encodeNode () {
    return [this.key, await this.address]
  }
}

const getValue = async (node, key) => {
  const { result: entry, cids } = await node.getEntry(key)
  return { result: entry.value, cids }
}

const getManyValues = async (node, keys) => {
  const { result: entries, cids } = await node.getEntries(keys)
  return { result: entries.map(entry => entry.value), cids }
}

class MapLeaf extends IPLDLeaf {
  get (key) {
    return getValue(this, key)
  }

  getMany (keys) {
    return getManyValues(this, keys)
  }

  bulk (bulk, opts = {}, isRoot = true) {
    return super.bulk(bulk, { ...classes, ...opts }, isRoot)
  }
}
class MapBranch extends IPLDBranch {
  get (key) {
    return getValue(this, key)
  }

  getMany (keys) {
    return getManyValues(this, keys)
  }

  bulk (bulk, opts = {}, isRoot = true) {
    return super.bulk(bulk, { ...classes, ...opts }, isRoot)
  }
}

const classes = {
  LeafClass: MapLeaf,
  LeafEntryClass: MapLeafEntry,
  BranchClass: MapBranch,
  BranchEntryClass: MapBranchEntry
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
    } /* c8 ignore next */ else {
      /* c8 ignore next */
      throw new Error('Unknown block data, does not match schema')
      /* c8 ignore next */
    }
    const entryList = new EntryList({ entries, closed: value.closed })
    const node = new CLS({ entryList, ...opts })
    cache.set(node)
    return node
  }
  const getNode = async cid => {
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

const load = ({ cid, get, cache, chunker, codec, hasher, compare, ...opts }) => {
  const getNode = createGetNode(get, cache, chunker, codec, hasher, compare, opts)
  return getNode(cid)
}

export { create, load, MapLeaf, MapBranch, MapLeafEntry, MapBranchEntry }
