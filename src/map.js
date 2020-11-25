import { Entry, EntryList, IPLDLeaf, IPLDBranch, create as baseCreate } from './base.js'
import { readUInt32LE } from './utils.js'

class MapEntry extends Entry {
  async identity () {
    const encoded = this.codec.encode(this.encode())
    const hash = await this.hasher.encode(encoded)
    return readUInt32LE(hash)
  }
}

class MapLeafEntry extends MapEntry {
  constructor (node) {
    super(node)
    this.value = node.value
  }

  encodeNode () {
    return [this.key, this.value]
  }
}

class MapBranchEntry extends MapEntry {
  encode () {
    return [this.key, this.address]
  }
}

class MapLeaf extends IPLDLeaf {
}
class MapBranch extends IPLDBranch {
}

const createGetNode = (get, cache, chunker, codec, hasher, compare) => {
  const decoder = block => {
    const { value } = block
    const opts = { chunker, cache, block, getNode, codec, hasher, compare }
    let entries
    let CLS
    if (value.leaf) {
      entries = value.leaf.map(([key, value]) => new MapLeafEntry({ key, value }))
      CLS = MapLeaf
    } else if (value.branch) {
      const [distance, _entries] = value.branch
      opts.distance = distance
      entries = _entries.map(([key, address]) => new MapBranchEntry({ key, address }))
      CLS = MapBranch
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

const create = ({ get, cache, chunker, list, codec, hasher, sorted, compare }) => {
  const getNode = createGetNode(get, cache, chunker, codec, hasher, compare)
  const opts = {
    list,
    codec,
    hasher,
    chunker,
    getNode,
    sorted,
    compare,
    cache,
    LeafClass: MapLeaf,
    LeafEntryClass: MapLeafEntry,
    BranchClass: MapBranch,
    BranchEntryClass: MapBranchEntry
  }
  return baseCreate(opts)
}

export { create }
