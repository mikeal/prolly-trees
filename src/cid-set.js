import { Entry, EntryList, IPLDLeaf, IPLDBranch, create as baseCreate } from './base.js'
import { readUInt32LE, binaryCompare } from './utils.js'

const compare = ({ bytes: a }, { bytes: b }) => binaryCompare(a, b)

class CIDEntry extends Entry {
  constructor (cid) {
    super({ address: cid, key: cid })
    this.cid = cid
  }

  encodeNode () {
    return this.cid
  }

  identity () {
    const buffer = this.cid.multihash.bytes
    return readUInt32LE(buffer)
  }
}

class CIDNodeEntry extends Entry {
  async identity () {
    const { multihash: { bytes } } = await this.address
    return readUInt32LE(bytes)
  }
}

class CIDSetBranch extends IPLDBranch {
}

class CIDSetLeaf extends IPLDLeaf {
}

const createGetNode = (get, cache, chunker, codec, hasher) => {
  const decoder = block => {
    const { value } = block
    const opts = { chunker, cache, block, getNode, codec, hasher, compare }
    let entries
    let CLS
    if (value.leaf) {
      entries = value.leaf.map(cid => new CIDEntry(cid))
      CLS = CIDSetLeaf
    } else if (value.branch) {
      const [distance, _entries] = value.branch
      opts.distance = distance
      entries = _entries.map(([key, address]) => new CIDNodeEntry({ key, address }))
      CLS = CIDSetBranch
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
  const getNode = cid => {
    if (cache.has(cid)) return cache.get(cid)
    return get(cid).then(block => decoder(block))
  }
  return getNode
}

const create = ({ get, cache, chunker, list, codec, hasher, sorted }) => {
  if (!sorted) list = list.sort(compare)
  const getNode = createGetNode(get, cache, chunker, codec, hasher)
  const opts = {
    list,
    codec,
    hasher,
    chunker,
    getNode,
    sorted,
    compare,
    cache,
    LeafClass: CIDSetLeaf,
    LeafEntryClass: CIDEntry,
    BranchClass: CIDSetBranch,
    BranchEntryClass: CIDNodeEntry
  }
  return baseCreate(opts)
}

const load = ({ cid, get, cache, chunker, codec, hasher, ...opts }) => {
  const getNode = createGetNode(get, cache, chunker, codec, hasher, opts)
  return getNode(cid)
}

export { create, load }
