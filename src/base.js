
class Entry {
  constructor ({ key, address }) {
    this.key = key
    this.address = address
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

const create = async function * (obj) {
  let {
    LeafNodeClass,
    LeafEntryClass,
    BranchNodeClass,
    BranchEntryClass,
    list,
    chunker,
    sorted,
    compare,
    ...opts } = obj
  if (!sorted) list = list.sort(compare)
  list = list.map(value => new LeafEntryClass(value))
  opts.compare = compare
  let nodes = await Node.from({ entries: list, chunker, NodeClass: LeafNodeClass, distance: 0, opts })
  yield * nodes
  let distance = 1
  while (nodes.length > 1) {
    const entries = nodes.map(node => new BranchEntryClass(node))
    nodes = await Node.from({ entries, chunker, NodeClass: BranchNodeClass, distance, opts })
    yield * nodes
    distance++
  }
}

export { Node, Entry, EntryList, create }
