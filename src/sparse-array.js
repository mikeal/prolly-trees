import {
  create as mapCreate, load as mapLoad, MapLeaf, MapBranch,
  MapLeafEntry as LeafEntryClass, MapBranchEntry as BranchEntryClass
} from './map.js'
import { simpleCompare } from './utils.js'

const compare = simpleCompare

const getLength = async node => {
  while (!node.isLeaf) {
    const { entries } = node.entryList
    const last = entries[entries.length - 1]
    node = await node.getNode(await last.address)
  }
  const { entries } = node.entryList
  const last = entries[entries.length - 1]
  return last.key + 1
}

class SparseArrayLeaf extends MapLeaf {
  bulk (bulk, opts = {}) {
    return super.bulk(bulk, { ...classes, ...opts })
  }

  getLength () {
    return getLength(this)
  }
}

class SparseArrayBranch extends MapBranch {
  bulk (bulk, opts = {}) {
    return super.bulk(bulk, { ...classes, ...opts })
  }

  getLength () {
    return getLength(this)
  }
}

const LeafClass = SparseArrayLeaf
const BranchClass = SparseArrayBranch

const classes = { LeafClass, BranchClass, LeafEntryClass, BranchEntryClass }

const defaults = { ...classes, compare }

const create = opts => {
  opts = { ...defaults, ...opts }
  return mapCreate(opts)
}

const load = opts => {
  opts = { ...defaults, ...opts }
  return mapLoad(opts)
}

export { create, load, SparseArrayBranch, SparseArrayLeaf }
