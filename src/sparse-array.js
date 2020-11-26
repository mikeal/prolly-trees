import { create as mapCreate, MapLeaf, MapBranch } from './map.js'
import { simpleCompare } from './utils.js'

const compare = simpleCompare

class SparseArrayLeaf extends MapLeaf {
}

class SparseArrayBranch extends MapBranch {
}

const LeafClass = SparseArrayLeaf
const BranchClass = SparseArrayBranch

const create = (opts) => {
  opts = { ...opts, LeafClass, BranchClass, compare }
  return mapCreate(opts)
}

export { create, SparseArrayBranch, SparseArrayLeaf }
