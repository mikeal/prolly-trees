import { create as mapCreate, load as mapLoad, MapLeaf, MapBranch } from './map.js'
import { simpleCompare } from './utils.js'

const compare = simpleCompare

class SparseArrayLeaf extends MapLeaf {
}

class SparseArrayBranch extends MapBranch {
}

const LeafClass = SparseArrayLeaf
const BranchClass = SparseArrayBranch

const defaults = { LeafClass, BranchClass, compare }

const create = opts => {
  opts = { ...defaults, ...opts }
  return mapCreate(opts)
}

const load = opts => {
  opts = { ...defaults, ...opts }
  return mapLoad(opts)
}

export { create, load, SparseArrayBranch, SparseArrayLeaf }
