import { create as mapCreate, MapLeaf, MapBranch } from './map.js'
import { simpleCompare } from './utils.js'

const compare = (a, b) => {
  const [ aKey, aRef ] = a
  const [ bKey, bRef ] = b
  let comp = simpleCompare(aKey, bKey)
  if (comp !== 0) return comp
  return simpleCompare(aRef, bRef)
}

const getIndex = async (node, key) => {
  const start = [ key, 0 ]
  const end = [ key, Infinity ]
  const entries = await node.getRangeEntries(start, end)
  return entries.map(entry => ({ id: entry.key[1], row: entry.value }))
}

const getRange = async (node, start, end) => {
  start = [ start, 0]
  end = [ end, Infinity ]
  const entries = await node.getRangeEntries(start, end)
  return entries.map(entry => {
    const [ id, key ] = entry.key
    return { id, key, row: entry.value }
  })
}

class DBIndexLeaf extends MapLeaf {
  get (key) {
    return getIndex(this, key)
  }
  range (start, end) {
    return getRange(this, start, end)
  }
}

class DBIndexBranch extends MapBranch {
  get (key) {
    return getIndex(this, key)
  }
  range (start, end) {
    return getRange(this, start, end)
  }
}

const LeafClass = DBIndexLeaf
const BranchClass = DBIndexBranch

const create = (opts) => {
  opts = { ...opts, LeafClass, BranchClass, compare }
  return mapCreate(opts)
}

export { create, DBIndexBranch, DBIndexLeaf }
