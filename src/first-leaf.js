import { Node } from './base.js'

export async function newInsertsBulker (that, inserts, nodeOptions, distance, root, results) {
  const opts = nodeOptions.opts
  const newLeaves = await createNewLeaves(that, inserts, opts)

  if (newLeaves.length === 0) {
    throw new Error('Failed to insert entries')
  }

  const branchEntries = await Promise.all(
    newLeaves.map(async (node) => {
      console.log('MapBranchEntry for LeafNode:', node.constructor.name, node.key, await node.address)
      const newBranchEntry = new opts.BranchEntryClass({ key: node.key, address: await node.address }, opts)
      return newBranchEntry
    })
  )

  const firstRootEntry = new opts.BranchEntryClass(
    {
      key: root.entryList.startKey,
      address: await root.address
    },
    opts
  )
  // these should be sorted by compare function
  const newBranchEntries = [firstRootEntry, ...branchEntries].sort(({ key: a }, { key: b }) => opts.compare(a, b))

  console.log(
    'newBranchEntries:',
    newBranchEntries.map((ent) => {
      const { key, address, value } = ent
      const out = { key, address, value }
      out.cls = ent.constructor.name
      return out
    })
  )
  const newBranches = await Node.from({
    ...nodeOptions,
    entries: newBranchEntries,
    chunker: that.chunker,
    NodeClass: opts.BranchClass,
    distance: distance + 1,
    opts
  })

  const newNodes = [...newLeaves, ...newBranches, root]
  const newBlocks = await Promise.all(newNodes.map(async (m) => await m.block))

  results.root = newBranches[0]
  results.blocks = [...results.blocks, ...newBlocks]
  results.nodes = newNodes
}

/**
 * Creates new leaf entries from an array of insert objects.
 * @param {*} that - {compare, codec, hasher, chunker} - The value of the this keyword passed from the caller.
 * @param {Array} inserts - An array of insert objects to create new leaf entries from.
 * @param {Object} opts - {codec, hasher, cache, sorted, ...} - An object containing options for creating new leaf entries.
 * @returns {Promise<Array>} An array of nodes created from the leaf entries.
 **/
async function createNewLeaves (that, inserts, opts) {
  const entries = inserts
    .map((insert) => new opts.LeafEntryClass(insert, opts))
    .sort((a, b) => that.compare(a.key, b.key))

  return await Node.from({
    entries,
    chunker: that.chunker,
    NodeClass: opts.LeafClass,
    distance: 0,
    opts
  })
}
