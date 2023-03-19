import { Node } from './base.js'

export async function newInsertsBulker (that, inserts, nodeOptions, distance, root, results) {
  const opts = nodeOptions.opts
  console.log('crate new leaves', inserts, await root.address)
  const newLeaves = await createNewLeaves(that, inserts, opts)

  const newLBlocks = await Promise.all(newLeaves.map(async (m) => await m.block))

  console.log('newLBlocks:', newLBlocks.length, newLBlocks.map((m) => m.cid))

  if (newLeaves.length === 0) {
    throw new Error('Failed to insert entries')
  }

  const branchEntries = await Promise.all(
    newLeaves.map(async (node) => {
      console.log('MapBranchEntry for LeafNode:', node.key, await node.address)
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

  const newBranchEntries = [firstRootEntry, ...branchEntries]

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
  const entries = []
  for (const insert of inserts) {
    const index = entries.findIndex((entry) => that.compare(entry.key, insert.key) > 0)
    const entry = new opts.LeafEntryClass(insert, opts)
    if (index >= 0) {
      entries.splice(index, 0, entry)
    } else {
      entries.push(entry)
    }
  }

  return await Node.from({
    entries,
    chunker: that.chunker,
    NodeClass: opts.LeafClass,
    distance: 0,
    opts
  })
}
