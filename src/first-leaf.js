import { Node } from './base.js'

export async function newInsertsBulker (that, inserts, nodeOptions, distance, root, results) {
  // const callID = 'cx.' + Math.random().toString(36).substring(2, 15)
  const opts = nodeOptions.opts
  console.log('crate new leaves', inserts)
  const newLeaves = await createNewLeaves(that, inserts, opts)

  const newLBlocks = await Promise.all(newLeaves.map(async (m) => await m.block))

  console.log('newLBlocks:', newLBlocks.length, newLBlocks.map((m) => m.cid))

  if (newLeaves.length === 0) {
    throw new Error('Failed to insert entries')
  }

  const branchEntries = await Promise.all(
    newLeaves.map(async (node) => new opts.BranchEntryClass({ key: node.key, address: await node.address }, opts))
  )

  const firstRootEntry = new opts.BranchEntryClass(
    {
      key: root.entryList.startKey,
      address: await root.address
    },
    opts
  )

  const newRootEntries = [firstRootEntry, ...branchEntries]

  // console.log('newRootEntries:', await Promise.all(newRootEntries.map(async (m) => await m.address)))

  const newBranches = await Node.from({
    ...nodeOptions,
    entries: newRootEntries,
    chunker: that.chunker,
    NodeClass: opts.BranchClass,
    distance: distance + 1,
    opts
  })

  const newNodes = [...newLeaves, ...newBranches]

  const newBlocks = await Promise.all(newNodes.map(async (m) => await m.block))
  console.log('newBlocks:', newBlocks.length, newBlocks.map((m) => m.cid))
  console.log('results.blocks:', results.blocks.length, results.blocks.map((m) => m.cid))

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
