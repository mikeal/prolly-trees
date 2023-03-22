import { Node } from './base.js'

/**
 * Process new leftmost inserts and updates the results.
 *
 * @param {Object} that - The instance of the class that this function is called from.
 * @param {Array} inserts - An array of new insert objects to be processed.
 * @param {Object} nodeOptions - Options to be used when creating new nodes.
 * @param {number} distance - The distance parameter used in processing the inserts.
 * @param {Object} root - The root node of the data structure being updated.
 * @param {Array} results - An array of result objects to be updated during the processing of new inserts.
 * @returns {Promise<void>} A promise that resolves when the processing is complete.
 */
export async function processLeftmostInserts (that, inserts, nodeOptions, distance, root, results) {
  const opts = nodeOptions.opts
  const newLeaves = await createNewLeaves(that, inserts, opts)

  const branchEntries = await Promise.all(
    newLeaves.map(async (node) => {
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
  const newBranchEntries = [firstRootEntry, ...branchEntries].sort(({ key: a }, { key: b }) => opts.compare(a, b))

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
