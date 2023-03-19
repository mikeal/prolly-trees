import { Node } from './base.js'

export async function newInsertsBulker (that, inserts, nodeOptions, distance, encode, root, results) {
  console.log(
    'newInsertsBulker',
    Object.keys(that),
    inserts,
    Object.keys(nodeOptions),
    distance,
    typeof encode,
    root.entryList.entries.map(({ key }) => key),
    Object.keys(results)
  )
  const opts = nodeOptions.opts
  // const chunker = nodeOptions.chunker
  const newLeaves = await createNewLeaves(that, inserts, opts)
  if (newLeaves.length === 0) {
    throw new Error('Failed to insert entries')
  }
  const mapper = async (node) => new opts.BranchEntryClass({ key: node.key, address: await node.address }, opts)
  const entries = await Promise.all(newLeaves.map(mapper))
  // const branches = await Node.from({ entries, chunker, NodeClass: opts.BranchClass, distance, opts })
  const newLeafBlocks = await Promise.all(
    newLeaves.map(async (node) => {
      return await encodeNodeWithoutCircularReference(that, node, encode)
    })
  )
  // create the content addressable blocks for the new nodes
  // const newBranchBlocks = await Promise.all(
  //   // do we also need to do this for newNodes, the leaf nodes?
  //   branches.map(async (node) => {
  //     return await encodeNodeWithoutCircularReference(that, node, encode)
  //   })
  // )
  // find the leftmost leaf and merge to the new root -- we should look inside this one
  const newRoots = await createNewRoot(that, entries, root, newLeaves, opts, nodeOptions, distance)
  const rootBlocks = await Promise.all(newRoots.map(async (node) => await node.encode()))
  const encodedRootBlocks = await Promise.all(
    rootBlocks.map(async (block) => {
      return await encodeNodeWithoutCircularReference(that, block, encode)
    })
  )
  results.root = newRoots[0]
  results.blocks = [...results.blocks, ...newLeafBlocks, ...encodedRootBlocks]
  results.nodes = newLeaves.concat(newRoots) // what about branches
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
    // console.log(
    //   'LeafEntryClass createNewLeaves',
    //   entry.constructor.name,
    //   JSON.stringify([entry.key, entry.address, entry.codec, entry.hasher])
    // )
    // these arent supposed to have addresses
    if (index >= 0) {
      entries.splice(index, 0, entry)
    } else {
      entries.push(entry)
    }
  }
  // console.log('entries:', entries)
  return await Node.from({
    entries,
    chunker: that.chunker,
    NodeClass: opts.LeafClass,
    distance: 0,
    opts
  })
}

async function encodeNodeWithoutCircularReference (that, node, encode) {
  const { codec, hasher } = that
  const value = await codec.encode(node.value)
  const encodeOpts = { codec, hasher, value }

  const result = await encode(encodeOpts)
  return result
}

async function createNewRoot (that, branchEntries, root, newLeaves, opts, nodeOptions, distance) {
  // find leftmost entry in root
  const firstRootEntry = new opts.BranchEntryClass({ key: root.entryList.startKey, address: await root.address }, opts)

  const leafEntries = await Promise.all(
    newLeaves.map(async (node) => {
      console.log('createNewRoot', node.cid, node.entryList?.startKey)
      const key = node.entryList.startKey
      const address = await node.address
      return new opts.BranchEntryClass({ key, address }, opts)
    })
  )

  // const branchEntries = branches// .map((node) => node.entryList.entries).flat()

  const newRootEntries = [firstRootEntry, ...leafEntries, ...branchEntries]

  // const fre = await that.getNode(await firstRootEntry.address)

  console.log('newRootEntries', firstRootEntry.value, leafEntries.map((e) => e.value), branchEntries.map((e) => e.entryList))

  const newRoots = await Node.from({
    ...nodeOptions,
    entries: newRootEntries,
    chunker: that.chunker,
    NodeClass: opts.BranchClass,
    distance: distance + 1
  })

  return newRoots
}
