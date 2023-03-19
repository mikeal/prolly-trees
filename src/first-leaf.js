import { Node } from './base.js'

export async function newInsertsBulker (that, inserts, nodeOptions, distance, root, results) {
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
  const newLeaves = await createNewLeaves(that, inserts, opts)

  if (newLeaves.length === 0) {
    throw new Error('Failed to insert entries')
  }

  console.log('newLeaves:', await Promise.all(newLeaves.map(async (m) => await m.entryList)))

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

  console.log('newRootEntries:', await Promise.all(newRootEntries.map(async (m) => await m.address)))

  const newBranches = await Node.from({
    ...nodeOptions,
    entries: newRootEntries,
    chunker: that.chunker,
    NodeClass: opts.BranchClass,
    distance: distance + 1,
    opts
  })

  const newBlocks = [...newLeaves, ...newBranches]

  console.log('newBlocks:', await Promise.all(newBlocks.map(async (m) => await m.address)))

  const encodedBlocks = await Promise.all(
    newBlocks.map(async (block) => {
      // console.log('encodedBlocks', await block.address)
      const ad = await block.address
      const enBlock = await encodeNodeWithoutCircularReference(that, block)
      console.log('did encodedBlocks', ad, await enBlock.cid)
      return enBlock
    })
  )
  // console.log('encodedBlocks:', await Promise.all(encodedBlocks.map(async (m) => await m.cid)))

  results.root = newBranches[0]
  results.blocks = [...results.blocks, ...encodedBlocks]
  results.nodes = newLeaves.concat(newBranches)
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

const myencode = async (that, opts) => {
  // console.log('encode', JSON.stringify(opts.value))
  const encd = await that.encode(opts)
  console.log('did encode', encd.value)
  return encd
}

export async function encodeNodeWithoutCircularReference (that, node) {
  const { codec, hasher } = that
  // console.log('node', node)
  console.log('node.value', node.block.value, node.value)
  const value = await codec.encode(node.block.value)
  console.log('encoded value', value.toString())
  const encodeOpts = { codec, hasher, value }
  const result = await myencode(that, encodeOpts)
  return result
}
