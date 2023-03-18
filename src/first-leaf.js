import { Node, processBranchEntries } from './base.js'

export async function newInsertsBulker (that, inserts, opts, nodeOptions, distance, encode, root, results) {
  console.log('newInsertsBulker', Object.keys(that), inserts, Object.keys({ ...opts, ...nodeOptions }), distance, typeof encode, root.entryList.entries.map(({ key }) => key), Object.keys(results))
  // this returns chunked LeafEntryClass entries
  const newEntries = await createNewLeafEntries(that, inserts, opts)
  console.log('New Entries:', newEntries)

  if (newEntries.length === 0) {
    throw new Error('Failed to insert entries')
  }

  // create new leafs and branch nodes for the new entries
  const { newNodes, newBranchNodes } = await createNewBranchNodes(
    that,
    newEntries,
    opts,
    nodeOptions,
    opts.LeafClass,
    distance, results
  )
  console.log('New Nodes:', newNodes.map(node => JSON.stringify(node?.entryList ? node?.entryList.entries : node)))
  console.log('New Branch Nodes:', newBranchNodes.map(node => JSON.stringify(node.entryList.entries)))

  const newLeafBlocks = await Promise.all(
    newNodes.map(async (node) => {
      return await encodeNodeWithoutCircularReference(that, node, encode)
    })
  )

  // create the content addressable blocks for the new nodes
  const newBranchBlocks = await Promise.all(
    // do we also need to do this for newNodes, the leaf nodes?
    newBranchNodes.map(async (node) => {
      return await encodeNodeWithoutCircularReference(that, node, encode)
    })
  )
  // find the leftmost leaf and merge to the new root -- we should look inside this one
  const newRoots = await createNewRoot(that, newBranchNodes, root, newNodes, opts, nodeOptions, distance)
  console.log('New Roots:', newRoots.map(node => JSON.stringify(node.entryList.entries)))

  // encode the new root blocks, why not use encodeNodeWithoutCircularReference?
  const rootBlocks = await Promise.all(newRoots.map(async (node) => await node.encode()))
  const encodedRootBlocks = await Promise.all(
    rootBlocks.map(async (block) => {
      return await encodeNodeWithoutCircularReference(that, block, encode)
    })
  )

  // update the results object, is this the correct new root?
  // it should match the return values for the non-leftmost insert case
  results.root = newRoots[0]
  results.blocks = [...results.blocks, ...newBranchBlocks, ...newLeafBlocks, ...encodedRootBlocks]
  results.nodes = newNodes.concat(newRoots)
}

// exported for testing
export async function createNewLeafEntries (that, inserts, opts) {
  const newEntries = []
  const entries = []
  for (const insert of inserts) {
    const index = entries.findIndex((entry) => that.compare(entry.key, insert.key) > 0)
    const entry = new opts.LeafEntryClass(insert, opts)
    console.log('LeafEntryClass createNewLeafEntries', entry.constructor.name, JSON.stringify([entry.key, entry.address, entry.codec, entry.hasher]))
    // these arent supposed to have addresses
    if (index >= 0) {
      entries.splice(index, 0, entry)
    } else {
      entries.push(entry)
    }
  }

  if (entries.length) {
    let chunk = []
    for (const entry of entries) {
      chunk.push(entry)
      console.log('push Entry', entry.constructor.name, JSON.stringify([entry.key, entry.address, entry.codec, entry.hasher]))
      // await entry.address
      if (await that.chunker(entry, 0)) {
        newEntries.push(chunk)
        chunk = []
      }
    }
    if (chunk.length) {
      newEntries.push(chunk)
    }
  }

  return newEntries
}

async function createNewBranchNodes (that, newEntries, opts, nodeOptions, LeafClass, distance, results) {
  const newNodes = await createNewNodes(that, newEntries, nodeOptions, LeafClass)

  const newBranchEntries = await processBranchEntries(that, results, newNodes, opts)

  const newBranchNodes = await Node.from({
    ...nodeOptions,
    entries: newBranchEntries,
    chunker: that.chunker,
    NodeClass: opts.BranchClass,
    distance: distance + 1
  })
  // both of these are arrays of Node.from results
  return { newNodes, newBranchNodes }
}

async function encodeNodeWithoutCircularReference (that, node, encode) {
  const { codec, hasher } = that
  console.log('encodeNodeWithoutCircularReference', node.constructor.name, JSON.stringify(node.entryList ? node.entryList.entries : node), codec, encode)
  const value = await codec.encode(node.value)
  const encodeOpts = { codec, hasher, value }
  return await encode(encodeOpts)
}

async function createNewRoot (that, newBranchNodes, root, newNodes, opts, nodeOptions, distance) {
  console.log('Creating New Root:')
  // console.log('Opts:', opts)
  console.log(
    'New Branch Nodes:',
    newBranchNodes.map((node) => JSON.stringify(node.entryList.entries))
  )
  console.log('Root:', JSON.stringify(root.entryList.entries))
  console.log(
    'New Nodes:',
    newNodes.map((node) =>
      JSON.stringify(node?.entryList ? node?.entryList.entries : node)
    )
  )

  // find leftmost entry in root
  console.log('opts.BranchEntryClass', JSON.stringify([opts.codec, opts.hasher]))
  const firstRootEntry = new opts.BranchEntryClass(
    { key: root.entryList.startKey, address: await root.address },
    opts
  )

  const leafEntries = await Promise.all(
    newNodes.map(async ([node]) => {
      console.log('node.entryList', JSON.stringify(node))
      const key = node.entryList.entries[0].key
      const address = await node.address
      return new opts.BranchEntryClass({ key, address }, opts)
    })
  )

  // const branchEntries = newBranchNodes[0].entryList.entries
  const branchEntries = newBranchNodes.map((node) => node.entryList.entries).flat()
  // why are we only using the first branch node?

  console.log('firstRootEntry:', JSON.stringify(firstRootEntry))
  console.log('leafEntries:', JSON.stringify(leafEntries))
  console.log('branchEntries:', JSON.stringify(branchEntries))

  const newRootEntries = [firstRootEntry, ...leafEntries, ...branchEntries]

  const newRoots = await Node.from({
    ...nodeOptions,
    entries: newRootEntries,
    chunker: that.chunker,
    NodeClass: opts.BranchClass,
    distance: distance + 1
  })
  console.log(
    'Merged Root Entries:',
    newRoots.map((root) => JSON.stringify(root.entryList.entries))
  )
  return newRoots
}

async function createNewNodes (that, entriesArray, nodeOptions, NodeClass) {
  return Promise.all(
    entriesArray.map(async (entries) => {
      const addresses = await entries.map(async (entry) => await entry.address)
      console.log('Node.from entries', entries, JSON.stringify(nodeOptions), await Promise.all(addresses))
      return Node.from({
        ...JSON.parse(JSON.stringify(nodeOptions)), // Create a shallow copy of nodeOptions
        entries,
        chunker: that.chunker,
        NodeClass,
        distance: 0
      })
    })
  )
}

// async function createNewBranchEntries (that, newNodes, opts) {
//   // the issue is newNodes are leaves, we should put them into a branch entry, not turn them into branch entries?
//   // we should refactor what we are doing so we can use processBranchEntries without modifying it
//   const newBranchEntries = []
//   for (const node of newNodes) {
//     const key = await node.key
//     const address = await node.address
//     console.log('await node.address', address)
//     newBranchEntries.push(new opts.BranchEntryClass({ key, address }, opts))
//   }
//   return newBranchEntries
// }
