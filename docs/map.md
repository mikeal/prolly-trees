
This module exports functions and classes to create and manipulate IPLD maps. The IPLD map is a data structure that maps keys to values and can be represented as a directed acyclic graph.

The module provides an interface to create a map, retrieve and manipulate its entries, and encode and decode it to and from IPLD data blocks. The module relies on the `base.js` module for basic encoding and decoding functionality.

## `Imports`

The following functions and classes are imported from `./base.js`:

*   `Entry`
*   `EntryList`
*   `IPLDLeaf`
*   `IPLDBranch`
*   `create`

The `readUInt32LE` function is imported from `./utils.js`.

## `Classes`

*   `MapEntry` extends the `Entry` class and provides a method `identity` that returns the identity of the node as a 32-bit unsigned integer.
    
*   `MapLeafEntry` extends the `MapEntry` class and represents a leaf node in the IPLD map. It takes a `node` and `opts` as its arguments and has the properties `key` and `value`. The `encodeNode` method returns an array of the key and the value of the node.
    
*   `MapBranchEntry` extends the `MapEntry` class and represents a branch node in the IPLD map. It takes a `node` and `opts` as its arguments and has the property `key`. The `encodeNode` method returns an array of the key and the address of the node.
    
*   `MapLeaf` extends the `IPLDLeaf` class and provides methods to retrieve values for given keys, `get` and `getMany`. The method `bulk` is inherited from the `IPLDLeaf` class and allows for bulk encoding and decoding of IPLD data.
    
*   `MapBranch` extends the `IPLDBranch` class and provides methods to retrieve values for given keys, `get` and `getMany`. The method `bulk` is inherited from the `IPLDBranch` class and allows for bulk encoding and decoding of IPLD data.
    

## `Functions`

*   `getValue` returns the value of a node for a given key.
    
*   `getManyValues` returns the values of a node for a list of keys.
    
*   `createGetNode` is a factory function that returns a `getNode` function. This function is used to retrieve nodes from an IPLD data block.
    
*   `create` is a factory function that creates an IPLD map. It takes an object as its argument and has the following properties:
    
    *   `get`: a function that retrieves an IPLD data block
    *   `cache`: a cache for IPLD data blocks
    *   `chunker`: a chunker function for encoding and decoding IPLD data blocks
    *   `list`: a list of entries for the IPLD map
    *   `codec`: a codec for encoding and decoding IPLD data blocks
    *   `hasher`: a hasher for encoding IPLD data blocks
    *   `sorted`: a boolean indicating if the list of entries is sorted
    *   `compare`: a comparator function for sorting the list of entries
    *   `LeafClass`: a class for leaf nodes in the IPLD map
    *   \`LeafEntry

## Usage

```js
import { create, load } from '../src/map.js'
import * as codec from '@ipld/dag-cbor'
import { sha256 as hasher } from 'multiformats/hashes/sha2'
import { nocache, global as globalCache } from '../src/cache.js'
import { bf, simpleCompare as compare } from '../src/utils.js'

const chunker = bf(3)
const cache = nocache

const storage = () => {
  const blocks = {}
  const put = block => {
    blocks[block.cid.toString()] = block
  }
  const get = async cid => {
    const block = blocks[cid.toString()]
    if (!block) throw new Error('Not found')
    return block
  }
  return { get, put, blocks }
}

const opts = { cache, chunker, codec, hasher }

const list = [
  ['a', 1],
  ['b', 1],
  ['bb', 2],
  ['c', 1],
  ['cc', 2],
  ['d', 1],
  ['ff', 2],
  ['h', 1],
  ['z', 1],
  ['zz', 2]
].map(([key, value]) => ({ key, value }))

const createMap = async () => {
  const { get, put } = storage()
  for await (const node of create({ get, compare, list, ...opts })) {
    await put(await node.block)
    return node.address
  }
}

const loadMap = async rootCID => {
  const { get } = storage()
  return await load({ cid: rootCID, get, compare, ...opts })
}

const rootCID = await createMap()
const root = await loadMap(rootCID)
const value = await root.get('a')
console.log(value)
```