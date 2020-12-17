const nocache = {
  has: () => false,
  get: () => { throw new Error('Cannot ask for entries from nocache') },
  set: () => {}
}

const toKey = key => key.asCID === key ? key.toString() : JSON.stringify(key)

const global = {
  blocks: {},
  has: key => !!global.blocks[toKey(key)],
  set: async node => {
    let key = node.address
    if (key.then) key = await key
    key = toKey(key)
    global.blocks[key] = node
  },
  get: key => {
    key = toKey(key)
    if (typeof global.blocks[key] === 'undefined') throw new Error('Not found')
    return global.blocks[key]
  }
}

export { nocache, global }
