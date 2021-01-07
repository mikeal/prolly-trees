const readUInt32LE = (buffer) => {
  const offset = buffer.byteLength - 4
  return ((buffer[offset]) |
          (buffer[offset + 1] << 8) |
          (buffer[offset + 2] << 16)) +
          (buffer[offset + 3] * 0x1000000)
}

const MAX_UINT32 = 4294967295

const bf = factor => {
  const threshold = Math.floor(MAX_UINT32 / factor)
  return async entry => {
    const identity = await entry.identity()
    /* c8 ignore next */
    if (typeof identity !== 'number') {
      /* c8 ignore next */
      throw new Error('Identity must be a number')
      /* c8 ignore next */
    }
    if (identity <= threshold) {
      return true
    }
    return false
  }
}

// TODO: cache small numbers to avoid unnecessary tiny allocations
const enc32 = value => {
  value = +value
  const buff = new Uint8Array(4)
  buff[3] = (value >>> 24)
  buff[2] = (value >>> 16)
  buff[1] = (value >>> 8)
  buff[0] = (value & 0xff)
  return buff
}
/*
const enc64 = num => {
  const b = Buffer.allocUnsafe(8)
  b.writeBigUint64LE(num)
  return b
}
*/

const simpleCompare = (a, b) => {
  if (a === b) return 0
  if (a > b) return 1
  return -1
}

const binaryCompare = (b1, b2) => {
  // Note: last perf profile of mutations showed that this function
  // is LazyCompile and using up a lot of the time. with some tweaking
  // this can probably get inlined
  for (let i = 0; i < b1.byteLength; i++) {
    if (b2.byteLength === i) return 1
    const c1 = b1[i]
    const c2 = b2[i]
    if (c1 === c2) continue
    if (c1 > c2) return 1
    else return -1
  }
  if (b2.byteLength > b1.byteLength) return -1
  return 0
}

class CIDCounter {
  constructor () {
    this._cids = new Set()
  }

  add (node) {
    /* c8 ignore next */
    if (!node.address) {
      /* c8 ignore next */
      throw new Error('Cannot add node without address')
      /* c8 ignore next */
    }
    if (node.address.then) {
      const p = node.address.then(cid => this._cids.add(cid.toString()))
      this._cids.add(p)
      p.then(() => this._cids.delete(p))
    } else {
      this._cids.add(node.address.toString())
    }
  }

  async all () {
    await Promise.all([...this._cids])
    return this._cids
  }
}

export { readUInt32LE, enc32, bf, binaryCompare, simpleCompare, CIDCounter }
