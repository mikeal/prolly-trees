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
    if (typeof identity !== 'number') {
      throw new Error('Identity must be a number')
    }
    if (identity <= threshold) {
      return true
    }
    return false
  }
}

// TODO: cache small numbers to avoid unnecessary tiny allocations
const enc32 = num => {
  const b = Buffer.allocUnsafe(4)
  b.writeUint32LE(num)
  return b
}
const enc64 = num => {
  const b = Buffer.allocUnsafe(8)
  b.writeBigUint64LE(num)
  return b
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

export { readUInt32LE, enc32, enc64, bf, binaryCompare }
