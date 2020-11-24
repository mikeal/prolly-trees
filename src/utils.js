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

export { readUInt32LE, enc32, enc64, bf }
