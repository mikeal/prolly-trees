/* globals describe, it */
import { binaryCompare } from '../src/utils.js'
import { deepStrictEqual as same } from 'assert'

describe('utils', () => {
  it('binary compare', async () => {
    const a = Buffer.from('a')
    const b = Buffer.from('aa')
    same(binaryCompare(a, b), -1)
    same(binaryCompare(b, a), 1)
  })
})
