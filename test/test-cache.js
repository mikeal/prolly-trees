/* globals describe, it */
import { nocache, global } from '../src/cache.js'
import { deepStrictEqual as same } from 'assert'

describe('cache', () => {
  it('nocache get error', async () => {
    let threw = true
    try {
      await nocache.get('nope')
      threw = false
    } catch (e) {
      if (e.message !== 'Cannot ask for entries from nocache') throw e
    }
    same(threw, true)
  })
  it('no found in global', async () => {
    let threw = true
    try {
      await global.get('nope')
      threw = false
    } catch (e) {
      if (e.message !== 'Not found') throw e
    }
    same(threw, true)
  })
})
