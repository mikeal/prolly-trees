/* globals describe, it */
import { deepStrictEqual as same } from 'assert'
import { Node, Entry, EntryList, create } from '../src/base.js'

const chunker = entry => entry.address

const mapper = value => ({ key: value, address: value })

const validate = (node, check) => {
  same(node.entryList.entries.length, check.entries)
  same(node.closed, check.closed)
  same(node.distance, check.distance)
}

describe('base', () => {
  const opts = {
    LeafNodeClass: Node,
    LeafEntryClass: Entry,
    BranchNodeClass: Node,
    BranchEntryClass: Entry,
    chunker
  }
  it('basic create', async () => {
    const list = [ false, true, false ].map(mapper)
    const checks = [
      { entries: 2, closed: true, distance: 0 },
      { entries: 1, closed: false, distance: 0 },
      { entries: 2, closed: false, distance: 1 }
    ]
    for await (const node of create({ list, ...opts })) {
      validate(node, checks.shift())
    }
    same(checks.length, 0)
  })
  it('EntryList must have closed argument', () => {
    let threw = true
    try {
      new EntryList({})
      threw = false
    } catch (e) {
      if (e.message !== 'Missing required argument "closed"') {
        throw e
      }
    }
    same(threw, true)
  })
})
