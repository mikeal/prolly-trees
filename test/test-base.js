/* globals describe, it */
import { deepStrictEqual as same } from 'assert'
import { Node, Entry, EntryList, create } from '../src/base.js'
import { simpleCompare as compare } from '../src/utils.js'

const chunker = entry => entry.address

const mapper = value => ({ key: value, address: value })

const validate = (node, check) => {
  same(node.entryList.entries.length, check.entries)
  same(node.closed, check.closed)
  same(node.distance, check.distance)
}

const entry = key => new Entry({key})
const entries = [
  entry(0),
  entry(1),
  entry(2),
  entry(3),
  entry(4),
  entry(5),
  entry(6),
  entry(7),
  entry(8)
]
const entryListFixture = new EntryList({ entries, closed: true })

describe('base', () => {
  const opts = {
    LeafClass: Node,
    LeafEntryClass: Entry,
    BranchClass: Node,
    BranchEntryClass: Entry,
    chunker
  }
  it('basic create', async () => {
    const list = [false, true, false].map(mapper)
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
      /* eslint-disable-next-line */
      new EntryList({})
      threw = false
    } catch (e) {
      if (e.message !== 'Missing required argument "closed"') {
        throw e
      }
    }
    same(threw, true)
  })
  it('entryList find', () => {
    const [i, entry] = entryListFixture.find(1, compare)
    same(i, 1)
    same(entry.key, 1)
  })
  it('entryList findMany', () => {
    const results = entryListFixture.findMany([1, 3, 5], compare)
    const comp = [...results.keys()].sort(compare).map(key => [ key, results.get(key)[0].key ])
    same(comp, [[1, 1], [3, 3], [5, 5]])
  })
  /* doesn't work yet
  it('entryList findRange', () => {
    const results = entryListFixture.findRange(2, 5, compare)
    console.log(results)
  })
  */
})
