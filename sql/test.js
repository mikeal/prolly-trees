/* globals describe, it */
import { sql, Database } from './index.js'
import { nocache } from '../src/cache.js'
import { bf } from '../src/utils.js'
import { deepStrictEqual as same } from 'assert'

const chunker = bf(3)

const cache = nocache

const { keys, entries } = Object

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

const q = `CREATE TABLE Persons (
  PersonID int,
  LastName varchar(255),
  FirstName varchar(255),
  Address varchar(255),
  City varchar(255)
)`

describe('sql', () => {
  it('basic create', async () => {
    const iter = sql(q, { database: Database.create() })
    const store = storage()

    let last
    for await (const block of iter) {
      await store.put(block)
      last = block
    }
    const opts = { get: store.get, cache }
    const db = await Database.from(last.cid, opts)
    same(entries(db.tables).length, 1)
    same(db.tables.Persons.rows, null)
  })

  it('create twice', async () => {
  })
})
