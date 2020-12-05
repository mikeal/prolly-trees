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

const createPersons = `CREATE TABLE Persons (
  PersonID int,
  LastName varchar(255),
  FirstName varchar(255),
  Address varchar(255),
  City varchar(255)
)`

const createPersons2 = `CREATE TABLE Persons2 (
  PersonID int,
  LastName varchar(255),
  FirstName varchar(255),
  Address varchar(255),
  City varchar(255)
)`

const loadFixture = async (q, database=Database.create(), store=storage()) => {
  const iter = sql(q, { database })

  let last
  for await (const block of iter) {
    await store.put(block)
    last = block
  }
  const opts = { get: store.get, cache }
  const db = await Database.from(last.cid, opts)
  return { database: db, store, cache, root: last.cid }
}

describe('sql', () => {
  it('basic create', async () => {
    const { database: db } = await loadFixture(createPersons)
    same(entries(db.tables).length, 1)
    same(db.tables.Persons.rows, null)
  })

  it('create twice', async () => {
    const { database, store } = await loadFixture(createPersons)
    const db = (await loadFixture(createPersons2, database, store)).database
    same(entries(db.tables).length, 2)
    same(db.tables.Persons2.rows, null)
  })
})
