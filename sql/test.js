/* globals describe, it */
import { sql, Database } from './index.js'
import { nocache } from '../src/cache.js'
import { deepStrictEqual as same } from 'assert'
import { bf } from '../src/utils.js'

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

const insertOnlyId = `INSERT INTO Persons (PersonID) VALUES (4006)`
const insertFullRow = `INSERT INTO Persons VALUES (12, 'Rogers', 'Mikeal', '241 BVA', 'San Francisco')`

const runSQL = async (q, database=Database.create(), store=storage()) => {
  const iter = database.sql(q, { chunker })

  let last
  for await (const block of iter) {
    await store.put(block)
    last = block
  }
  const opts = { get: store.get, cache }
  const db = await Database.from(last.cid, opts)
  return { database: db, store, cache, root: last.cid }
}

const verifyPersonTable = table => {
  const expected = [
    { name: 'PersonID',
      dataType: 'INT'
    },
    { name: 'LastName',
      dataType: 'VARCHAR',
      length: 255
    },
    { name: 'FirstName',
      dataType: 'VARCHAR',
      length: 255
    },
    { name: 'Address',
      dataType: 'VARCHAR',
      length: 255
    },
    { name: 'City',
      dataType: 'VARCHAR',
      length: 255
    }
  ]
  for (const column of table.columns) {
    const { name, dataType, length } = expected.shift()
    same(column.name, name)
    same(column.schema.definition.dataType, dataType)
    same(column.schema.definition.length, length)
  }
}

describe('sql', () => {
  it('basic create', async () => {
    const { database: db } = await runSQL(createPersons)
    same(entries(db.tables).length, 1)
    same(db.tables.Persons.rows, null)
    verifyPersonTable(db.tables.Persons)
  })

  it('create twice', async () => {
    const { database, store } = await runSQL(createPersons)
    const db = (await runSQL(createPersons2, database, store)).database
    same(entries(db.tables).length, 2)
    same(db.tables.Persons2.rows, null)
    verifyPersonTable(db.tables.Persons)
    verifyPersonTable(db.tables.Persons2)
  })

  it('insert initial row', async () => {
    const { database, store } = await runSQL(createPersons)
    const { database: db } = await runSQL(insertFullRow, database, store)
  })
})
