import sql from 'node-sql-parser'
import { sha256 as hasher } from 'multiformats/hashes/sha2'
import * as codec from '@ipld/dag-cbor'
import { encode as encoder, decode as decoder } from 'multiformats/block'

const mf = { codec, hasher }

const encode = value => encoder({ value, ...mf })
const decode = bytes => decoder({ bytes, ...mf })

const immediate = () => new Promise(resolve => setImmediate(resolve))

const getNode = async (cid, get, cache, create) => {
  if (cache.has(cid)) {
    return cache.get(cid)
  }
  const block = await get(cid)
  const node = await create(block)
  cache.set(cid, node)
  return node
}

class SQLBase {
  constructor ({ block }) {
    this.block = block || this.encode()
    this.address = this.block.then ? this.block.then(b => b.cid) : this.block.cid
  }
  async encode () {
    if (this.block) return this.block
    await immediate()
    const node = await this.encodeNode()
    return encode(node)
  }
}

class Column extends SQLBase {
  constructor ({ schema, index, ...opts }) {
    super(opts)
    this.name = schema.column.column
    this.definition = schema.definition
    this.schema = schema
    this.index = index
  }
  async encodeNode () {
    const index = this.index === null ? null : await this.index.address
    return { schema: this.schema, index }
  }
  static create (schema) {
    return new Column({ schema, index: null })
  }
  static from (cid, { get, cache }) {
    const create = async (block) => {
      let { schema, index } = block.value
      if (index !== null) {
        index = await loadDBIndex({ cid: index, cache, get, ...mf })
      }
      return new Column({ index, schema, get, cache, block })
    }
    return getNode(cid, get, cache, create)
  }
}

class Table extends SQLBase {
  constructor ({ rows, columns, ...opts }) {
    super(opts)
    this.rows = rows
    this.columns = columns
  }
  async encodeNode () {
    const columns = await Promise.all(this.columns.map(column => column.address))
    const rows = this.rows === null ? null : await this.rows.address
    return { columns, rows }
  }
  static create (columnSchemas) {
    const columns = columnSchemas.map(schema => Column.create(schema))
    const table = new Table({ rows: null, columns })
    return table
  }
  static from (cid, { get, cache }) {
    const create = async (block) => {
      let { columns, rows } = block.value
      const promises = columns.map(cid => Column.from(cid, { get, cache }))
      if (rows !== null) {
        rows = loadSparseArray({ cid: rows, cache, get, ...mf })
      }
      columns = fromEntries(await Promise.all(promises))
      rows = await rows
      return new Table({ columns, rows, get, cache, block })
    }
    return getNode(cid, get, cache, create)
  }
}

const createTable = async function * (database, ast) {
  const [ { table: name } ] = ast.table
  const table = Table.create(ast.create_definitions)
  const columns = await Promise.all(table.columns.map(column => column.encode()))
  yield * columns

  const tableBlock = await table.encode()
  yield tableBlock

  const node = await database.encodeNode()
  node.tables[name] = tableBlock.cid
  yield encode(node)
}

const { entries, fromEntries } = Object

class Database extends SQLBase {
  constructor ({ tables, get, cache, ...opts }) {
    super(opts)
    this.get = get
    this.cache = cache
    this.tables = tables
  }
  createTable (ast) {
    return createTable(this, ast)
  }
  async encodeNode () {
    const promises = entries(this.tables).map(async ([ key, value ]) => {
      return [ key, await value.encode.address ]
    })
    const tables = fromEntries(await Promise.all(promises))
    return { tables }
  }
  static create (opts) {
    return new Database({ tables: {}, ...opts })
  }
  static async from (cid, { get, cache }) {
    const create = async (block) => {
      let { tables } = block.value
      const promises = entries(tables).map(async ([key, cid]) => {
        return [ key, await Table.from(cid, { get, cache }) ]
      })
      tables = fromEntries(await Promise.all(promises))
      return new Database({ tables, get, cache, block })
    }
    return getNode(cid, get, cache, create)
  }
}

const parse = query => (new sql.Parser()).astify(query)

const exec = (ast, { database }) => {
  const { keyword, type } = ast
  if (keyword === 'table') {
    if (type === 'create') {
      const columnSchemas = ast.create_definitions
      if (!database) throw new Error('No database to create table in')
      return database.createTable(ast)
    }
  }
}

const sqlQuery = (q, opts) => exec(parse(q), opts)

export { Database, Table, Column, exec, sqlQuery as sql }


