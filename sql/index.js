import sql from 'node-sql-parser'
import { sha256 as hasher } from 'multiformats/hashes/sha2'
import * as codec from '@ipld/dag-cbor'
import { encode as encoder, decode as decoder } from 'multiformats/block'
import { create as createSparseArray, load as loadSparseArray } from '../src/sparse-array.js'
import { create as createDBIndex, load as loadDBIndex } from '../src/db-index.js'

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
  static from (cid, { get, cache, chunker }) {
    const create = async (block) => {
      let { schema, index } = block.value
      if (index !== null) {
        index = await loadDBIndex({ cid: index, get, cache, chunker, ...mf })
      }
      return new Column({ index, schema, get, cache, block })
    }
    return getNode(cid, get, cache, create)
  }
}

const validate = (schema, val) => {
  const { dataType, length } = schema.definition
  const { type, value } = val
  if (value.length > length) throw new Error('Schema validation: value too long')
  if (type === 'string' && dataType === 'VARCHAR') return true
  if (type === 'number' && dataType === 'INT') return true
  console.log({schema, val})
  throw new Error('Not implemented')
}

class Row {
  constructor ({ block, table }) {
    this.block = block
    this.value = block.value
    this.props = table.columns.map(col => col.schema)
  }
  get address () {
    return this.block.cid
  }
  getIndex (i) {
    return this.value[i]
  }
  get (columnName) {
    if (Array.isArray(this.value)) {
      // TODO: optimize this find to use only a single iteration
      const i = this.props.findIndex(p => p.column.column === columnName)
      if (i === -1) throw new Error(`No column named "${columnName}"`)
      return this.value[i]
    } else {
      return this.value[columnName]
    }
  }
  columns (query) {
    if (query === '*') {
      return this.toArray()
    } else if (Array.isArray(query)) {
      const result = []
      for (const { expr, as } of query) {
        if (as !== null) throw new Error('Not Implmented')
        if (expr.type !== 'column_ref') throw new Error('Not Implmented')
        if (expr.table !== null) throw new Error('Not Implemented')
        result.push(this.get(expr.column))
      }
      return result
    } else {
      throw new Error('Not Implemented')
    }
  }
  toArray () {
    if (Array.isArray(this.value)) {
      return this.value
    } else {
      throw new Error('Unsupported')
    }
  }
  toObject () {
    if (Array.isArray(this.value)) {
      const props = [...this.props()]
      console.log({props})
      throw new Error('here')
    } else {
      throw new Error('Unsupported')
    }
  }
}

const tableInsert = async function * (table, ast, { database, chunker }) {
  if (ast.columns !== null) throw new Error('Not implemented')
  const { get, cache } = database
  const { values } = ast
  const inserts = []
  const schemas = table.columns.map(col => col.schema)
  for (const { type, value } of values) {
    const row = []
    if (type !== 'expr_list') throw new Error('Not implemented')
    for (let i = 0; i < value.length; i++) {
      const schema = schemas[i]
      const val = value[i]
      validate(schema, val)
      row.push(val.value)
    }
    const block = await encode(row)
    yield block
    const _row = new Row({ block, table })
    cache.set(_row.address, _row)
    inserts.push({ block, row: _row})
  }
  const opts = { chunker, get, cache, ...mf }
  if (table.rows === null) {
    let i = 1
    const list = inserts.map(({ block: { cid }, row }) => ({ key: i++, value: cid, row }))
    let rows

    for await (const node of createSparseArray({ list, ...opts })) {
      yield node.block
      rows = node
    }
    let blocks = []
    const writeIndex = async (column, i) => {
      const entries = []
      for (const { key, value, row } of list) {
        const val = row.getIndex(i)
        entries.push({ key: [ val, key ], row })
      }
      let index
      for await (const node of createDBIndex({ list: entries, ...opts })) {
        blocks.push(node.block)
        index = node
      }
      return index
    }
    const promises = table.columns.map((...args) => writeIndex(...args))
    const pending = new Set(promises)
    promises.forEach(p => p.then(() => pending.delete(p)))
    while (pending.size) {
      await Promise.race([...pending])
      yield * blocks
      blocks = []
    }
    const indexes = await Promise.all(promises.map(p => p.then(index => index.address)))
    const node = await table.encodeNode()
    node.rows = await rows.address
    node.columns = []
    const columns = await Promise.all(table.columns.map(c => c.encodeNode()))
    while (columns.length) {
      const col = columns.shift()
      col.index = await indexes.shift()
      const block = await encode(col)
      yield block
      node.columns.push(block.cid)
    }
    const newTable = await encode(node)
    yield newTable
    const dbNode = await database.encodeNode()
    dbNode.tables[table.name] = newTable.cid
    yield encode(dbNode)
  }
}

class Table extends SQLBase {
  constructor ({ name, rows, columns, ...opts }) {
    super(opts)
    this.name = name
    this.rows = rows
    this.columns = columns
  }
  async encodeNode () {
    const columns = await Promise.all(this.columns.map(column => column.address))
    const rows = this.rows === null ? null : await this.rows.address
    return { columns, rows }
  }
  insert (ast, opts) {
    return tableInsert(this, ast, opts)
  }
  static create (columnSchemas) {
    const columns = columnSchemas.map(schema => Column.create(schema))
    const table = new Table({ rows: null, columns })
    return table
  }
  static from (cid, name, { get, cache, chunker }) {
    const create = async (block) => {
      let { columns, rows } = block.value
      const promises = columns.map(cid => Column.from(cid, { get, cache, chunker }))
      if (rows !== null) {
        rows = loadSparseArray({ cid: rows, cache, get, chunker, ...mf })
      }
      columns = await Promise.all(promises)
      rows = await rows
      return new Table({ name, columns, rows, get, cache, block })
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
      return [ key, await value.address ]
    })
    const tables = fromEntries(await Promise.all(promises))
    return { tables }
  }
  static create (opts) {
    return new Database({ tables: {}, ...opts })
  }
  static async from (cid, { get, cache, chunker }) {
    const create = async (block) => {
      let { tables } = block.value
      const promises = entries(tables).map(async ([key, cid]) => {
        return [ key, await Table.from(cid, key, { get, cache, chunker }) ]
      })
      tables = fromEntries(await Promise.all(promises))
      return new Database({ tables, get, cache, block })
    }
    return getNode(cid, get, cache, create)
  }
  sql (q, opts) {
    return sqlQuery(q, { ...opts, database: this })
  }
}

const parse = query => (new sql.Parser()).astify(query)

const notsupported = select => {
  const keys = [
    'options',
    'distinct',
    'where',
    'groupby',
    'having',
    'orderby',
    'limit',
    'for_update'
  ]
  keys.forEach(key => {
    if (select[key] !== null) throw new Error(`Not supported "${key}"`)
  })
}

const runSelect = async function * (select) {
  for await (const { entry, table } of select.where()) {
    const result = await select.columns(entry, table)
    yield { entry, table, ...result }
  }
}

const runWhere = async function * (select) {
  const tables = select.ast.from.map(({ table }) => select.db.tables[table])
  if (select.ast.where === null) {
    for (const table of tables) {
      for await (const entry of table.rows.getAllEntries()) {
        yield { entry, table }
      }
    }
  }
}

const filterResults = async function * (results, name) {
  for await (const r of results) {
    yield r[name]
  }
}

class Select {
  constructor (db, ast) {
    notsupported(ast)
    this.db = db
    this.ast = ast
  }
  async columns (entry, table) {
    const { value } = entry
    const { get, cache } = this.db
    const create = block => new Row({ block, table })
    const row = await getNode(value, get, cache, create)
    return { row, columns: row.columns(this.ast.columns) }
  }
  where () {
    return runWhere(this)
  }
  run () {
    return runSelect(this)
  }
  each () {
    return filterResults(this.run(), 'columns')
  }
  async all () {
    const results = []
    for await (const result of this.each()) {
      results.push(result)
    }
    return results
  }
}

const exec = (ast, { database, chunker }) => {
  const { keyword, type } = ast
  if (keyword === 'table') {
    if (type === 'create') {
      const columnSchemas = ast.create_definitions
      if (!database) throw new Error('No database to create table in')
      return database.createTable(ast)
    }
    throw new Error('Not implemented')
  }
  if (type === 'insert') {
    if (!database) throw new Error('No database to create table in')
    const [ { db, table: name } ] = ast.table
    if (db !== null) throw new Error('Not implemented')
    const table = database.tables[name]
    if (!table) throw new Error(`Missing table '${name}'`)
    return table.insert(ast, { database, chunker })
  }
  if (type === 'select') {
    return new Select(database, ast)
  }
  throw new Error('Not implemented')
}

const sqlQuery = (q, opts) => exec(parse(q), opts)

export { Database, Table, Column, exec, sqlQuery as sql }

