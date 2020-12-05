IPLD Schema

```sh
type Column {
  schema &Map
  index nullable &DBIndex
}
type Columns { String: Column }

type Table struct {
  columns Columns
  rows nullable &SparseArray
}
type Tables { String: Table }

type Database struct {
  tables Tables
}
```
