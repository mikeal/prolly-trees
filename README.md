# Chunky Trees

Implementation of [peer-to-peer b-trees](https://0fps.net/2020/12/19/peer-to-peer-ordered-search-indexes/) which
are [prolly trees](https://www.dolthub.com/blog/2020-04-01-how-dolt-stores-table-data/) as
used in dolt and noms.

While this library has 100% test coverage and is relatively stable, it's
not recommended for broad use. The implementation internals are all very
exposed and have not been properly documented. Without thorough documentation
there's a lot of mistakes consumers will make regarding caching and block
storage.

Some time in the near future I will fully document the library.

# Notes

## ranges

Range queries match `>=` the `start` key and `<` the `end` key.

In other words, ranges do not include matches against the end key
but do match against the start key. This is so that more advanced
range queries can be built with only appending to keys rather than
needing to do more advanced key modifications to reduce the closing
match.
