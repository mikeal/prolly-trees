# Prolly Trees

Implementation of [peer-to-peer search trees](https://0fps.net/2020/12/19/peer-to-peer-ordered-search-indexes/) [(probabalistic b-trees trees)](https://www.dolthub.com/blog/2020-04-01-how-dolt-stores-table-data/) as
used in [dolt](https://www.github.com/dolthub/dolt) and [noms](https://www.github.com/attic-labs/noms).

While this library has 100% test coverage and is relatively stable, it's
not recommended for broad use. The implementation internals are all very
exposed and have not been properly documented. Without thorough documentation
there's a lot of mistakes consumers will make regarding caching and block
storage.

Some time in the near future I will fully document the library.

# Notes

This is *just* an implementation of the trees. It does not have an opinion about
how blocks are encoded and hashed. The tests use a `dag-cbor` IPLD encoder, and
the library is typically encoded into IPLD in other libraries.

## ranges

Range queries match `>=` the `start` key and `<` the `end` key.

In other words, ranges do not include matches against the end key
but do match against the start key. This is so that more advanced
range queries can be built with only appending to keys rather than
needing to do more advanced key modifications to reduce the closing
match.
