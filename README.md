

# ranges

Range queries match `>=` the `start` key and `<` the `end` key.

In other words, ranges do not include matches against the end key
but do match against the start key. This is so that more advanced
range queries can be built with only appending to keys rather than
needing to do more advanced key modifications to reduce the closing
match.
