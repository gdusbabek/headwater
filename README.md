# Headwater - Bitmap Indexing Primitives

__NOTE__: This project currently exists as a proof of concept.  While I trust the indexer, there are a good deal of
performance and usability problems to solve.
It represents a couple hackdays worth of work that I needed to get off my computer.

## Concepts

[Read this first](http://en.wikipedia.org/wiki/Bitmap_index), then come back.

You can all the code doing something useful by examining `TestIndexing.java`.

###  headwater.bitmap

This is basically a `java.util.Bitset` with a few conveniences. I've built reference bitmaps from the very excellent
`OpenBitSet` (copied from Lucene) and a simple one that wraps a `byte[]` array.

### headwater.hashing

Utilities for hashing objets. The key abstraction is `FunnelHasher` which hashes objects into `BitHashableKey`s, which
wrap an object and the bit it hashes to.

`Hashers` contains all the current `FunnelHasher` implementations (mainly for primtives).

### headwater.io

All data is read from and written to `IO` instances.  There are cassandra- and memory-backed implementations.

### headwater.trigram

A string is broken up into a set of `Trigram`s prior to indexing.
You can [read more](http://en.wikipedia.org/wiki/Trigram) about trigrams.

In cases where you can't make a full trigram from a string, an `AugmentationStrategy` is used to augment the partial
trigram with all remaining values out of the possible values of your particular alphabet/character set.  I'm not very
happy with this concept and am looking for better ways to address this (including not indexing the partials or
designamting a single filler character).

### headwater.index

I index trigrams.  The index is basically a mapping from a (field,trigram) tuple to a bitmap representing set of all
keys of the original data.  The bitmap is broken up into segments for convenience. I represent segments in cassandra
using columns where the name is a long and the value is a binary array.

Rather than a straight index, I've chose to make fields a part of the interfaces, so you end up creating indexes of
(key, field, value).  This way you can ask questions like: "give me all the keys where field=$FOO and value matches
\*fo* (if you wanted to find the 'foo's.

You can reduce this to a simple (key, value) index by assuming a constant field value.

Glob queries are the goal here.

#### Lookup interface

#### KeyObserver interface

#### MemoryJack

