# Headwater - Distributed Bitmap Indexing Primitives [![Build Status](https://secure.travis-ci.org/gdusbabek/headwater.png)](http://travis-ci.org/gdusbabek/headwater) [![Coverage Status](https://img.shields.io/coveralls/gdusbabek/headwater.svg)](https://coveralls.io/r/gdusbabek/headwater)

__NOTE__: This project currently exists as a proof of concept.  While I trust the indexer, there are a good deal of
performance and usability problems to solve.  On the plus side, I think there is a lot of low-hanging fruit to grab.
It represents a couple hackdays worth of work that I needed to get off my computer.

## Concepts

[Read this first](http://en.wikipedia.org/wiki/Bitmap_index), then come back.

To see indexing in action take a look at `TestIndexing.java` and `Shakespeare.java`.

## Goals

* Glob searching over lots of data.

## Performance

Shorter queries result in faster responses.  That is `*foo*` will always return more quickly than `*foosterific*`.

## Java

Uses Java 7. It would work fine on Java 6 except for a few methods used involving java.util.BitSet.

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

Remember, glob queries are the goal here.

#### Lookup interface

Used on query.  When you query an index, this interface is used to convert a hashed bit (read from the bitmap) back to
the key it represents.  An additional method takes that key and an additional field to lookup the value that was
indexed.

This may be broken up into two interfaces in the future, as the `toKey*()` and `lookup()` methods do different enough
things and may have quite different implementations. 

#### KeyObserver interface

Used during indexing.  When you build an index, the act of adding to the index is observed by this class.  
Implementations can be used to build the bit->key and (key,field)->value mappings that are expected by the `Lookup`
interface.

You will not need to supply a `KeyObserver` impelementation if you already have or know those mappings, or can derive 
them easily. 

#### MemoryJack

Implements both the `Lookup` and `KeyObserver` interfaces in memory.  You wouldn't want to use it in production, but
it gives you an idea of what the interfaces expect and is handy for testing things out.

## Roadmap

No order.

* Cassandra-backed `Lookup` and `KeyObserver` implementations.
* Some real documentation and how-to guides.

## Questions?

You can reach me several ways:

* email: gdusbabek@gmail.com
* twitter: @gdusbabek
* IRC: gdusbabek on freenode (I'm almost always connected)
