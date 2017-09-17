Redis storage engine for Lucene 
===============================

**I am not maintaining or developing this project anymore, however it does a proof of concept to replace Lucene's storage engine Redis (can be distributed filesystem)**. You can read about [my blog post here](http://blog.creapptives.com/post/33172587388/smoking-lucene-on-redis).


Requirements
------------

* Lucene 3.6
* Apache Ant
* Jedis 2.0+

Installation
------------

*   Clone the repo _git clone git@github.com:maxpert/RedisDirectory.git RedisDirectory_
*   cd RedisDirectory
*   ant lib.jar to build jar library under build/jar folder (same directory)

Features
--------
*   Supports Solr 
*   Supports sharding
*   Storage level distribution

Usage
-----

 Make sure you have the RedisDirectory.jar in you class path (Gradle or Ant can help you). To use it just:

```java
 // Initlaize ShardedJedisPool according to your settings
 List<JedisShardInfo> shards = new ArrayList<JedisShardInfo>();
 JedisShardInfo si = new JedisShardInfo("localhost", 6379);
 JedisShardInfo si2 = new JedisShardInfo("localhost", 6389);
 JedisShardInfo si3 = new JedisShardInfo("localhost", 6399);
 shards.add(si);
 shards.add(si2);
 shards.add(si3);
 
 ShardedJedisPool pool = new ShardedJedisPool(new GenericObjectPool.Config(), shards);

 //Intialize 
 RedisDirectory redisDir = new RedisDirectory("max", pool);
 Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_36);
 IndexWriterConfig writerConfig = new IndexWriterConfig(Version.LUCENE_36, analyzer);
 IndexWriter writer = new IndexWriter(redisDir, writerConfig);

 //Intialize your document...
 Document doc = new Document();
 doc.add(...);

 //Add document to index...
 writer.addDocument(doc);
 writer.close();

 redisDir.close();
```

File is divided into blocks and stored as HASH in redis in binary format that can be loaded on demand. You can customise the block size by modifying the FILE_BUFFER_SIZE in RedisDirectory. *Remember its a 1 time intialization once index is created on a particular size it can't be changed; higher block size causes higher fragmentation*. Example:

```java
  RedisDirectory.FILE_BUFFER_SIZE = 16 << 10; // Sets 16K block size
```

Sharding
--------

 Look closely Jedis is doing the complete sharding for us.

Solr Installation
-----------------

 Place the RedisDirectory.jar file with jedis.jar and commons-pool.jar at your required place. Then modify the solrconf.xml to use RedisDirectoryFactory

```xml
<directoryFactory class="mxp.solr.core.RedisDirectoryFactory">
  <str name="shard">localhost:6399</str>
  <str name="shard">localhost:6379</str>
  <str name="shard">localhost:6389</str>
  <int name="fileBufferSize">65536</int>
</directoryFactory>
```

Each name shard points to the redis host string (UNIX sock files yet to come). And you can set custom buffer size in bytes 32K is default (if not mentioned). 

*Note:* Make sure you introduce lib tag to point to the directory containing the RedisDirectory.jar and other jar files

```xml
    <lib dir="/path/to/your/lib/folder" />
```

TODO
----

Still missing features are: 

*   Include support for Snappy compression to compress file block.
*   Rock solid JUNIT test cases for each class.
*   Enable atomic operations on RedisFile, this will allow multiple connections to manipulate single file.
*   Redundancy support, maintain multiple copies of a file (or its blocks).

## License

Copyright (c) 2012 Zohaib Sibte Hassan

Permission is hereby granted, free of charge, to any person
obtaining a copy of this software and associated documentation
files (the "Software"), to deal in the Software without
restriction, including without limitation the rights to use,
  copy, modify, merge, publish, distribute, sublicense, and/or sell
  copies of the Software, and to permit persons to whom the
  Software is furnished to do so, subject to the following
  conditions:

  The above copyright notice and this permission notice shall be
  included in all copies or substantial portions of the Software.

  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
  OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
  NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
  HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
  WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
  FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
  OTHER DEALINGS IN THE SOFTWARE.

