# LevelDB in Java
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.github.pcmind/leveldb/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.github.pcmind/leveldb)
[![TravisCI Build Status](https://travis-ci.org/pcmind/leveldb.svg?branch=master)](https://travis-ci.org/pcmind/leveldb)
[![AppVeyor Build Status](https://ci.appveyor.com/api/projects/status/kkiy4t9983gsy6bj/branch/master?svg=true)](https://ci.appveyor.com/project/pcmind/leveldb/branch/master)

## About this fork
This is a rewrite (port) of [LevelDB](https://github.com/google/leveldb) in
Java. Forked from original [dain/leveldb](https://github.com/dain/leveldb/), aimed at
improving the work already done, add missing features and make it production ready. 

## Current status

The plan is to maintain the port as close as possible to original C++ code, with minimal 
refactoring until everything is ported. For now, port will also maintain its api 
as close as possible to original [dain/leveldb](https://github.com/dain/leveldb/) 
to enable merges and compare compatibility with [fusesource/leveldbjni](https://github.com/fusesource/leveldbjni/).

## API Usage:

Recommended Package imports:

```java
import org.iq80.leveldb.*;
import java.io.*;
```

Opening and closing the database.

```java
Options options = new Options();
options.createIfMissing(true);
DB db = factory.open(new File("example"), options);
try {
  // Use the db in here....
} finally {
  // Make sure you close the db to shutdown the 
  // database and avoid resource leaks.
  db.close();
}
```

Putting, Getting, and Deleting key/values.

```java
db.put(bytes("Tampa"), bytes("rocks"));
String value = asString(db.get(bytes("Tampa")));
db.delete(bytes("Tampa"), wo);
```

Performing Batch/Bulk/Atomic Updates.

```java
WriteBatch batch = db.createWriteBatch();
try {
  batch.delete(bytes("Denver"));
  batch.put(bytes("Tampa"), bytes("green"));
  batch.put(bytes("London"), bytes("red"));

  db.write(batch);
} finally {
  // Make sure you close the batch to avoid resource leaks.
  batch.close();
}
```

Iterating key/values.

```java
DBIterator iterator = db.iterator();
try {
  for(iterator.seekToFirst(); iterator.hasNext(); iterator.next()) {
    String key = asString(iterator.peekNext().getKey());
    String value = asString(iterator.peekNext().getValue());
    System.out.println(key+" = "+value);
  }
} finally {
  // Make sure you close the iterator to avoid resource leaks.
  iterator.close();
}
```

Working against a Snapshot view of the Database.

```java
ReadOptions ro = new ReadOptions();
ro.snapshot(db.getSnapshot());
try {
  
  // All read operations will now use the same 
  // consistent view of the data.
  ... = db.iterator(ro);
  ... = db.get(bytes("Tampa"), ro);

} finally {
  // Make sure you close the snapshot to avoid resource leaks.
  ro.snapshot().close();
}
```

Using a custom Comparator.

```java
DBComparator comparator = new DBComparator(){
    public int compare(byte[] key1, byte[] key2) {
        return new String(key1).compareTo(new String(key2));
    }
    public String name() {
        return "simple";
    }
    public byte[] findShortestSeparator(byte[] start, byte[] limit) {
        return start;
    }
    public byte[] findShortSuccessor(byte[] key) {
        return key;
    }
};
Options options = new Options();
options.comparator(comparator);
DB db = factory.open(new File("example"), options);
```
    
Disabling Compression

```java
Options options = new Options();
options.compressionType(CompressionType.NONE);
DB db = factory.open(new File("example"), options);
```

Configuring the Cache

```java    
Options options = new Options();
options.cacheSize(100 * 1048576); // 100MB cache
DB db = factory.open(new File("example"), options);
```

Getting approximate sizes.

```java
long[] sizes = db.getApproximateSizes(new Range(bytes("a"), bytes("k")), new Range(bytes("k"), bytes("z")));
System.out.println("Size: "+sizes[0]+", "+sizes[1]);
```
    
Getting database status.

```java
String stats = db.getProperty("leveldb.stats");
System.out.println(stats);
```

Getting informational log messages.

```java
Logger logger = new Logger() {
  public void log(String message) {
    System.out.println(message);
  }
};
Options options = new Options();
options.logger(logger);
DB db = factory.open(new File("example"), options);
```

Destroying a database.

```java    
Options options = new Options();
factory.destroy(new File("example"), options);
```

## Maven
In a Maven project, include the `io.github.pcmind:leveldb` artifact in the dependencies section
of your `pom.xml`:
```xml
<dependency>
    <groupId>io.github.pcmind</groupId>
    <artifactId>leveldb</artifactId>
    <classifier>uber</classifier>
    <version>0.11</version>
</dependency>
```

## Todo

- Port reverse iteration
- Port some minor API differences (like `max_file_size` Option)
- Port some missing tests

## Performance

Benchmark test were ported to Java from original `db_bench` program. 
In Java, benchmark testing should be done a differently than native code, to take in consideration JVM environment. 
An additional option `jvm_warm_up_iterations` was added to configure the number 
of warm up runs to execute before displaying results. 

### Setup

We use a data base with a million entries.  Each entry has a 16 byte key, and a 100 byte value. 
Values used by the benchmark compress to about half their original size. Snappy artifact use for benchmark is: `org.xerial.snappy:snappy-java:jar:1.1.2.6` 

    LevelDB:    iq80 leveldb version 0.11
    Date:       Tue Nov 06 00:02:19 WET 2018
    CPU:        8 * Intel(R) Core(TM) i7-6700HQ CPU @ 2.60GHz
    CPUCache:   6144 KB
    Keys:       16 bytes each
    Values:     100 bytes each (50 bytes after compression)
    Entries:    1000000
    RawSize:    110.6 MB (estimated)
    FileSize:   62.9 MB (estimated)
    
Note: Hard drive used for benchmark [HTS721010A9E630](https://www.hgst.com/sites/default/files/resources/TS-7K1000-ds.pdf) is 7200RPM.

To have a performance estimate, we also add comparable results of running the same benchmarks on 
[Google LevelDB v1.20](https://github.com/google/leveldb/releases/tag/v1.20) +
 [Google Snappy v1.1.7](https://github.com/google/snappy/tree/1.1.7) using its `db_bench` application.

At the moment of writing, original Google LevelDB readme page has similar benchmark result, but executed with older version 1.1 and with a different hardware setup.
 
### Write performance

The "fill" benchmarks create a brand new database, in either sequential, or random 
order. The "fillsync" benchmark flushes data to the disk after every operation; the other write operations don't force data to disk. The "overwrite" benchmark does random 
writes that update existing keys in the database.

Google LevelDB:   

    fillseq      :       5.186 micros/op;   21.3 MB/s     
    fillsync     :   33063.259 micros/op;    0.0 MB/s (1000 ops)
    fillrandom   :      11.419 micros/op;    9.7 MB/s      
    overwrite    :      19.496 micros/op;    5.7 MB/s 

Java LevelDB:

    fillseq      :     2.30419 micros/op;    48,0 MB/s 
    fillsync     :  9396.26333 micros/op;     0,0 MB/s (1000 ops)
    fillrandom   :     5.86861 micros/op;    18,9 MB/s 
    overwrite    :    11.46700 micros/op;     9,6 MB/s 

We did run benchmark multiple times, and unexpectedly Java did perform better on this setup. Some additional investigation are required here.

### Read performance

We list the performance of reading sequentially and also the performance of a random lookup. Note that the database 
created by the benchmark is quite small. Therefore the report characterizes the 
performance of leveldb when the working set fits in memory. The cost of reading a piece 
of data that is not present in the operating system buffer cache will be dominated 
by the one or two disk seeks needed to fetch the data from disk. Write performance 
will be mostly unaffected by whether or not the working set fits in memory.

Google LevelDB:

    readrandom   :       6.572 micros/op;
    readseq      :       0.311 micros/op;     355.6 MB/s  

Java LevelDB:

    readrandom   :       6.38116 micros/op;    17,3 MB/s
    readseq      :       0.31444 micros/op;   351,8 MB/s 

### Multithreaded

One of the improvement area from version 0.10 to 0.11, is on lock management. 

Same benchmarks where executed on same hardware setup but with 4 threads (`--threads=4`).

#### Write

Google LevelDB:
 
    fillseq      :      24.148 micros/op;   18.3 MB/s     
    fillsync     :   47978.135 micros/op;    0.0 MB/s (1000 ops)
    fillrandom   :      69.203 micros/op;    6.4 MB/s     
    overwrite    :      78.089 micros/op;    5.5 MB/s 

Java LevelDB:

    fillseq      :    16.85414 micros/op;    6,6 MB/s 
    fillsync     : 16428.35844 micros/op;    0,0 MB/s (1000 ops)
    fillrandom   :    39.56687 micros/op;    2,8 MB/s 
    overwrite    :    44.58619 micros/op;    2,5 MB/s 
    
#### Read

Google LevelDB:

    readrandom   :       7.799 micros/op;
    readseq      :       0.402 micros/op; 1100.1 MB/s     

Java LevelDB:

    readrandom   :     9.00297 micros/op;   49,2 MB/s
    readseq      :     0.46418 micros/op; 1106,3 MB/s

