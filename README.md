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

## Performance
