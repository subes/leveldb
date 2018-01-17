/*
 * Copyright (C) 2011 the original author or authors.
 * See the notice.md file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.iq80.leveldb.impl;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalCause;
import com.google.common.cache.RemovalListener;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.table.BlockHandleSliceWeigher;
import org.iq80.leveldb.table.CacheKey;
import org.iq80.leveldb.table.FilterPolicy;
import org.iq80.leveldb.table.KeyValueFunction;
import org.iq80.leveldb.table.Table;
import org.iq80.leveldb.table.UserComparator;
import org.iq80.leveldb.util.Closeables;
import org.iq80.leveldb.util.Finalizer;
import org.iq80.leveldb.util.ILRUCache;
import org.iq80.leveldb.util.InternalTableIterator;
import org.iq80.leveldb.util.LRUCache;
import org.iq80.leveldb.util.RandomInputFile;
import org.iq80.leveldb.util.Slice;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutionException;

import static java.util.Objects.requireNonNull;

public class TableCache
{
    private final LoadingCache<Long, TableAndFile> cache;
    private final Finalizer<Table> finalizer = new Finalizer<>(1);
    private final ILRUCache<CacheKey, Slice> blockCache;

    public TableCache(final File databaseDir,
                      int tableCacheSize,
                      final UserComparator userComparator,
                      final Options options, Env env)
    {
        requireNonNull(databaseDir, "databaseName is null");
        blockCache = options.cacheSize() == 0 ? null : LRUCache.createCache((int) options.cacheSize(), new BlockHandleSliceWeigher());
        cache = CacheBuilder.newBuilder()
                .maximumSize(tableCacheSize)
                .removalListener((RemovalListener<Long, TableAndFile>) notification -> {
                    final TableAndFile value = notification.getValue();
                    if (value != null) {
                        final Table table = value.getTable();
                        if (notification.getCause() == RemovalCause.EXPLICIT) {
                            //on explicit it is already know to be unused, no need to wait
                            //blockCache do save DirectByteBuffers references (only by arrays)
                            try {
                                table.closer().call();
                            }
                            catch (Exception e) {
                                //todo do proper exception notification
                                Finalizer.IGNORE_FINALIZER_MONITOR.unexpectedException(e);
                            }
                        }
                        finalizer.addCleanup(table, table.closer());
                    }
                })
                .build(new CacheLoader<Long, TableAndFile>()
                {
                    @Override
                    public TableAndFile load(Long fileNumber)
                            throws IOException
                    {
                        return new TableAndFile(databaseDir, fileNumber, userComparator, options, blockCache, env);
                    }
                });
    }

    public InternalTableIterator newIterator(FileMetaData file)
    {
        return newIterator(file.getNumber());
    }

    public InternalTableIterator newIterator(long number)
    {
        return new InternalTableIterator(getTable(number).iterator());
    }

    public <T> T get(Slice key, FileMetaData fileMetaData, KeyValueFunction<T> resultBuilder)
    {
        final Table table = getTable(fileMetaData.getNumber());
        return table.internalGet(key, resultBuilder);

    }

    public long getApproximateOffsetOf(FileMetaData file, Slice key)
    {
        return getTable(file.getNumber()).getApproximateOffsetOf(key);
    }

    private Table getTable(long number)
    {
        Table table;
        try {
            table = cache.get(number).getTable();
        }
        catch (ExecutionException e) {
            Throwable cause = e;
            if (e.getCause() != null) {
                cause = e.getCause();
            }
            throw new RuntimeException("Could not open table " + number, cause);
        }
        return table;
    }

    public void close()
    {
        cache.invalidateAll();
        finalizer.destroy();
    }

    public void evict(long number)
    {
        cache.invalidate(number);
    }

    private static final class TableAndFile
    {
        private final Table table;

        private TableAndFile(File databaseDir, long fileNumber, UserComparator userComparator, Options options, ILRUCache<CacheKey, Slice> blockCache, Env env)
                throws IOException
        {
            final File tableFile = tableFileName(databaseDir, fileNumber);
            RandomInputFile source = null;
            try {
                source = env.newRandomAccessFile(tableFile);
                final FilterPolicy filterPolicy = (FilterPolicy) options.filterPolicy();
                table = new Table(source, userComparator,
                        options.verifyChecksums(), blockCache, filterPolicy);
            }
            catch (IOException e) {
                Closeables.closeQuietly(source);
                throw e;
            }
        }

        private File tableFileName(File databaseDir, long fileNumber)
        {
            final String tableFileName = Filename.tableFileName(fileNumber);
            File tableFile = new File(databaseDir, tableFileName);
            if (!tableFile.canRead()) {
                // attempt to open older .sst extension
                final String sstFileName = Filename.sstTableFileName(fileNumber);
                final File sstPath = new File(databaseDir, sstFileName);
                if (sstPath.canRead()) {
                    tableFile = sstPath;
                }
            }
            return tableFile;
        }

        public Table getTable()
        {
            return table;
        }
    }
}
