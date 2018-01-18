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
package org.iq80.leveldb.table;

import com.google.common.base.Throwables;
import org.iq80.leveldb.DBException;
import org.iq80.leveldb.impl.SeekingIterable;
import org.iq80.leveldb.util.ILRUCache;
import org.iq80.leveldb.util.RandomInputFile;
import org.iq80.leveldb.util.Slice;
import org.iq80.leveldb.util.Slices;
import org.iq80.leveldb.util.Snappy;
import org.iq80.leveldb.util.TableIterator;
import org.iq80.leveldb.util.VariableLengthQuantity;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Comparator;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static org.iq80.leveldb.CompressionType.SNAPPY;

public final class Table
        implements SeekingIterable<Slice, Slice>
{
    private static final Charset CHARSET = Charset.forName("UTF-8");
    private static final AtomicLong ID_GENERATOR = new AtomicLong();
    private final long id = ID_GENERATOR.incrementAndGet();
    private final Comparator<Slice> comparator;
    private final boolean verifyChecksums;
    private final Block indexBlock;
    private final BlockHandle metaindexBlockHandle;
    private final RandomInputFile source;
    private final ILRUCache<CacheKey, Slice> blockCache;
    private final FilterBlockReader filter;

    public Table(RandomInputFile source, Comparator<Slice> comparator, boolean verifyChecksum, ILRUCache<CacheKey, Slice> blockCache, final FilterPolicy filterPolicy)
            throws IOException
    {
        this.source = source;
        this.blockCache = blockCache;
        requireNonNull(source, "source is null");
        long size = source.size();
        checkArgument(size >= Footer.ENCODED_LENGTH, "File is corrupt: size must be at least %s bytes", Footer.ENCODED_LENGTH);
        requireNonNull(comparator, "comparator is null");

        this.verifyChecksums = verifyChecksum;
        this.comparator = comparator;
        final ByteBuffer footerData = source.read(size - Footer.ENCODED_LENGTH, Footer.ENCODED_LENGTH);

        Footer footer = Footer.readFooter(Slices.avoidCopiedBuffer(footerData));
        indexBlock = new Block(readRawBlock(footer.getIndexBlockHandle()), comparator); //no need for cache
        metaindexBlockHandle = footer.getMetaindexBlockHandle();
        this.filter = readMeta(filterPolicy);

    }

    private FilterBlockReader readMeta(FilterPolicy filterPolicy) throws IOException
    {
        if (filterPolicy == null) {
            return null;  // Do not need any metadata
        }

        final Block meta = new Block(readRawBlock(metaindexBlockHandle), new BytewiseComparator());
        final BlockIterator iterator = meta.iterator();
        final Slice targetKey = new Slice(("filter." + filterPolicy.name()).getBytes(CHARSET));
        iterator.seek(targetKey);
        if (iterator.hasNext() && iterator.peek().getKey().equals(targetKey)) {
            return readFilter(filterPolicy, iterator.next().getValue());
        }
        else {
            return null;
        }
    }

    protected FilterBlockReader readFilter(FilterPolicy filterPolicy, Slice filterHandle) throws IOException
    {
        final Slice filterBlock = readRawBlock(BlockHandle.readBlockHandle(filterHandle.input()));
        return new FilterBlockReader(filterPolicy, filterBlock);
    }

    @Override
    public TableIterator iterator()
    {
        return new TableIterator(this, indexBlock.iterator());
    }

    public FilterBlockReader getFilter()
    {
        return filter;
    }

    public Block openBlock(Slice blockEntry)
    {
        BlockHandle blockHandle = BlockHandle.readBlockHandle(blockEntry.input());
        Block dataBlock;
        try {
            dataBlock = readBlock(blockHandle);
        }
        catch (IOException e) {
            throw new DBException(e);
        }
        return dataBlock;
    }

    private Block readBlock(BlockHandle blockHandle)
            throws IOException
    {
        try {
            final Slice rawBlock = blockCache == null ? readRawBlock(blockHandle) : blockCache.load(new CacheKey(id, blockHandle), () -> readRawBlock(blockHandle));
            return new Block(rawBlock, comparator);
        }
        catch (ExecutionException e) {
            Throwables.propagateIfPossible(e.getCause(), IOException.class);
            throw new IOException(e.getCause());
        }
    }

    protected Slice readRawBlock(BlockHandle blockHandle)
            throws IOException
    {
        // read block trailer
        final ByteBuffer content = source.read(blockHandle.getOffset(), blockHandle.getDataSize() + BlockTrailer.ENCODED_LENGTH);
        content.mark().position(content.position() + blockHandle.getDataSize());
        final BlockTrailer blockTrailer = BlockTrailer.readBlockTrailer(Slices.avoidCopiedBuffer(content));

// todo re-enable crc check when ported to support direct buffers
//        // only verify check sums if explicitly asked by the user
//        if (verifyChecksums) {
//            // checksum data and the compression type in the trailer
//            PureJavaCrc32C checksum = new PureJavaCrc32C();
//            checksum.update(data.getRawArray(), data.getRawOffset(), blockHandle.getDataSize() + 1);
//            int actualCrc32c = checksum.getMaskedValue();
//
//            checkState(blockTrailer.getCrc32c() == actualCrc32c, "Block corrupted: checksum mismatch");
//        }

        // decompress data
        Slice uncompressedData;
        content.reset();
        content.limit(content.limit() - BlockTrailer.ENCODED_LENGTH);
        if (blockTrailer.getCompressionType() == SNAPPY) {
            int uncompressedLength = uncompressedLength(content);
            final ByteBuffer uncompressedScratch = ByteBuffer.allocateDirect(uncompressedLength);
            Snappy.uncompress(content, uncompressedScratch);
            uncompressedData = Slices.copiedBuffer(uncompressedScratch);
        }
        else {
            uncompressedData = Slices.avoidCopiedBuffer(content);
        }

        return uncompressedData;
    }

    public <T> T internalGet(Slice key, KeyValueFunction<T> keyValueFunction)
    {
        final BlockIterator iterator = indexBlock.iterator();
        iterator.seek(key);
        if (iterator.hasNext()) {
            final BlockEntry peek = iterator.peek();
            final Slice handleValue = peek.getValue();
            if (filter != null && !filter.keyMayMatch(BlockHandle.readBlockHandle(handleValue.input()).getOffset(), key)) {
                return null;
            }
            else {
                final BlockIterator iterator1 = openBlock(handleValue).iterator();
                iterator1.seek(key);
                if (iterator1.hasNext()) {
                    final BlockEntry next = iterator1.next();
                    return keyValueFunction.apply(next.getKey(), next.getValue());
                }
            }
        }
        return null;
    }

    private int uncompressedLength(ByteBuffer data)
    {
        return VariableLengthQuantity.readVariableLengthInt(data.duplicate());
    }

    /**
     * Given a key, return an approximate byte offset in the file where
     * the data for that key begins (or would begin if the key were
     * present in the file).  The returned value is in terms of file
     * bytes, and so includes effects like compression of the underlying data.
     * For example, the approximate offset of the last key in the table will
     * be close to the file length.
     */
    public long getApproximateOffsetOf(Slice key)
    {
        BlockIterator iterator = indexBlock.iterator();
        iterator.seek(key);
        if (iterator.hasNext()) {
            BlockHandle blockHandle = BlockHandle.readBlockHandle(iterator.next().getValue().input());
            return blockHandle.getOffset();
        }

        // key is past the last key in the file.  Approximate the offset
        // by returning the offset of the metaindex block (which is
        // right near the end of the file).
        return metaindexBlockHandle.getOffset();
    }

    @Override
    public String toString()
    {
        return "Table" +
                "{source='" + source + '\'' +
                ", comparator=" + comparator +
                ", verifyChecksums=" + verifyChecksums +
                '}';
    }

    public Callable<?> closer()
    {
        return new CloseableToCallable(source);
    }

    private static class CloseableToCallable implements Callable<Object>
    {
        private RandomInputFile source;

        public CloseableToCallable(RandomInputFile source)
        {
            this.source = source;
        }

        @Override
        public Object call() throws Exception
        {
            source.close();
            return null;
        }
    }
}
