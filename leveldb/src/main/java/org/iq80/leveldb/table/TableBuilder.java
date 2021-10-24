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

import org.iq80.leveldb.CompressionType;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.compression.Compressor;
import org.iq80.leveldb.env.WritableFile;
import org.iq80.leveldb.util.PureJavaCrc32C;
import org.iq80.leveldb.util.Slice;
import org.iq80.leveldb.util.Slices;

import java.io.IOException;
import java.nio.charset.Charset;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class TableBuilder
{
    /**
     * TABLE_MAGIC_NUMBER was picked by running
     * echo http://code.google.com/p/leveldb/ | sha1sum
     * and taking the leading 64 bits.
     */
    public static final long TABLE_MAGIC_NUMBER = 0xdb4775248b80fb57L;
    private static final Charset CHARSET = Charset.forName("UTF-8");

    private final int blockRestartInterval;
    private final int blockSize;
    private final Compressor compressor;

    private final WritableFile file;
    private final BlockBuilder dataBlockBuilder;
    private final BlockBuilder indexBlockBuilder;
    private final FilterBlockBuilder filterPolicyBuilder;
    private Slice lastKey;
    private final UserComparator userComparator;

    private long entryCount;

    // Either Finish() or Abandon() has been called.
    private boolean closed;

    // We do not emit the index entry for a block until we have seen the
    // first key for the next data block.  This allows us to use shorter
    // keys in the index block.  For example, consider a block boundary
    // between the keys "the quick brown fox" and "the who".  We can use
    // "the r" as the key for the index block entry since it is >= all
    // entries in the first block and < all entries in subsequent
    // blocks.
    private boolean pendingIndexEntry;
    private BlockHandle pendingHandle;  // Handle to add to index block

    private Slice compressedOutput;

    private long position;

    public TableBuilder(Options options, WritableFile file, UserComparator userComparator, Compressor selectedCompressor)
    {
        requireNonNull(options, "options is null");
        requireNonNull(file, "file is null");

        this.file = file;
        this.userComparator = userComparator;

        blockRestartInterval = options.blockRestartInterval();
        blockSize = options.blockSize();
        compressor = selectedCompressor;

        dataBlockBuilder = new BlockBuilder((int) Math.min(blockSize * 1.1, options.maxFileSize()), blockRestartInterval, userComparator);

        // with expected 50% compression
        int expectedNumberOfBlocks = 1024;
        indexBlockBuilder = new BlockBuilder(BlockHandle.MAX_ENCODED_LENGTH * expectedNumberOfBlocks, 1, userComparator);

        lastKey = Slices.EMPTY_SLICE;

        if (options.filterPolicy() != null) {
            filterPolicyBuilder = new FilterBlockBuilder((FilterPolicy) options.filterPolicy());
            filterPolicyBuilder.startBlock(0);
        }
        else {
            filterPolicyBuilder = null;
        }
    }

    public long getEntryCount()
    {
        return entryCount;
    }

    public long getFileSize()
    {
        return position;
    }

    public void add(BlockEntry blockEntry)
            throws IOException
    {
        requireNonNull(blockEntry, "blockEntry is null");
        add(blockEntry.getKey(), blockEntry.getValue());
    }

    public void add(Slice key, Slice value)
            throws IOException
    {
        requireNonNull(key, "key is null");
        requireNonNull(value, "value is null");

        checkState(!closed, "table is finished");

        if (entryCount > 0) {
            assert (userComparator.compare(key, lastKey) > 0) : "key must be greater than last key";
        }

        // If we just wrote a block, we can now add the handle to index block
        if (pendingIndexEntry) {
            checkState(dataBlockBuilder.isEmpty(), "Internal error: Table has a pending index entry but data block builder is empty");

            Slice shortestSeparator = userComparator.findShortestSeparator(lastKey, key);

            Slice handleEncoding = BlockHandle.writeBlockHandle(pendingHandle);
            indexBlockBuilder.add(shortestSeparator, handleEncoding);
            pendingIndexEntry = false;
        }

        if (filterPolicyBuilder != null) {
            filterPolicyBuilder.addKey(key);
        }

        lastKey = key;
        entryCount++;
        dataBlockBuilder.add(key, value);

        int estimatedBlockSize = dataBlockBuilder.currentSizeEstimate();
        if (estimatedBlockSize >= blockSize) {
            flush();
        }
    }

    private void flush()
            throws IOException
    {
        checkState(!closed, "table is finished");
        if (dataBlockBuilder.isEmpty()) {
            return;
        }

        checkState(!pendingIndexEntry, "Internal error: Table already has a pending index entry to flush");

        pendingHandle = writeBlock(dataBlockBuilder);

        if (filterPolicyBuilder != null) {
            filterPolicyBuilder.startBlock(position);
        }

        pendingIndexEntry = true;
    }

    private BlockHandle writeBlock(BlockBuilder blockBuilder)
            throws IOException
    {
        // close the block
        Slice raw = blockBuilder.finish();
        BlockHandle blockHandle = writeRawBlock(raw);

        // clean up state
        blockBuilder.reset();

        return blockHandle;
    }

    private BlockHandle writeRawBlock(Slice raw) throws IOException
    {
        // attempt to compress the block
        Slice blockContents = raw;
        CompressionType blockCompressionType = CompressionType.NONE;
        if (compressor != null) {
            try {
                ensureCompressedOutputCapacity(compressor.maxCompressedLength(raw.length()));
                int compressedSize = compressor.compress(raw.getRawArray(), raw.getRawOffset(), raw.length(), compressedOutput.getRawArray(), 0);

                // Don't use the compressed data if compressed less than 12.5%,
                if (compressedSize < raw.length() - (raw.length() / 8)) {
                    blockContents = compressedOutput.slice(0, compressedSize);
                    blockCompressionType = compressor.type();
                }
            }
            catch (IOException ignored) {
                // compression failed, so just store uncompressed form
            }
        }

        // create block trailer
        BlockTrailer blockTrailer = new BlockTrailer(blockCompressionType, crc32c(blockContents, blockCompressionType));
        Slice trailer = BlockTrailer.writeBlockTrailer(blockTrailer);

        // create a handle to this block
        BlockHandle blockHandle = new BlockHandle(position, blockContents.length());

        // write data and trailer
        file.append(blockContents);
        file.append(trailer);
        position += blockContents.length() + trailer.length();
        return blockHandle;
    }

    public void finish()
            throws IOException
    {
        checkState(!closed, "table is finished");

        // flush current data block
        flush();

        // mark table as closed
        closed = true;

        BlockHandle filterBlockHandle = null;

        if (filterPolicyBuilder != null) {
            filterBlockHandle = writeRawBlock(filterPolicyBuilder.finish());
        }

        // write (empty) meta index block
        BlockBuilder metaIndexBlockBuilder = new BlockBuilder(256, blockRestartInterval, new BytewiseComparator());

        if (filterBlockHandle != null) {
            metaIndexBlockBuilder.add(new Slice(("filter." + filterPolicyBuilder.name()).getBytes(CHARSET)), BlockHandle.writeBlockHandle(filterBlockHandle));
        }

        BlockHandle metaindexBlockHandle = writeBlock(metaIndexBlockBuilder);

        // add last handle to index block
        if (pendingIndexEntry) {
            Slice shortSuccessor = userComparator.findShortSuccessor(lastKey);

            Slice handleEncoding = BlockHandle.writeBlockHandle(pendingHandle);
            indexBlockBuilder.add(shortSuccessor, handleEncoding);
            pendingIndexEntry = false;
        }

        // write index block
        BlockHandle indexBlockHandle = writeBlock(indexBlockBuilder);

        // write footer
        Footer footer = new Footer(metaindexBlockHandle, indexBlockHandle);
        Slice footerEncoding = Footer.writeFooter(footer);
        file.append(footerEncoding);
        position += footerEncoding.length();
    }

    public void abandon()
    {
        closed = true; //mark it as unusable
    }

    public static int crc32c(Slice data, CompressionType type)
    {
        PureJavaCrc32C crc32c = new PureJavaCrc32C();
        crc32c.update(data.getRawArray(), data.getRawOffset(), data.length());
        crc32c.update(type.persistentId() & 0xFF);
        return crc32c.getMaskedValue();
    }

    public void ensureCompressedOutputCapacity(int capacity)
    {
        if (compressedOutput != null && compressedOutput.length() > capacity) {
            return;
        }
        compressedOutput = Slices.allocate(capacity);
    }
}
