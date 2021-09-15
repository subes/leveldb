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
package org.iq80.leveldb.compression;

import org.iq80.leveldb.CompressionType;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkPositionIndex;

/**
 * Compressions service that efficiently load compression algorithm implementation at first use.
 */
public class Compressions
{
    private Compressions()
    {
        //static factory
    }

    //load each compression, only if used, by static initialization
    private static Compression get(CompressionType compressionType)
    {
        switch (compressionType) {
            case SNAPPY:
                return Snappy.SNAPPY;
            case LZ4:
                return LZ4.LZ4FAST;
            case LZ4_HC:
                return LZ4.LZ4HC;
            case NONE:
            default:
                return null;
        }
    }

    private static final Decompressor DECOMPRESSOR = new Decompressor()
    {
        @Override
        public ByteBuffer uncompress(CompressionType compressionType, ByteBuffer compressed) throws IOException
        {
            final Compression compression = get(compressionType);
            if (compression == null) {
                throw new IOException("Unavailable compression: " + compressionType);
            }
            final int uncompressedLength = compression.uncompressedLength(compressed);
            final ByteBuffer uncompressedScratch = ByteBuffer.allocateDirect(uncompressedLength);
            compression.uncompress(compressed, uncompressedScratch);
            return uncompressedScratch;
        }
    };

    /**
     * Return {@code true} if compression implementation is loaded and can be used to compress/decompress data.
     */
    public static boolean isAvailable(CompressionType compressionType)
    {
        return get(compressionType) != null;
    }

    /**
     * Get a new compressor instance if available else throw {@link IllegalStateException}
     */
    public static Compressor requireCompressor(CompressionType compressionType)
    {
        final Compressor reference = tryToGetCompressor(compressionType);
        if (reference == null) {
            throw new IllegalStateException("Unavailable compression: " + compressionType);
        }
        return Objects.requireNonNull(reference);
    }

    /**
     * Get a new compressor instance if available else return {@code null}.
     */
    public static Compressor tryToGetCompressor(CompressionType compressionType)
    {
        final Compression compression = get(compressionType);
        if (compression == null) {
            return null;
        }
        return new CompressorAdapter(compressionType, compression);
    }

    /**
     * Get decompressor that will support all compression implementation available to leveldb at time of decompression.
     */
    public static Decompressor decompressor()
    {
        return DECOMPRESSOR;
    }

    private static final class CompressorAdapter implements Compressor
    {
        private final CompressionType compressionType;
        private final Compression compressor;

        public CompressorAdapter(CompressionType compressionType, Compression compressor)
        {
            this.compressionType = compressionType;
            this.compressor = compressor;
        }

        @Override
        public CompressionType type()
        {
            return compressionType;
        }

        @Override
        public int compress(byte[] input, int inputOffset, int length, byte[] output, int outputOffset) throws IOException
        {
            checkPositionIndex(inputOffset, input.length, "inputOffset");
            checkPositionIndex(outputOffset, output.length, "outputOffset");
            checkPositionIndex(inputOffset + length, input.length, "length");
            return compressor.compress(input, inputOffset, length, output, outputOffset);
        }

        @Override
        public int maxCompressedLength(int length)
        {
            return compressor.maxCompressedLength(length);
        }
    }
}
