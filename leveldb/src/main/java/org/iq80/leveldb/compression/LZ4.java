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

import org.iq80.leveldb.util.VariableLengthQuantity;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.function.Function;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

import static org.iq80.leveldb.compression.LoadCompressionUtil.loadCompression;

/**
 * <p>
 * LZ4 abstraction for lz4-java implementation. Both {@link org.iq80.leveldb.CompressionType#LZ4} and
 * {@link org.iq80.leveldb.CompressionType#LZ4_HC} use java save implementations {@link LZ4Factory#safeInstance}.
 * <p>
 * You can change the {@link org.iq80.leveldb.CompressionType#LZ4} compression implementation by setting the 'leveldb.lz4fast' system property.
 * Example:
 * <p/>
 * <code>
 * -Dleveldb.lz4fast=org.iq80.leveldb.compression.LZ4$Lz4FastSpi
 * </code>
 * <p/>
 * You can also change the {@link org.iq80.leveldb.CompressionType#LZ4_HC} compression implementation by setting the 'leveldb.lz4hc' system property.
 * Example:
 * <p/>
 * <code>
 * -Dleveldb.lz4hc=org.iq80.leveldb.compression.LZ4$Lz4HcSpi
 * </code>
 * <p/>
 * The class referred by those properties must implement {@link Compression} interface.
 * </p>
 */
final class LZ4
{
    public static final Compression LZ4FAST = loadCompression("leveldb.lz4fast", "org.iq80.leveldb.compression.LZ4$Lz4FastCompression");
    public static final Compression LZ4HC = loadCompression("leveldb.lz4hc", "org.iq80.leveldb.compression.LZ4$Lz4HcCompression");

    private LZ4()
    {
    }

    private abstract static class Lz4Compression implements Compression
    {
        public final LZ4Compressor lz4Compressor;
        public final LZ4FastDecompressor lz4Decompressor;

        public Lz4Compression(LZ4Factory factory, Function<LZ4Factory, LZ4Compressor> compressor)
        {
            lz4Compressor = Objects.requireNonNull(compressor.apply(factory));
            lz4Decompressor = factory.fastDecompressor();
        }

        @Override
        public void uncompress(ByteBuffer compressed, ByteBuffer uncompressed)
        {
            final int uncompressedPosition = uncompressed.position();
            int uncompressedBytes = VariableLengthQuantity.readVariableLengthInt(compressed);
            assert uncompressedBytes == uncompressed.remaining() : "Buffer is expected to be the same size as the original text";
            lz4Decompressor.decompress(compressed, compressed.position(), uncompressed, uncompressed.position(), uncompressed.remaining());
            uncompressed.position(uncompressedPosition);
        }

        @Override
        public int compress(byte[] input, int inputOffset, int length, byte[] output, int outputOffset) throws IOException
        {
            final int prefixSize = VariableLengthQuantity.writeVariableLengthInt(length, output, outputOffset);
            return lz4Compressor.compress(input, inputOffset, length, output, outputOffset + prefixSize) + prefixSize;
        }

        @Override
        public int maxCompressedLength(int length)
        {
            return lz4Compressor.maxCompressedLength(length) + 5 /*variable length prefix*/;
        }

        @Override
        public int uncompressedLength(ByteBuffer compressed)
        {
            return VariableLengthQuantity.readVariableLengthInt(compressed.duplicate());
        }
    }

    static class Lz4FastCompression extends Lz4Compression
    {
        public Lz4FastCompression()
        {
            super(LZ4Factory.fastestJavaInstance(), LZ4Factory::highCompressor);
        }
    }

    static class Lz4HcCompression extends Lz4Compression
    {
        public Lz4HcCompression()
        {
            super(LZ4Factory.fastestJavaInstance(), LZ4Factory::fastCompressor);
        }

    }
}
