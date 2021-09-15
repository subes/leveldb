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

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Compression Service Interface that must be implemented to serve a given Compression Implementation.
 * @see Compressions
 * @see org.iq80.leveldb.CompressionType
 */
public interface Compression
{
    /**
     * Uncompress {@code compressed} to {@code uncompressed} that have exactly the size of original data to be
     * uncompressed.
     * @param compressed compressed bytes
     * @param uncompressed uncompressed buffer with exactly the size of uncompressed data
     * @throws IOException if any data corruption occurs
     */
    void uncompress(ByteBuffer compressed, ByteBuffer uncompressed)
            throws IOException;

    /**
     * Compress {@code length} bytes from {@code input} starting at {@code inputOffset}. Compressed byte must be written to
     * {@code output} starting at offset {@code outputOffset}.
     *
     * {@link Compression} must be able to recover original {@code length} from compressed data written into {@code output}.
     *
     * @param input input array
     * @param inputOffset input array data offset
     * @param length input data length to compress
     * @param output output array to write compressed data to. Size must be at least equal to {@link #maxCompressedLength(int)} + {@code outputOffset} of input length.
     * @param outputOffset output array offset
     * @return number of bytes written to {@code output}
     * @throws IOException if any data corruption occurs
     */
    int compress(byte[] input, int inputOffset, int length, byte[] output, int outputOffset)
            throws IOException;

    /**
     * Returns the maximum compressed length for an input of size {@code length}.
     *
     * @param length the input size in bytes
     * @return the maximum compressed length in bytes
     */
    int maxCompressedLength(int length);

    /**
     * Returns the exact size of uncompressed (original) data present in compressed data present in {@code compressed}.
     * @param compressed compressed byte buffer
     * @return return the exact length in bytes of original data present in {@code compressed} {@link ByteBuffer}
     */
    int uncompressedLength(ByteBuffer compressed);
}
