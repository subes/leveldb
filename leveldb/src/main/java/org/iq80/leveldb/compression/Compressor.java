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

public interface Compressor
{
    /**
     * Return the type of compression implemented by this compressor.
     */
    CompressionType type();

    /**
     * Compress exactly {@code length} bytes from index {@code inputOffset} of {@code input}. Write output to {@code output}
     * from {@code outputOffset} array index. Make sure {@code output} is at least equal to {@link #maxCompressedLength(int)} with {@code length} as argument.
     *
     * @param input byte source array
     * @param inputOffset source offset
     * @param length length of bytes to read from {@code input}
     * @param output compressed bytes output array
     * @param outputOffset output offset point from where {@code output} must be written.
     * @return the number of bytes written to {@code output}
     * @throws IOException
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
}
