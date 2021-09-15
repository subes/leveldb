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

/**
 * Internal leveldb decompressor interface.
 */
public interface Decompressor
{
    /**
     * Uncompress {@code compressed} bytes with {@code compressionType} compression implementation.
     * If compression is not available, throw an {@link IOException}.
     *
     * @param compressionType type of compression to use to uncompress data. {@link CompressionType#NONE} is unsupported
     * @param compressed      compressed data
     * @return a new {@link ByteBuffer} with size and uncompressed data
     * @throws IOException if compression implementation is not available or compressed data is corrupted
     */
    ByteBuffer uncompress(CompressionType compressionType, ByteBuffer compressed)
            throws IOException;
}
