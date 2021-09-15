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
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import org.testng.internal.junit.ArrayAsserts;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.function.IntFunction;

import static org.testng.Assert.assertEquals;

public class CompressionTest
{
    private static final byte[] DATA = "+XHfZF$g5j]@8,NrD85:7WM$=9LiNAydjtfMC;H*()(Ct]czTfM]U_[HZ-2xqx(a(wddjBH92zq&,+fn:$igT)V;[fhi$ma.z:q6VGShwp?Td3b9SV6Hb}4?bBi6$_/mkP{Kunfm-PmLz(==iK,E$bW?RdHxBbVG$d!d%Y28dS?5B$B-Lwyx2uE$M4A6D=w,_]DMGjk]".getBytes(StandardCharsets.UTF_8);

    private static final IntFunction<ByteBuffer> DIRECT_LE = cap -> ByteBuffer.allocateDirect(cap).order(ByteOrder.LITTLE_ENDIAN);
    private static final IntFunction<ByteBuffer> DIRECT_BE = cap -> ByteBuffer.allocateDirect(cap).order(ByteOrder.BIG_ENDIAN);
    private static final IntFunction<ByteBuffer> HEAP = cap -> ByteBuffer.allocate(cap);

    @DataProvider(name = "testArgs")
    public Object[][] testArgsProvider()
    {
        List<Object[]> args = new ArrayList<>();
        for (CompressionType value : CompressionType.values()) {
            if (value == CompressionType.NONE) {
                continue;
            }
            for (IntFunction<ByteBuffer> f : new IntFunction[] {DIRECT_LE, DIRECT_BE, HEAP}) {
                args.add(new Object[] {value, f});
            }
        }

        return args.toArray(new Object[0][]);
    }

    @Test(dataProvider = "testArgs")
    public void testCompressUncompressAlg(CompressionType compressionType, IntFunction<ByteBuffer> f) throws IOException
    {
        final Compressor compress = Compressions.tryToGetCompressor(compressionType);
        Assert.assertNotNull(compress);
        final byte[] compressedBytes = compress(compress, DATA);
        final ByteBuffer compressedBuf = fillBuffer(compressedBytes, f.apply(compressedBytes.length), 0);
        assertBufferBoundaries(compressedBuf, 0, compressedBytes.length);
        final ByteBuffer uncompressedBuf = Compressions.decompressor().uncompress(compressionType, compressedBuf);
        assertBufferBoundaries(uncompressedBuf, 0, DATA.length);
        final byte[] uncompressedBytes = new byte[DATA.length];
        uncompressedBuf.get(uncompressedBytes);
        ArrayAsserts.assertArrayEquals(DATA, uncompressedBytes);
    }

    private void assertBufferBoundaries(ByteBuffer buf, int position, int length)
    {
        assertEquals(buf.position(), position);
        assertEquals(buf.remaining(), length);
    }

    private byte[] compress(Compressor compress, byte[] bytes) throws IOException
    {
        final byte[] bytes1 = new byte[2000];
        final int compressedBytes = compress.compress(bytes, 0, bytes.length, bytes1, 0);
        final byte[] compressedData = new byte[compressedBytes];
        System.arraycopy(bytes1, 0, compressedData, 0, compressedBytes);
        return compressedData;
    }

    private static ByteBuffer fillBuffer(byte[] data, ByteBuffer byteBuffer, int initialPos)
    {
        byteBuffer.position(initialPos);
        byteBuffer.put(data);
        byteBuffer.position(initialPos);
        byteBuffer.limit(initialPos + data.length);
        return byteBuffer;
    }
}
