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

import org.iq80.leveldb.util.MMRandomInputFile;
import org.iq80.leveldb.util.MMWritableFile;
import org.iq80.leveldb.util.RandomInputFile;
import org.iq80.leveldb.util.SequentialFile;
import org.iq80.leveldb.util.SequentialFileImpl;
import org.iq80.leveldb.util.Slice;
import org.iq80.leveldb.util.UnbufferedRandomInputFile;
import org.iq80.leveldb.util.UnbufferedWritableFile;
import org.iq80.leveldb.util.WritableFile;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

public class EnvImpl implements Env
{
    private static final int PAGE_SIZE = 1024 * 1024;
    private final MmapLimiter mmapLimiter;

    private EnvImpl(MmapLimiter mmapLimiter)
    {
        this.mmapLimiter = mmapLimiter;
    }

    public static Env createEnv()
    {
        return new EnvImpl(MmapLimiter.defaultLimiter());
    }

    public static Env createEnv(MmapLimiter mmapLimiter)
    {
        return new EnvImpl(mmapLimiter);
    }

    @Override
    public long nowMicros()
    {
        return TimeUnit.NANOSECONDS.toMicros(System.nanoTime());
    }

    @Override
    public SequentialFile newSequentialFile(File file) throws IOException
    {
        return SequentialFileImpl.open(file);
    }

    @Override
    public RandomInputFile newRandomAccessFile(File file) throws IOException
    {
        if (mmapLimiter.acquire()) {
            try {
                return new DelegateRandomInputFile(mmapLimiter, MMRandomInputFile.open(file));
            }
            catch (IOException e) {
                mmapLimiter.release();
                throw e;
            }
        }
        return UnbufferedRandomInputFile.open(file);
    }

    @Override
    public WritableFile newWritableFile(File file) throws IOException
    {
        if (mmapLimiter.acquire()) {
            try {
                return new DelegateWritableFile(mmapLimiter, MMWritableFile.open(file, PAGE_SIZE));
            }
            catch (IOException e) {
                mmapLimiter.release();
                throw e;
            }
        }
        return UnbufferedWritableFile.open(file);
    }

    @Override
    public WritableFile newAppendableFile(File file) throws IOException
    {
        return UnbufferedWritableFile.open(file);
    }

    private static class DelegateRandomInputFile implements RandomInputFile
    {
        private final MmapLimiter mmapLimiter;
        private final RandomInputFile open;

        DelegateRandomInputFile(MmapLimiter mmapLimiter, RandomInputFile open)
        {
            this.mmapLimiter = mmapLimiter;
            this.open = open;
        }

        @Override
        public long size()
        {
            return open.size();
        }

        @Override
        public ByteBuffer read(long offset, int length) throws IOException
        {
            return open.read(offset, length);
        }

        @Override
        public void close() throws IOException
        {
            try {
                open.close();
            }
            finally {
                mmapLimiter.release();
            }
        }
    }

    private static class DelegateWritableFile implements WritableFile
    {
        private final MmapLimiter mmapLimiter;
        private final WritableFile open;

        DelegateWritableFile(MmapLimiter mmapLimiter, WritableFile open)
        {
            this.mmapLimiter = mmapLimiter;
            this.open = open;
        }

        @Override
        public void append(Slice data) throws IOException
        {
            open.append(data);
        }

        @Override
        public void force() throws IOException
        {
            open.force();
        }

        @Override
        public void close() throws IOException
        {
            try {
                open.close();
            }
            finally {
                mmapLimiter.release();
            }
        }
    }
}
