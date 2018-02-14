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
package org.iq80.leveldb.util;

import org.iq80.leveldb.impl.InternalKey;
import org.iq80.leveldb.impl.SeekingIterator;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map.Entry;

public final class DbIterator implements SeekingIterator<InternalKey, Slice>, Closeable
{
    /*
     * NOTE: This code has been specifically tuned for performance of the DB
     * iterator methods.  Before committing changes to this code, make sure
     * that the performance of the DB benchmark with the following parameters
     * has not regressed:
     *
     *    --num=10000000 --benchmarks=fillseq,readrandom,readseq,readseq,readseq
     *
     * The code in this class purposely does not use the SeekingIterator
     * interface, but instead used the concrete implementations.  This is
     * because we want the hot spot compiler to inline the code from the
     * concrete iterators, and this can not happen with truly polymorphic
     * call-sites.  If a future version of hot spot supports inlining of truly
     * polymorphic call-sites, this code can be made much simpler.
     */
    private final MergingIterator mergingIterator;
    private final Runnable cleanup;

    public DbIterator(MergingIterator mergingIterator, Runnable cleanup)
    {
        this.mergingIterator = mergingIterator;
        this.cleanup = cleanup;
    }

    @Override
    public void close() throws IOException
    {
        //end user api is protected against multiple close
        try {
            mergingIterator.close();
        }
        finally {
            cleanup.run();
        }
    }

    @Override
    public void seekToFirst()
    {
        mergingIterator.seekToFirst();
    }

    @Override
    public void seek(InternalKey targetKey)
    {
        mergingIterator.seek(targetKey);
    }

    @Override
    public Entry<InternalKey, Slice> peek()
    {
        return mergingIterator.peek();
    }

    @Override
    public boolean hasNext()
    {
        return mergingIterator.hasNext();
    }

    @Override
    public Entry<InternalKey, Slice> next()
    {
        return mergingIterator.next();
    }

    @Override
    public void remove()
    {
        mergingIterator.remove();
    }
}
