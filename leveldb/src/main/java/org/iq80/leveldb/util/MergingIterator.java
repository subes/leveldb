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

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import java.util.Queue;

public final class MergingIterator implements SeekingIterator<InternalKey, Slice>
{
    private final List<? extends SeekingIterator<InternalKey, Slice>> iterators;
    private final Queue<SeekingIterator<InternalKey, Slice>> queue;

    public MergingIterator(List<? extends SeekingIterator<InternalKey, Slice>> iterators, Comparator<InternalKey> comparator)
    {
        Comparator<SeekingIterator<InternalKey, Slice>> heapComparator = (o1, o2) -> comparator.compare(o1.peek().getKey(), o2.peek().getKey());
        this.queue = new PriorityQueue<>(iterators.size(), heapComparator);
        this.iterators = iterators;
        rebuildQueue();
    }

    private void rebuildQueue()
    {
        queue.clear();
        for (SeekingIterator<InternalKey, Slice> iterator : iterators) {
            if (iterator.hasNext()) {
                queue.add(iterator);
            }
        }
    }

    @Override
    public void seekToFirst()
    {
        for (SeekingIterator<InternalKey, Slice> iterator : this.iterators) {
            iterator.seekToFirst();
        }
        rebuildQueue();
    }

    @Override
    public void seek(InternalKey targetKey)
    {
        for (SeekingIterator<InternalKey, Slice> iterator : this.iterators) {
            iterator.seek(targetKey);
        }
        rebuildQueue();
    }

    @Override
    public Entry<InternalKey, Slice> peek()
    {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        return queue.peek().peek();
    }

    @Override
    public boolean hasNext()
    {
        return !queue.isEmpty();
    }

    @Override
    public Entry<InternalKey, Slice> next()
    {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        SeekingIterator<InternalKey, Slice> nextIter = queue.remove();
        Entry<InternalKey, Slice> next = nextIter.next();
        if (nextIter.hasNext()) {
            queue.add(nextIter);
        }
        return next;
    }

    @Override
    public void remove()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws IOException
    {
        Closeables.closeAll(iterators);
    }
}
