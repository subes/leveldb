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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import org.iq80.leveldb.impl.InternalKey;
import org.iq80.leveldb.impl.InternalKeyComparator;
import org.iq80.leveldb.impl.ValueType;
import org.iq80.leveldb.table.BytewiseComparator;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class MergingIteratorTest
{
    @Test
    public void testMergeSeek()
    {
        InternalKeyComparator comparator = new InternalKeyComparator(new BytewiseComparator());
        InternalKey key1 = new InternalKey(Slices.wrappedBuffer(new byte[] {'1'}), 1, ValueType.VALUE);
        InternalKey key2 = new InternalKey(Slices.wrappedBuffer(new byte[] {'3'}), 2, ValueType.VALUE);
        InternalKey key3 = new InternalKey(Slices.wrappedBuffer(new byte[] {'2'}), 3, ValueType.VALUE);
        InternalKey key4 = new InternalKey(Slices.wrappedBuffer(new byte[] {'4'}), 4, ValueType.VALUE);
        ImmutableList<Map.Entry<InternalKey, Slice>> of1 = ImmutableList
                .of(
                        Maps.immutableEntry(key1, Slices.EMPTY_SLICE),
                        Maps.immutableEntry(key2, Slices.EMPTY_SLICE)
                );
        ImmutableList<Map.Entry<InternalKey, Slice>> of2 = ImmutableList
                .of(
                        Maps.immutableEntry(key3, Slices.EMPTY_SLICE),
                        Maps.immutableEntry(key4, Slices.EMPTY_SLICE)
                );
        ImmutableList<InternalIterator> of = ImmutableList.of(new InternalKeySliceAbstractSeekingIterator(of1, comparator), new InternalKeySliceAbstractSeekingIterator(of2, comparator));
        MergingIterator mergingIterator = new MergingIterator(of, comparator);
        assertEquals(mergingIterator.next().getKey(), key1);
        assertEquals(mergingIterator.next().getKey(), key3);
        mergingIterator.seekToFirst();
        assertEquals(mergingIterator.next().getKey(), key1);
        assertEquals(mergingIterator.next().getKey(), key3);
        assertEquals(mergingIterator.next().getKey(), key2);
        assertEquals(mergingIterator.next().getKey(), key4);
        assertFalse(mergingIterator.hasNext());
        mergingIterator.seek(key2);
        assertEquals(mergingIterator.next().getKey(), key2);
        assertEquals(mergingIterator.next().getKey(), key4);
        assertFalse(mergingIterator.hasNext());
    }

    private static class InternalKeySliceAbstractSeekingIterator extends AbstractSeekingIterator<InternalKey, Slice> implements InternalIterator
    {
        int index = 0;
        private final List<Map.Entry<InternalKey, Slice>> entries;
        private final InternalKeyComparator comparator;

        public InternalKeySliceAbstractSeekingIterator(List<Map.Entry<InternalKey, Slice>> entries, InternalKeyComparator comparator)
        {
            this.entries = entries;
            this.comparator = comparator;
        }

        @Override
        protected void seekToFirstInternal()
        {
            index = 0;
        }

        @Override
        protected void seekInternal(InternalKey targetKey)
        {
            index = entries.size();
            for (int i = 0; i < entries.size(); i++) {
                Map.Entry<InternalKey, Slice> entry = entries.get(i);
                if (comparator.compare(entry.getKey(), targetKey) >= 0) {
                    index = i;
                }
            }
        }

        @Override
        protected Map.Entry<InternalKey, Slice> getNextElement()
        {
            return index < entries.size() ? entries.get(index++) : null;
        }

        @Override
        public void close() throws IOException
        {
        }
    }
}
