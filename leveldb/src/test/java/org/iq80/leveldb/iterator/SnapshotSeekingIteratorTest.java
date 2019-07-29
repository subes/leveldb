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
package org.iq80.leveldb.iterator;

import com.google.common.collect.Maps;
import org.iq80.leveldb.impl.InternalKey;
import org.iq80.leveldb.impl.InternalKeyComparator;
import org.iq80.leveldb.impl.ValueType;
import org.iq80.leveldb.table.BytewiseComparator;
import org.iq80.leveldb.table.UserComparator;
import org.iq80.leveldb.util.Slice;
import org.iq80.leveldb.util.Slices;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.function.Function;

import static org.iq80.leveldb.util.TestUtils.asciiToBytes;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class SnapshotSeekingIteratorTest
{
    @Test
    public void testPrefixWithDeleteValues() throws Exception
    {
        UserComparator userComparator = new BytewiseComparator();
        List<Map.Entry<InternalKey, Slice>> data = new CollectionBuilder(userComparator)
            .add("aa12", "valueHold")
            .add("aa12", "newValue")
            .add("bb11", "bb11Value")
            .add("bb12", "vv12Value")
            .add("cc11", "cc11Value")
            .add("cc11", "cc11Value2")
            .remove("cc11")
            .add("cc11", "ccValue")
            .remove("cc11")
            .add("cc11", "ccValue")
            .remove("cc11")
            .build();

        List<Map.Entry<InternalKey, Slice>> expectResultWithPrefix = new CollectionBuilder(userComparator)
            .add("bb11", "bb11Value")
            .add("bb12", "vv12Value")
            .build();

        {
            int readCount1 = readIteratorWithPrefix(userComparator, data, expectResultWithPrefix, "bb", true, "bb", SeekingIterator::next);
            int readCount2 = readIteratorWithPrefix(userComparator, data, expectResultWithPrefix, "bb", false, "bb", SeekingIterator::next);

            assertEquals(readCount1, 3); // seek + 1 next  + 1 next reaching undesired entry by prefix
            assertEquals(readCount2, 10); //seek + 1 next + 7 hidden entries + end;
        }
        {
            int readCountImproved = readIteratorWithPrefix(userComparator, data, expectResultWithPrefix, "b", true, "bb12", SeekingIterator::prev);
            int readWithoutHint = readIteratorWithPrefix(userComparator, data, expectResultWithPrefix, "b", false, "bb12", SeekingIterator::prev);
            assertEquals(readCountImproved, 3); // 1 seek + 1 prev + 1 prev reaching undesired entry by prefix
            assertEquals(readWithoutHint, 5);
        }
    }

    @Test
    public void testPrefixWithoutDeleteValues() throws Exception
    {
        UserComparator userComparator = new BytewiseComparator();
        List<Map.Entry<InternalKey, Slice>> data = new CollectionBuilder(userComparator)
            .add("aa12", "valueHold")
            .add("aa12", "newValue")
            .add("bb11", "aaValue")
            .add("cc11", "ccValue")
            .add("cc11", "ccValue")
            .remove("cc11")
            .add("cc11", "ccValue")
            .remove("cc11")
            .add("cc11", "ccValue")
            .remove("cc11")
            .add("mm00", "valuemm00")
            .add("mm01", "valuemm01")
            .add("uu1", "valueuu1")
            .build();

        List<Map.Entry<InternalKey, Slice>> expectResultWithPrefix = new CollectionBuilder(userComparator)
            .add("mm00", "valuemm00")
            .add("mm01", "valuemm01")
            .build();

        {
            int readCountImproved = readIteratorWithPrefix(userComparator, data, expectResultWithPrefix, "mm", true, "mm", SeekingIterator::next);
            int readWithoutHint = readIteratorWithPrefix(userComparator, data, expectResultWithPrefix, "mm", false, "mm", SeekingIterator::next);
            assertEquals(readCountImproved, 3); // 1 seek + 1 next + 1 next reaching undesired entry by prefix
            assertEquals(readWithoutHint, 4); // 1 seek + 1 next + 1 next reaching undesired entry by prefix + one last check next
        }
        {
            int readCountImproved = readIteratorWithPrefix(userComparator, data, expectResultWithPrefix, "mm", true, "mm01", SeekingIterator::prev);
            int readWithoutHint = readIteratorWithPrefix(userComparator, data, expectResultWithPrefix, "mm", false, "mm01", SeekingIterator::prev);
            assertEquals(readCountImproved, 3); // 1 seek + 1 prev + 1 prev reaching undesired entry by prefix
            assertEquals(readWithoutHint, 13); // 1 seek + 1 next + 1 next reaching undesired entry by prefix + one last check next
        }
    }

    @Test
    public void testAtTheEndAndBegining() throws Exception
    {
        UserComparator userComparator = new BytewiseComparator();
        List<Map.Entry<InternalKey, Slice>> data = new CollectionBuilder(userComparator)
            .add("ab1", "v1")
            .add("uu1", "valueuu1")
            .build();
        List<Map.Entry<InternalKey, Slice>> expectedNext = new CollectionBuilder(userComparator).add("uu1", "valueuu1").build();
        int readCountImproved = readIteratorWithPrefix(userComparator, data, expectedNext, "uu", true, "uu01", SeekingIterator::next);
        int readWithoutHint = readIteratorWithPrefix(userComparator, data, expectedNext, "uu", false, "uu01", SeekingIterator::next);
        assertEquals(readCountImproved, readWithoutHint);

        List<Map.Entry<InternalKey, Slice>> expectedPrev = new CollectionBuilder(userComparator).add("ab1", "v1").build();
        readCountImproved = readIteratorWithPrefix(userComparator, data, expectedPrev, "a", true, "aa", SeekingIterator::prev);
        readWithoutHint = readIteratorWithPrefix(userComparator, data, expectedPrev, "a", false, "aa", SeekingIterator::prev);
        assertEquals(readCountImproved, readWithoutHint);
    }

    /**
     * iterate over DB iterator a check returned object with a specific prefix match expected set
     *
     * @return number of read operations delegated to source iterator
     */
    private int readIteratorWithPrefix(
        UserComparator userComparator,
        List<Map.Entry<InternalKey, Slice>> data,
        List<Map.Entry<InternalKey, Slice>> expectedList,
        String prefixStr,
        boolean useIteratoPrefixHint,
        String seekEntry,
        Function<SeekingIterator, Boolean> nextPrev
    ) throws Exception
    {
        InternSortedListIterator putData = new InternSortedListIterator(data, userComparator);
        Slice prefix = Slices.wrappedBuffer(asciiToBytes(prefixStr));
        SnapshotSeekingIterator snapshotSeekingIterator = new SnapshotSeekingIterator(putData, 100, userComparator, useIteratoPrefixHint ? prefix : null, (internalKey, bytes) -> {
        });

        Slice seekSlice = new Slice(asciiToBytes(seekEntry));
        //advance iterator and verify it has expected content
        InternSortedListIterator expected = new InternSortedListIterator(expectedList, userComparator);
        Assert.assertEquals(snapshotSeekingIterator.valid(), expected.valid());
        boolean valid = snapshotSeekingIterator.seek(seekSlice);
        boolean seek = expected.seek(new InternalKey(seekSlice, Integer.MAX_VALUE, ValueType.VALUE));
        assertEquals(valid, seek);
        //we are just interested in key with expected prefix
        Callable<Boolean> moveIterator =
            () -> snapshotSeekingIterator.valid() && nextPrev.apply(snapshotSeekingIterator) && userComparator.startWith(snapshotSeekingIterator.key(), prefix);
        while (valid) {
            assertEquals(snapshotSeekingIterator.key(), expected.key().getUserKey());
            assertEquals(snapshotSeekingIterator.value(), expected.value());
            valid = nextPrev.apply(expected);
            assertEquals(moveIterator.call().booleanValue(), valid);
        }
        assertFalse(moveIterator.call().booleanValue());

        return putData.getOpCount();
    }

    private static class InternSortedListIterator
        extends SortedCollectionIterator<Map.Entry<InternalKey, Slice>, InternalKey, Slice>
        implements InternalIterator
    {
        //number of times read operation has been called
        private int opCount = 0;

        InternSortedListIterator(List<Map.Entry<InternalKey, Slice>> entries, UserComparator comparator)
        {
            super(entries, internalKeySliceEntry -> internalKeySliceEntry.getKey(), internalKeySliceEntry -> internalKeySliceEntry.getValue(), new InternalKeyComparator(comparator));
        }

        @Override
        protected boolean internalNext(boolean switchDirection)
        {
            opCount++;
            return super.internalNext(switchDirection);
        }

        @Override
        protected boolean internalPrev(boolean switchDirection)
        {
            opCount++;
            return super.internalPrev(switchDirection);
        }

        @Override
        protected boolean internalSeekToFirst()
        {
            opCount++;
            return super.internalSeekToFirst();
        }

        @Override
        protected boolean internalSeekToLast()
        {
            opCount++;
            return super.internalSeekToLast();
        }

        @Override
        protected boolean internalSeek(InternalKey targetKey)
        {
            opCount++;
            return super.internalSeek(targetKey);
        }

        public int getOpCount()
        {
            return opCount;
        }
    }

    // build entries as if they where added sequencialy to DB
    private static class CollectionBuilder
    {
        private final InternalKeyComparator internalKeyComparator;
        int version = 1;
        List<Map.Entry<InternalKey, Slice>> data = new ArrayList<>();

        public CollectionBuilder(UserComparator userComparator)
        {
            internalKeyComparator = new InternalKeyComparator(userComparator);
        }

        public CollectionBuilder add(String key, String value)
        {
            data.add(Maps.immutableEntry(new InternalKey(Slices.wrappedBuffer(asciiToBytes(key)), version++, ValueType.VALUE), Slices.wrappedBuffer(asciiToBytes(value))));
            return this;
        }

        public CollectionBuilder remove(String key)
        {
            data.add(Maps.immutableEntry(new InternalKey(Slices.wrappedBuffer(asciiToBytes(key)), version++, ValueType.DELETION), new Slice(new byte[0])));
            return this;
        }

        public List<Map.Entry<InternalKey, Slice>> build()
        {
            ArrayList<Map.Entry<InternalKey, Slice>> entries = new ArrayList<>(data);
            Collections.sort(entries, (o1, o2) -> internalKeyComparator.compare(o1.getKey(), o2.getKey()));
            return entries;
        }
    }
}
