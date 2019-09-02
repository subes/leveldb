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

import com.google.common.collect.Maps;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.Snapshot;
import org.iq80.leveldb.WriteBatch;
import org.iq80.leveldb.WriteOptions;
import org.iq80.leveldb.fileenv.EnvImpl;
import org.iq80.leveldb.fileenv.FileUtils;
import org.iq80.leveldb.util.Closeables;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Random;

import static org.testng.Assert.assertFalse;

public class GIssue320Test
{
    private Random rand;
    private DB db;
    private File databaseDir;

    @BeforeMethod
    public void setUp()
    {
        rand = new Random(0);
        databaseDir = FileUtils.createTempDir("leveldbIssues");
    }

    @AfterMethod
    public void tearDown()
    {
        if (db != null) {
            Closeables.closeQuietly(db);
        }
        boolean b = FileUtils.deleteRecursively(databaseDir);
        //assertion is specially useful in windows
        assertFalse(!b && databaseDir.exists(), "Dir should be possible to delete! All files should have been released. Existing files: " + FileUtils.listFiles(databaseDir));
    }

    private byte[] newString(int index)
    {
        int len = 1024;
        byte[] bytes = new byte[len];
        int i = 0;
        while (i < 8) {
            bytes[i] = (byte) ('a' + ((index >> (4 * i)) & 0xf));
            ++i;
        }
        while (i < bytes.length) {
            bytes[i] = (byte) ('a' + rand.nextInt(26));
            ++i;
        }
        return bytes;
    }

    @Test
    public void testIssue320() throws IOException
    {
        Map.Entry<byte[], byte[]>[] testMap = new Map.Entry[10000];
        Snapshot[] snapshots = new Snapshot[100];

        db = new DbImpl(new Options().createIfMissing(true), databaseDir.getAbsolutePath(), EnvImpl.createEnv());

        int targetSize = 10000;
        int numItems = 0;
        long count = 0;

        WriteOptions writeOptions = new WriteOptions();
        while (count++ < 200000) {
            int index = rand.nextInt(testMap.length);
            WriteBatch batch = new WriteBatchImpl();

            if (testMap[index] == null) {
                numItems++;
                testMap[index] =
                        Maps.immutableEntry(newString(index), newString(index));
                batch.put(testMap[index].getKey(), testMap[index].getValue());
            }
            else {
                byte[] oldValue = db.get(testMap[index].getKey());
                if (!Arrays.equals(oldValue, testMap[index].getValue())) {
                    Assert.fail("ERROR incorrect value returned by get"
                            + " \ncount=" + count
                            + " \nold value=" + new String(oldValue)
                            + " \ntestMap[index].getValue()=" + new String(testMap[index].getValue())
                            + " \ntestMap[index].getKey()=" + new String(testMap[index].getKey())
                            + " \nindex=" + index);
                }

                if (numItems >= targetSize && rand.nextInt(100) > 30) {
                    batch.delete(testMap[index].getKey());
                    testMap[index] = null;
                    --numItems;
                }
                else {
                    testMap[index] = Maps.immutableEntry(testMap[index].getKey(), newString(index));
                    batch.put(testMap[index].getKey(), testMap[index].getValue());
                }
            }

            db.write(batch, writeOptions);

            if (rand.nextInt(10) == 0) {
                final int i = rand.nextInt(snapshots.length);
                if (snapshots[i] != null) {
                    snapshots[i].close();
                }
                snapshots[i] = db.getSnapshot();
            }
        }
        for (Snapshot snapshot : snapshots) {
            if (snapshot != null) {
                snapshot.close();
            }
        }
        db.close();
    }
}
