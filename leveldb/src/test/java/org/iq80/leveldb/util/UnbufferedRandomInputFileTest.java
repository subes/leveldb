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

import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;

import static org.testng.Assert.assertEquals;

public class UnbufferedRandomInputFileTest
{
    @Test
    public void testResilientToThreadInterruptOnReaderThread() throws IOException
    {
        File file = File.createTempFile("table", ".db");
        try (final FileOutputStream fileOutputStream = new FileOutputStream(file)) {
            fileOutputStream.write(new byte[1024]);
        }
        try (final RandomInputFile open = UnbufferedRandomInputFile.open(file)) {
            //mark current thread as interrupted
            Thread.currentThread().interrupt();
            try {
                open.read(200, 200);
                Assert.fail("Should have failed with ClosedByInterruptException");
            }
            catch (ClosedByInterruptException e) {
                //reader that was interrupted is expected fail at this point
                //no other threads
            }
            //clear current thread interrupt
            Thread.interrupted();

            //verify file is still accessible after previous failure
            final ByteBuffer read = open.read(200, 200);
            assertEquals(read.remaining(), 200);
        }
        finally {
            file.delete();
        }
    }

    //TODO add multi thread, client thread, interruption resilience test
}
