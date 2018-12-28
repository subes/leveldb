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

import org.iq80.leveldb.Logger;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.time.LocalDateTime;
import java.util.function.Supplier;

public class FileLogger implements Logger
{
    private static final int DATE_SIZE = 28;
    private final PrintStream ps;
    private final Supplier<LocalDateTime> clock;

    private FileLogger(PrintStream ps, Supplier<LocalDateTime> clock)
    {
        this.ps = ps;
        this.clock = clock;
    }

    public static FileLogger createFileLogger(OutputStream outputStream, Supplier<LocalDateTime> clock)
    {
        return new FileLogger(new PrintStream(outputStream), clock);
    }

    public static FileLogger createFileLogger(File loggerFile) throws IOException
    {
        return createFileLogger(new FileOutputStream(loggerFile), LocalDateTime::now);
    }

    @Override
    public void log(String template, Object... args)
    {
        template = String.valueOf(template); // null -> "null"

        // start substituting the arguments into the '%s' placeholders
        StringBuilder builder = new StringBuilder(DATE_SIZE + template.length() + 16 * args.length);
        builder.append(clock.get());
        builder.append(" ");
        int templateStart = 0;
        int i = 0;
        while (i < args.length) {
            int placeholderStart = template.indexOf("%s", templateStart);
            if (placeholderStart == -1) {
                break;
            }
            builder.append(template, templateStart, placeholderStart);
            builder.append(args[i++]);
            templateStart = placeholderStart + 2;
        }
        builder.append(template, templateStart, template.length());

        // if we run out of placeholders, append the extra args in square braces
        if (i < args.length) {
            builder.append(" [");
            builder.append(args[i++]);
            while (i < args.length) {
                builder.append(", ");
                builder.append(args[i++]);
            }
            builder.append(']');
        }
        log2(builder.toString());
    }

    @Override
    public void log(String message)
    {
        final StringBuilder sb = new StringBuilder(message.length() + DATE_SIZE);
        sb.append(clock.get());
        sb.append(' ');
        sb.append(message);
        log2(sb.toString());
    }

    private void log2(String message)
    {
        synchronized (ps) {
            ps.println(message);
        }
    }

    @Override
    public void close() throws IOException
    {
        ps.close();
    }
}
