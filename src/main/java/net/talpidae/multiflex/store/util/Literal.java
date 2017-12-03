/*
 * Copyright (C) 2017  Jonas Zeiger <jonas.zeiger@talpidae.net>
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
 *
 */


package net.talpidae.multiflex.store.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;


public final class Literal
{
    private Literal()
    {

    }

    /**
     * Load resources as String literals. Consider this a workaround for non-existing Java multi-line strings.
     *
     * @param path The path of the String literal resource to use ("/blabla/resource.txt" for "/src/main/resources/blabla/resource.txt").
     * @return The String content of the specified resource or null if the resources wasn't found
     */
    public static String from(String path) throws IOException
    {
        final InputStream inputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(path);
        if (inputStream != null)
        {
            return readAll(inputStream);
        }

        return null;
    }

    private static String readAll(InputStream inputStream) throws IOException
    {
        final StringBuilder builder = new StringBuilder();
        final char[] buffer = new char[512];

        try (final Reader reader = new InputStreamReader(inputStream, StandardCharsets.UTF_8))
        {
            int readCount;
            while ((readCount = reader.read(buffer)) > 0)
            {
                builder.append(buffer, 0, readCount);
            }
        }

        return builder.toString();
    }
}
