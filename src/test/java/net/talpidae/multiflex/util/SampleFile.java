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

package net.talpidae.multiflex.util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;


public class SampleFile
{
    /**
     * Read a file containing white-space separated signed integers into an int[].
     */
    public static int[] readToIntArray(Path path) throws IOException
    {
        final int blockSize = 0x2000;  // 8192
        final ArrayList<int[]> blocks = new ArrayList<>((int) (path.toFile().length() / 3));

        int[] block = new int[blockSize];
        blocks.add(block);
        int total = 0;
        int iBlock = 0;
        int i = 0;
        int flags = 0; // flags: 0x0 - nothing, (-1 & (~1)) - is negative, 2 - is number
        try (final FileChannel channel = FileChannel.open(path, StandardOpenOption.READ))
        {
            //final byte[] remains =
            final ByteBuffer buffer = ByteBuffer.allocateDirect(16 * 1024);
            while (channel.read(buffer) >= 0)
            {
                buffer.flip();

                // convert until end of buffer
                final int remaining = buffer.remaining();
                for (int p = 0; p < remaining; ++p)
                {
                    // time to allocate a new block?
                    if (iBlock >= blockSize)
                    {
                        block = new int[blockSize];
                        blocks.add(block);
                        total += iBlock;
                        iBlock = 0;
                    }

                    int d = buffer.get(p) - 0x30;
                    if (d == -0x3)
                    {
                        if ((flags & 1) != 0)
                        {
                            throw new NumberFormatException("invalid sign at position " + total);
                        }

                        flags |= -2;
                    }
                    else if (d >= 0 && d <= 9)
                    {
                        i = (i * 10) + d;
                        flags |= 1;
                    }
                    else
                    {
                        if ((flags & 1) != 0)
                        {
                            // multiply with 1 or -1 (if minus sign was encountered)
                            block[iBlock] = i * flags;
                            ++iBlock;
                        }

                        i = 0;
                        flags = 0;
                    }
                }

                buffer.clear();
            }
        }

        // rest of last number (EOF may come without trailing white-space)
        if ((flags & 1) != 0)
        {
            block[iBlock] = i * (flags | 1);
            ++iBlock;
        }

        // rest from last block
        total += iBlock;

        // merge blocks
        final int[] merged = new int[total];
        final int blockCount = blocks.size();

        int iMerged = 0;
        for (final int[] b : blocks)
        {
            final int count = Math.min(blockSize, total);
            for (int j = 0; j < count; ++j, ++iMerged)
            {
                merged[iMerged] = b[j];
            }

            total -= count;
        }

        return merged;
    }
}
