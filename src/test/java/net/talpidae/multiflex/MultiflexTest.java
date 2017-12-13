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

package net.talpidae.multiflex;

import net.talpidae.multiflex.format.Chunk;
import net.talpidae.multiflex.format.Descriptor;
import net.talpidae.multiflex.format.Encoding;
import net.talpidae.multiflex.store.Store;
import net.talpidae.multiflex.store.StoreException;
import net.talpidae.multiflex.util.Wave;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;


public class MultiflexTest
{
    @Test
    public void testOpenWritable() throws IOException, StoreException
    {
        final File file = File.createTempFile(MultiflexTest.class.getSimpleName(), ".mfx");

        try (Store store = Multiflex.openSqlite(file, true))
        {
            assertNotNull("store is null", store);
        }
    }

    @Test
    public void testPutChunkGetChunk() throws Exception
    {
        final File file = File.createTempFile(MultiflexTest.class.getSimpleName(), ".mfx");

        try (Store store = Multiflex.openSqlite(file, true))
        {
            assertNotNull("store is null", store);

            final Descriptor descriptor = store.descriptorBuilder()
                    .track(42, Encoding.INT32_DELTA_VAR_BYTE_FAST_PFOR)
                    .track(86, Encoding.UTF8_STRING)
                    .track(69, Encoding.BINARY)
                    .build();

            final Chunk.Builder builder = store.chunkBuilder(descriptor);

            // write signal, keeping waveform for later comparison
            final List<int[]> waves = new ArrayList<>();
            final Random random = new Random(86);
            final long endMillies = TimeUnit.MINUTES.toMillis(60);
            final long epochMicros = TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis());

            long totalIntegersSize = 0;
            store.setEpoch(epochMicros);
            for (long t = 0; t < endMillies; t += 1000)
            {
                builder.timestamp(t);
                builder.text(86, "blub");
                builder.binary(69, ByteBuffer.wrap(new byte[]{0x17, 0x33, 0x44}));

                final int[] wave = Wave.sine(Short.MIN_VALUE, Short.MAX_VALUE, 1000, (int) (1023 * random.nextDouble()) + 1, (149 * random.nextDouble()) + 1);
                waves.add(wave);
                totalIntegersSize += wave.length * 4;
                builder.integers(42, wave);

                store.put(builder.build());
            }

            assertNotNull("id is invalid UUID", store.getId());
            assertEquals("version is not set", store.getVersion(), 1);
            assertEquals("epoch not stored correctly", store.getEpoch(), epochMicros);

            // read chunk by chunk, comparing values
            int i = 0;
            final byte[] expectedBytes = new byte[]{0x17, 0x33, 0x44};
            for (long t = 0; t < endMillies; t += 1000)
            {
                try (final Chunk chunk = store.findByTimestamp(t))
                {
                    assertArrayEquals("wrong wave is returned", chunk.getIntegers(42), waves.get(i));
                    assertEquals("wrong text is returned", chunk.getText(86), "blub");

                    final ByteBuffer buffer = chunk.getBinary(69);
                    final byte[] binaryValue = new byte[buffer.remaining()];
                    buffer.get(binaryValue);
                    assertArrayEquals("wrong binary data is returned", binaryValue, new byte[]{0x17, 0x33, 0x44});
                }

                ++i;
            }
        }
    }
}
