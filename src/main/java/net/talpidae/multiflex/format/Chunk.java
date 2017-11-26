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

package net.talpidae.multiflex.format;

public interface Chunk extends AutoCloseable
{
    /**
     * Get a new Builder instance.
     */
    Builder builder();

    /**
     * Get the descriptors for all fields of this stream.
     */
    Descriptor getDescriptors();

    /**
     * Get the chunk's timestamp in microseconds since the containing store's epoch.
     */
    long getTimestamp();

    /**
     * Get the integers for the field identified by the specified stream ID.
     */
    int[] getIntegers(int streamId);

    /**
     * Get the text for the field identified by the specified stream ID (must be of a text type).
     */
    String getText(int streamId);

    /**
     * Get the binary data for the field identified by the specified stream ID (must be of a binary type).
     */
    byte[] getBinary(int streamId);


    interface Builder
    {
        Builder descriptor(Descriptor descriptor);

        Builder timestamp(long timestamp);

        Builder integers(int streamId, int[] integers);

        Builder text(int streamId, String text);

        Builder binary(int streamId, byte[] binary);

        Chunk build();
    }
}