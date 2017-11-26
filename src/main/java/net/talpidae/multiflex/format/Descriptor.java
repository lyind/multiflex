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

/**
 * Describes the format of the data stored in one or multiple chunks.
 */
public interface Descriptor
{
    /**
     * Return a new Builder instance.
     */
    Builder builder();


    /**
     * Get the application defined stream ID.
     */
    int[] getStreamIds();


    /**
     * Get the encoding for the part of the stream with the specified ID that is associated with this descriptor.
     */
    Encoding getEncoding(int streamId);


    interface Builder
    {
        Builder addStream(int streamId, Encoding encoding);

        Descriptor build();
    }
}
