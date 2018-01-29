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

public class Wave
{
    /**
     * Create a sine wave form.
     *
     * @param min             Minimum value
     * @param max             Maximum value
     * @param durationMillies Duration in milliseconds
     * @param sampleRate      Sample rate in samples per second
     * @param frequency       Frequency in Hz
     * @return Array with the requested wave-form
     */
    public static int[] sine(int min, int max, int durationMillies, int sampleRate, double frequency)
    {
        if (sampleRate <= 0)
            throw new IllegalArgumentException("sampleRate must be larger than 0");

        if (min > max)
            throw new IllegalArgumentException("min must be smaller or equal to max");

        final int samples = (durationMillies * sampleRate) / 1000;
        final int[] out = new int[samples];
        final int width = max - min + 1;
        final int center = width / 2;

        final double period = Math.max(1, sampleRate) / frequency;
        for (int i = 0; i < samples; ++i)
        {
            final double angle = 2.0 * Math.PI * (i / period);
            out[i] = (int) Math.min(max, Math.max(min, ((Math.sin(angle) + 1.0) * center) + min));
        }

        return out;
    }
}
