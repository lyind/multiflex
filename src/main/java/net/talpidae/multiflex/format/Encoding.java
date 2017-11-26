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
 * All supported encodings for data fields.
 */
public enum Encoding
{
    /**
     * Uncompressed binary data, 0x0
     */
    BINARY,

    /**
     * UTF-8 encoded character string (zero terminated), 0x1
     */
    UTF8_STRING,

    /**
     * Array of 32-bit signed integer, compressed using VariableByte and FastPFOR, 0x2
     */
    INT32_VAR_BYTE_FAST_PFOR,

    /**
     * Array of 32-bit signed integers, compressed using Delta, VariableByte and FastPFOR, 0x3
     */
    INT32_DELTA_VAR_BYTE_FAST_PFOR
}
