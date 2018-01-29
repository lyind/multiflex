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

import com.github.plot.Plot;
import net.talpidae.multiflex.util.Q32dot8;
import org.junit.Test;

import java.awt.*;
import java.io.IOException;

import static net.talpidae.multiflex.util.Q32dot8.*;
import static org.junit.Assert.assertEquals;


public class Q32dot8Test
{
    private static double[] createXSeriesCount(int start, int n)
    {
        final double[] x = new double[n];

        for (int i = start; i < n; ++i)
        {
            x[i] = i;
        }

        return x;
    }

    private static double[] toDoubleArray(int[] ints)
    {
        final int n = ints.length;
        final double[] doubles = new double[n];

        for (int i = 0; i < n; ++i)
        {
            doubles[i] = ints[i];
        }

        return doubles;
    }

    private static int toIntSaturated(double v)
    {
        return (int) Math.min(Integer.MAX_VALUE, Math.max(Integer.MIN_VALUE, (long) v));
    }

    @Test
    public void testMul() throws IOException
    {
        assertEquals(fromInt(-1806), mul(fromInt(-42), fromInt(43)));

        assertEquals(fromDouble(6), mul(fromDouble(0.5), fromDouble(12)));

        assertEquals(fromDouble(Integer.MAX_VALUE * 0.5), mul(fromInt(Integer.MAX_VALUE), fromDouble(0.5)));

        assertEquals(fromDouble(Integer.MIN_VALUE * 0.5), mul(fromInt(Integer.MIN_VALUE), fromDouble(0.5)));
    }


    @Test
    public void testLerp8() throws IOException
    {
        final short fraction8bit0point5 = (short)((500 << 8) / 1000);

        assertEquals(fromInt(500), lerp8(fromInt(0), fromInt(1000), fraction8bit0point5));

        final short fraction8bit5 = (short)((5000 << 8) / 1000);
        assertEquals(fromInt(5000), lerp8(fromInt(0), fromInt(1000), fraction8bit5));

        final int[] curve = new int[5000];
        for (int i = 0; i < curve.length; ++i)
        {
            curve[i] = toIntSaturated(lerp8(fromInt(0), fromInt(1000), (short)((i << 8) / 1000)));
        }

        Plot.plot(Plot.plotOpts().legend(Plot.LegendFormat.BOTTOM))
                .series("v", Plot.data().xy(createXSeriesCount(0, curve.length), toDoubleArray(curve)), Plot.seriesOpts().color(Color.BLUE))
                // now tester, please look at "testDerivations-1.png"
                .save("testLerp8-1", "png");
    }
}
