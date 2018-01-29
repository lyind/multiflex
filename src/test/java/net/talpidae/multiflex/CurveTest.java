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
import me.lemire.integercompression.Composition;
import me.lemire.integercompression.FastPFOR;
import me.lemire.integercompression.IntWrapper;
import me.lemire.integercompression.VariableByte;
import me.lemire.integercompression.differential.Delta;
import net.talpidae.multiflex.store.util.Center31BitEncoding;
import net.talpidae.multiflex.store.util.Curve;
import net.talpidae.multiflex.util.SampleFile;
import net.talpidae.multiflex.util.Wave;
import org.junit.Test;

import java.awt.*;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;

import static net.talpidae.multiflex.util.Q32dot8.*;
import static org.junit.Assert.assertArrayEquals;


public class CurveTest
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

    private static Plot.Data interleavedData(int[] points)
    {
        return new InterleavedData(points);
    }

    @Test
    public void testDerivations() throws IOException
    {
        final int[] curve = Wave.sine(Short.MIN_VALUE, Short.MAX_VALUE, 1000, 256, 10);
        final int[] firstDerivation = Curve.firstDerivation(curve);
        final int[] secondDerivation = Curve.secondDerivation(curve);

        Plot.plot(Plot.plotOpts().legend(Plot.LegendFormat.BOTTOM))
                .series("v", Plot.data().xy(createXSeriesCount(0, curve.length), toDoubleArray(curve)), Plot.seriesOpts().color(Color.BLUE))
                .series("f'", Plot.data().xy(createXSeriesCount(1, firstDerivation.length), toDoubleArray(firstDerivation)), Plot.seriesOpts().color(Color.MAGENTA))
                .series("f''", Plot.data().xy(createXSeriesCount(2, secondDerivation.length), toDoubleArray(secondDerivation)), Plot.seriesOpts().color(Color.RED))
                // now tester, please look at "testDerivations-1.png"
                .save("testDerivations-1", "png");
    }

    @Test
    public void testReverseDerivations() throws IOException
    {
        final int[] curve = Wave.sine(Short.MIN_VALUE, Short.MAX_VALUE, 1000, 256, 10);
        final int[] firstDerivation = Curve.firstDerivation(curve);
        final int[] secondDerivation = Curve.secondDerivation(curve);

        assertArrayEquals("failed to reverse first derivation", curve, Curve.reverseFirstDerivation(curve[0], firstDerivation, 0, firstDerivation.length));
        assertArrayEquals("failed to reverse second derivation", curve, Curve.reverseSecondDerivation(curve[0], firstDerivation[0], secondDerivation, 0, secondDerivation.length));
    }

    @Test
    public void testEnvelopes() throws IOException
    {
        final int[] curve = Wave.sine(Short.MIN_VALUE, Short.MAX_VALUE, 1000, 256, 5);
        final int[] upperEnvelope = Curve.upperEnvelope(curve);
        final int[] lowerEnvelope = Curve.lowerEnvelope(curve);
        final int[] center = Curve.center(curve);

        Plot.plot(Plot.plotOpts().legend(Plot.LegendFormat.BOTTOM))
                .series("v", Plot.data().xy(createXSeriesCount(0, curve.length), toDoubleArray(curve)), Plot.seriesOpts().color(Color.BLUE))
                .series("E1", interleavedData(upperEnvelope), Plot.seriesOpts().color(Color.RED))
                .series("E2", interleavedData(lowerEnvelope), Plot.seriesOpts().color(Color.GREEN))
                .series("center", interleavedData(center), Plot.seriesOpts().color(Color.MAGENTA))
                // now tester, please look at "testEnvelopes-1.png"
                .save("testEnvelopes-1", "png");
    }

    @Test
    public void testSimplifiedCenter() throws IOException
    {
        final double noiseLevel = 0.2;
        //final int[] curve = Wave.sine(Short.MIN_VALUE, Short.MAX_VALUE, 1000, 256, 2);
        final int[] curve = Wave.sine(Short.MIN_VALUE, Short.MAX_VALUE, 1000, 256, 5);
        final int[] curveNoise = Wave.sine(toIntSaturated(Short.MIN_VALUE * noiseLevel), toIntSaturated(Short.MAX_VALUE * noiseLevel), 1000, 256, 50);
        final int[] mixCurve = Curve.addSaturated(curve, curveNoise);

        final int[] upperEnvelope = Curve.upperEnvelope(mixCurve);
        final int[] lowerEnvelope = Curve.lowerEnvelope(mixCurve);
        final int[] simplifiedCenter = Curve.simplifiedCenter(mixCurve);

        // calculate upper/lower envelope and merge slopes where the next low peak value is higher or equal to the current high peak or the other way around
        Plot.plot(Plot.plotOpts().legend(Plot.LegendFormat.BOTTOM))
                .series("v", Plot.data().xy(createXSeriesCount(0, mixCurve.length), toDoubleArray(mixCurve)), Plot.seriesOpts().color(Color.BLUE))
                //.series("E1", Plot.data().xy(toDoubleArray(upperEnvelope[0]), toDoubleArray(upperEnvelope[1])), Plot.seriesOpts().color(Color.BLACK))
                // .series("E2", Plot.data().xy(toDoubleArray(lowerEnvelope[0]), toDoubleArray(lowerEnvelope[1])), Plot.seriesOpts().color(Color.LIGHT_GRAY))
                .series("simple center", interleavedData(simplifiedCenter), Plot.seriesOpts().color(Color.MAGENTA).marker(Plot.Marker.CIRCLE))
                // now tester, please look at "testEnvelopes-1.png"
                .save("testSimplifiedCenter-1", "png");
    }

    @Test
    public void testEncode() throws IOException
    {
        /*
        final double noiseLevel = 0.2;
        final int[] curve = Wave.sine(Short.MIN_VALUE, Short.MAX_VALUE, 1000, 256, 10);
        final int[] curveNoise = Wave.sine(toIntSaturated(Short.MIN_VALUE * noiseLevel), toIntSaturated(Short.MAX_VALUE * noiseLevel), 1000, 256, 50);
        final int[] mixCurve = Curve.addSaturated(curve, curveNoise);
                */
        final int[] mixCurve = SampleFile.readToIntArray(Paths.get("test_series.asc"));

        //final int[] encoded = EnvelopeEncoding.encode(mixCurve);
        final int[] encoded = Center31BitEncoding.encode(mixCurve);
        //final int[] encoded = Arrays.copyOf(mixCurve, mixCurve.length);

        //final int[] encoded2 = Center30BitEncoding.encode(encoded);
        final int[] encoded2 = Arrays.copyOf(mixCurve, mixCurve.length);

        //final int[] decoded = Center30BitEncoding.decode(encoded2);
        final int[] decoded = Arrays.copyOf(mixCurve, mixCurve.length);

        //final int[] decoded2 = CenterLineEncoding.decode(decoded);
        final int[] decoded2 = Center31BitEncoding.decode(encoded);
        //final int[] decoded2 = Arrays.copyOf(mixCurve, mixCurve.length);

        //final int[] encoded2 = Curve.simplifiedCenter(encoded);
        //final int[] encoded2 = CenterLineEncoding.encode(encoded);

        final int[] simplifiedCenter = Curve.simplifiedCenter(mixCurve);

        final int[] diff = new int[mixCurve.length];
        int x0 = 0;
        int y0 = 0;
        int x1 = simplifiedCenter[0];
        int y1 = simplifiedCenter[1];
        for (int i = 0, j = 2; i < diff.length; ++i)
        {
            // next point
            if (i == x1 && j < simplifiedCenter.length)
            {
                x0 = x1;
                y0 = y1;
                x1 = simplifiedCenter[j];
                y1 = simplifiedCenter[j + 1];
                j += 2;
            }

            // lerp8 x0,y0 -> x1,y1
            final short fraction8bitX0toX1 = (short) (((i - x0) << 8) / (x1 - x0 + 1));
            final int yt = toInt(lerp8(fromInt(y0), fromInt(y1), fraction8bitX0toX1));

            // store delta to lerp'd line
            diff[i] = yt - mixCurve[i];
        }

        final int[] diffShift = new int[mixCurve.length];
        x0 = 0;
        y0 = 0;
        x1 = simplifiedCenter[0];
        y1 = simplifiedCenter[1];
        for (int i = 0, j = 2; i < diffShift.length; ++i)
        {
            // next point
            if (i == x1 && j < simplifiedCenter.length)
            {
                x0 = x1;
                y0 = y1;
                x1 = simplifiedCenter[j];
                y1 = simplifiedCenter[j + 1];
                j += 2;
            }

            // lerp8 x0,y0 -> x1,y1
            final short fraction8bitX0toX1 = (short) (((i - x0) << 8) / (x1 - x0 + 1));
            final int yt = toInt(lerp8(fromInt(y0), fromInt(y1), fraction8bitX0toX1));

            // store positive encoded delta to lerp'd line
            final int delta = yt - mixCurve[i];
            if (delta > 0)
            {
                diffShift[i] = delta << 1;
            }
            else
            {
                diffShift[i] = ((delta * -1) << 1) | 1;
            }
        }

        final int[] lowerEnvelope = Curve.lowerEnvelope(mixCurve);
        final int[] fullLowerEnvelope = new int[lowerEnvelope.length + 4];
        System.arraycopy(lowerEnvelope, 0, fullLowerEnvelope, 2, lowerEnvelope.length);
        fullLowerEnvelope[0] = 0;
        fullLowerEnvelope[1] = mixCurve[0];
        fullLowerEnvelope[fullLowerEnvelope.length - 2] = mixCurve.length - 1;
        fullLowerEnvelope[fullLowerEnvelope.length - 1] = mixCurve[mixCurve.length - 1];

        final int[] diffLow = new int[mixCurve.length];
        x0 = 0;
        y0 = 0;
        x1 = fullLowerEnvelope[0];
        y1 = fullLowerEnvelope[1];
        for (int i = 0, j = 2; i < diffLow.length; ++i)
        {
            // next point
            if (i == x1 && j < fullLowerEnvelope.length)
            {
                x0 = x1;
                y0 = y1;
                x1 = fullLowerEnvelope[j];
                y1 = fullLowerEnvelope[j + 1];
                j += 2;
            }

            // lerp8 x0,y0 -> x1,y1
            final short fraction8bitX0toX1 = (short) (((i - x0) << 8) / (x1 - x0 + 1));
            final int yt = toInt(lerp8(fromInt(y0), fromInt(y1), fraction8bitX0toX1));

            // store delta to lerp'd line
            diffLow[i] = yt - mixCurve[i];
        }

        final Composition packer = new Composition(new FastPFOR(), new VariableByte());

        final IntWrapper outPos = new IntWrapper();
        int[] mixCurveDelta = Arrays.copyOf(mixCurve, mixCurve.length);
        Delta.delta(mixCurveDelta);
        int[] compressedDelta = new int[mixCurveDelta.length];
        packer.compress(mixCurveDelta, new IntWrapper(), mixCurveDelta.length, compressedDelta, outPos);
        compressedDelta = Arrays.copyOf(compressedDelta, outPos.get());

        int[] compressedDiff = new int[diff.length];
        outPos.set(0);
        packer.compress(diff, new IntWrapper(), diff.length, compressedDiff, outPos);
        compressedDiff = Arrays.copyOf(compressedDiff, outPos.get());

        int[] compressedDiffShift = new int[diffShift.length];
        outPos.set(0);
        packer.compress(diffShift, new IntWrapper(), diffShift.length, compressedDiffShift, outPos);
        compressedDiffShift = Arrays.copyOf(compressedDiffShift, outPos.get());

        int[] compressedDiffLow = new int[diffLow.length * 2];
        outPos.set(0);
        packer.compress(diffLow, new IntWrapper(), diffLow.length, compressedDiffLow, outPos);
        compressedDiffLow = Arrays.copyOf(compressedDiffLow, outPos.get());

        int[] compressed = new int[mixCurve.length];
        outPos.set(0);
        packer.compress(mixCurve, new IntWrapper(), mixCurve.length, compressed, outPos);
        compressed = Arrays.copyOf(compressed, outPos.get());

        int[] compressed2 = new int[encoded.length];
        outPos.set(0);
        packer.compress(encoded, new IntWrapper(), encoded.length, compressed2, outPos);
        compressed2 = Arrays.copyOf(compressed2, outPos.get());

        int[] compressed3 = new int[encoded2.length];
        outPos.set(0);
        packer.compress(encoded2, new IntWrapper(), encoded2.length, compressed3, outPos);
        compressed3 = Arrays.copyOf(compressed3, outPos.get());

        // calculate upper/lower envelope and merge slopes where the next low peak value is higher or equal to the current high peak or the other way around
        Plot.plot(Plot.plotOpts().legend(Plot.LegendFormat.BOTTOM))
                .series("v", Plot.data().xy(createXSeriesCount(0, mixCurve.length), toDoubleArray(mixCurve)), Plot.seriesOpts().color(Color.BLUE))
                //.series("diff", Plot.data().xy(createXSeriesCount(0, diff.length), toDoubleArray(diff)), Plot.seriesOpts().color(Color.RED).lineWidth(1))
                //.series("diffLow", Plot.data().xy(createXSeriesCount(0, diffLow.length), toDoubleArray(diffLow)), Plot.seriesOpts().color(Color.RED).lineWidth(1))
                //.series("diff-shift", Plot.data().xy(createXSeriesCount(0, diffShift.length), toDoubleArray(diffShift)), Plot.seriesOpts().color(Color.RED).lineWidth(1))
                //.series("encoded", Plot.data().xy(createXSeriesCount(0, encoded.length), toDoubleArray(encoded)), Plot.seriesOpts().color(Color.GREEN).lineWidth(1))
                //.series("encoded2", Plot.data().xy(createXSeriesCount(0, encoded2.length), toDoubleArray(encoded2)), Plot.seriesOpts().color(Color.MAGENTA).lineWidth(1))
                //.series("decoded", Plot.data().xy(createXSeriesCount(0, decoded.length), toDoubleArray(decoded)), Plot.seriesOpts().color(Color.GREEN))
                .series("decoded2", Plot.data().xy(createXSeriesCount(0, decoded2.length), toDoubleArray(decoded2)), Plot.seriesOpts().color(Color.ORANGE).lineWidth(1).line(Plot.Line.DASHED))
                //.series("E1", interleavedData(upperEnvelope), Plot.seriesOpts().color(Color.RED).lineWidth(1))
                //.series("E2", interleavedData(lowerEnvelope), Plot.seriesOpts().color(Color.GREEN).lineWidth(1))
                //.series("trace", interleavedData(trace), Plot.seriesOpts().color(Color.MAGENTA).lineWidth(1))
                //.series("simple center", interleavedData(simplifiedCenter), Plot.seriesOpts().color(Color.MAGENTA).lineWidth(1))
                // now tester, please look at "testEnvelopes-1.png"
                .save("encoded-1", "png");
    }


    private static class InterleavedData extends Plot.Data
    {
        private final int[] points;

        private InterleavedData(int[] points)
        {
            super();

            if ((points.length & 0x1) != 0)
            {
                throw new IllegalArgumentException("input array length not even");
            }

            this.points = points;
        }


        @Override
        public int size()
        {
            return points.length / 2;
        }

        @Override
        public double x(int i)
        {
            return points[i * 2];
        }

        @Override
        public double y(int i)
        {
            return points[(i * 2) + 1];
        }
    }
}
