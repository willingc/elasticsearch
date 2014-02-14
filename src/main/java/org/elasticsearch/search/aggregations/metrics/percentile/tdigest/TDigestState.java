/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements. See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.elasticsearch.search.aggregations.metrics.percentile.tdigest;

import com.carrotsearch.hppc.cursors.IntCursor;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.aggregations.metrics.percentile.tdigest.GroupRedBlackTree.SizeAndSum;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

/**
 * Fork of https://github.com/tdunning/t-digest/blob/master/src/main/java/com/tdunning/math/stats/TDigest.java
 * Modified for less object allocation, faster estimations and integration with Elasticsearch serialization.
 */
public class TDigestState {

    private final Random gen;
    private double compression = 100;
    private GroupRedBlackTree summary;
    private long count = 0;
    private final SizeAndSum sizeAndSum = new SizeAndSum();

    /**
     * A histogram structure that will record a sketch of a distribution.
     *
     * @param compression How should accuracy be traded for size?  A value of N here will give quantile errors
     *                    almost always less than 3/N with considerably smaller errors expected for extreme
     *                    quantiles.  Conversely, you should expect to track about 5 N centroids for this
     *                    accuracy.
     */
    public TDigestState(double compression) {
        this.compression = compression;
        gen = new Random();
        summary = new GroupRedBlackTree((int) compression);
    }

    /**
     * Adds a sample to a histogram.
     *
     * @param x The value to add.
     */
    public void add(double x) {
        add(x, 1);
    }

    /**
     * Adds a sample to a histogram.
     *
     * @param x The value to add.
     * @param w The weight of this point.
     */
    public void add(double x, long w) {
        int startNode = summary.floorNode(x);
        if (startNode == RedBlackTree.NIL) {
            startNode = summary.ceilingNode(x);
        }

        if (startNode == RedBlackTree.NIL) {
            summary.addGroup(x, w);
            count = w;
        } else {
            double minDistance = Double.POSITIVE_INFINITY;
            int lastNeighbor = 0;
            summary.headSum(startNode, sizeAndSum);
            final int headSize = sizeAndSum.size;
            int i = headSize;
            for (int node = startNode; node != RedBlackTree.NIL; node = summary.nextNode(node)) {
                double z = Math.abs(summary.mean(node) - x);
                if (z <= minDistance) {
                    minDistance = z;
                    lastNeighbor = i;
                } else {
                    break;
                }
                i++;
            }

            int closest = RedBlackTree.NIL;
            long sum = sizeAndSum.sum;
            i = headSize;
            double n = 1;
            for (int node = startNode; node != RedBlackTree.NIL; node = summary.nextNode(node)) {
                if (i > lastNeighbor) {
                    break;
                }
                double z = Math.abs(summary.mean(node) - x);
                double q = (sum + summary.count(node) / 2.0) / count;
                double k = 4 * count * q * (1 - q) / compression;

                // this slightly clever selection method improves accuracy with lots of repeated points
                if (z == minDistance && summary.count(node) + w <= k) {
                    if (gen.nextDouble() < 1 / n) {
                        closest = node;
                    }
                    n++;
                }
                sum += summary.count(node);
                i++;
            }

            if (closest == RedBlackTree.NIL) {
                summary.addGroup(x, w);
            } else {
                double centroid = summary.mean(closest);
                long count = summary.count(closest);
                count += w;
                centroid += w * (x - centroid) / count;
                summary.updateGroup(closest, centroid, count);
            }
            count += w;

            if (summary.size() > 100 * compression) {
                // something such as sequential ordering of data points
                // has caused a pathological expansion of our summary.
                // To fight this, we simply replay the current centroids
                // in random order.

                // this causes us to forget the diagnostic recording of data points
                compress();
            }
        }
    }

    private int[] shuffleNodes(RedBlackTree tree) {
        int[] nodes = new int[tree.size()];
        int i = 0;
        for (IntCursor cursor : tree) {
            nodes[i++] = cursor.value;
        }
        assert i == tree.size();
        for (i = tree.size() - 1; i > 0; --i) {
            final int slot = gen.nextInt(i + 1);
            final int tmp = nodes[slot];
            nodes[slot] = nodes[i];
            nodes[i] = tmp;
        }
        return nodes;
    }

    public void add(TDigestState other) {
        final int[] shuffledNodes = shuffleNodes(other.summary);
        for (int node : shuffledNodes) {
            add(other.summary.mean(node), other.summary.count(node));
        }
    }

    public static TDigestState merge(double compression, Iterable<TDigestState> subData) {
        Preconditions.checkArgument(subData.iterator().hasNext(), "Can't merge 0 digests");
        List<TDigestState> elements = Lists.newArrayList(subData);
        int n = Math.max(1, elements.size() / 4);
        TDigestState r = new TDigestState(compression);
        for (int i = 0; i < elements.size(); i += n) {
            if (n > 1) {
                r.add(merge(compression, elements.subList(i, Math.min(i + n, elements.size()))));
            } else {
                r.add(elements.get(i));
            }
        }
        return r;
    }

    public void compress() {
        compress(summary);
    }

    private void compress(GroupRedBlackTree other) {
        TDigestState reduced = new TDigestState(compression);
        final int[] shuffledNodes = shuffleNodes(other);
        for (int node : shuffledNodes) {
            reduced.add(other.mean(node), other.count(node));
        }
        summary = reduced.summary;
    }

    /**
     * Returns the number of samples represented in this histogram.  If you want to know how many
     * centroids are being used, try centroids().size().
     *
     * @return the number of samples that have been added.
     */
    public long size() {
        return count;
    }

    public GroupRedBlackTree centroids() {
        return summary;
    }

    /**
     * @param x the value at which the CDF should be evaluated
     * @return the approximate fraction of all samples that were less than or equal to x.
     */
    public double cdf(double x) {
        GroupRedBlackTree values = summary;
        if (values.size() == 0) {
            return Double.NaN;
        } else if (values.size() == 1) {
            return x < values.mean(values.root()) ? 0 : 1;
        } else {
            double r = 0;

            // we scan a across the centroids
            Iterator<IntCursor> it = values.iterator();
            int a = it.next().value;

            // b is the look-ahead to the next centroid
            int b = it.next().value;

            // initially, we set left width equal to right width
            double left = (values.mean(b) - values.mean(a)) / 2;
            double right = left;

            // scan to next to last element
            while (it.hasNext()) {
                if (x < values.mean(a) + right) {
                    return (r + values.count(a) * interpolate(x, values.mean(a) - left, values.mean(a) + right)) / count;
                }
                r += values.count(a);

                a = b;
                b = it.next().value;

                left = right;
                right = (values.mean(b) - values.mean(a)) / 2;
            }

            // for the last element, assume right width is same as left
            left = right;
            a = b;
            if (x < values.mean(a) + right) {
                return (r + values.count(a) * interpolate(x, values.mean(a) - left, values.mean(a) + right)) / count;
            } else {
                return 1;
            }
        }
    }

    /**
     * @param q The quantile desired.  Can be in the range [0,1].
     * @return The minimum value x such that we think that the proportion of samples is <= x is q.
     */
    public double quantile(double q) {
        GroupRedBlackTree values = summary;
        if (values.size() == 0) {
            return Double.NaN;
        } else if (values.size() == 1) {
            return values.mean(values.root());
        }

        Iterator<IntCursor> it = values.iterator();
        int a = it.next().value;
        int b = it.next().value;
        if (!it.hasNext()) {
            // both a and b have to have just a single element
            double diff = (summary.mean(b) - summary.mean(a)) / 2;
            if (q > 0.75) {
                return summary.mean(b) + diff * (4 * q - 3);
            } else {
                return summary.mean(a) + diff * (4 * q - 1);
            }
        } else {
            q *= count;
            double right = (summary.mean(b) - summary.mean(a)) / 2;
            // we have nothing else to go on so make left hanging width same as right to start
            double left = right;

            if (q <= summary.count(a)) {
                return summary.mean(a) + left * (2 * q - summary.count(a)) / summary.count(a);
            } else {
                double t = summary.count(a);
                while (it.hasNext()) {
                    if (t + summary.count(b) / 2 >= q) {
                        // left of b
                        return summary.mean(b) - left * 2 * (q - t) / summary.count(b);
                    } else if (t + summary.count(b) >= q) {
                        // right of b but left of the left-most thing beyond
                        return summary.mean(b) + right * 2 * (q - t - summary.count(b) / 2.0) / summary.count(b);
                    }
                    t += summary.count(b);

                    a = b;
                    b = it.next().value;
                    left = right;
                    right = (summary.mean(b) - summary.mean(a)) / 2;
                }
                // shouldn't be possible but we have an answer anyway
                return summary.mean(b) + right;
            }
        }
    }

    public int centroidCount() {
        return summary.size();
    }

    public double compression() {
        return compression;
    }

    private double interpolate(double x, double x0, double x1) {
        return (x - x0) / (x1 - x0);
    }

    //===== elastic search serialization ======//

    public static void write(TDigestState state, StreamOutput out) throws IOException {
        out.writeDouble(state.compression);
        out.writeVInt(state.summary.size());
        for (IntCursor cursor : state.summary) {
            final int node = cursor.value;
            out.writeDouble(state.summary.mean(node));
            out.writeVLong(state.summary.count(node));
        }
    }

    public static TDigestState read(StreamInput in) throws IOException {
        double compression = in.readDouble();
        TDigestState state = new TDigestState(compression);
        int n = in.readVInt();
        for (int i = 0; i < n; i++) {
            state.add(in.readDouble(), in.readVInt());
        }
        return state;
    }
}
