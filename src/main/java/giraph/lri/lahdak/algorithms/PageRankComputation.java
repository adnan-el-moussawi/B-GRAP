/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package giraph.lri.lahdak.algorithms;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.utils.MathUtils;
import org.apache.hadoop.io.DoubleWritable;

import giraph.lri.lahdak.data.format.bgrap.BGRAP_LongWritable;

/**
 * The PageRank algorithm, with uniform transition probabilities on the edges
 * http://en.wikipedia.org/wiki/PageRank
 */
public class PageRankComputation extends RandomWalkComputation<DoubleWritable> {

  @Override
  protected double transitionProbability(
      Vertex<BGRAP_LongWritable, DoubleWritable, DoubleWritable> vertex,
      double stateProbability, Edge<BGRAP_LongWritable, DoubleWritable> edge) {
    // Uniform transition probability
    return stateProbability / vertex.getNumEdges();
  }

  @Override
  protected double recompute(
      Vertex<BGRAP_LongWritable, DoubleWritable, DoubleWritable> vertex,
      Iterable<DoubleWritable> partialRanks, double teleportationProbability) {
    // Rank contribution from incident neighbors
    double rankFromNeighbors = MathUtils.sum(partialRanks);
    // Rank contribution from dangling vertices
    double danglingContribution =
        getDanglingProbability() / getTotalNumVertices();

    // Recompute rank
    return (1d - teleportationProbability) *
        (rankFromNeighbors + danglingContribution) +
        teleportationProbability / getTotalNumVertices();
  }

}
