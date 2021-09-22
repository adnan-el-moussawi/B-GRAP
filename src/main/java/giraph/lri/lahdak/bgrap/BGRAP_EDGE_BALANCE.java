/**
 * Copyright 2018 SpinnerPlusPlus.ml
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
 */
package giraph.lri.lahdak.bgrap;

import it.unimi.dsi.fastutil.shorts.ShortArrayList;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Random;
import org.apache.giraph.aggregators.DoubleSumAggregator;
import org.apache.giraph.aggregators.IntSumAggregator;
import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.AbstractComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import com.google.common.collect.Lists;

import giraph.lri.lahdak.bgrap.BGRAP_Samplers.CC_SamplingBasedOnClusteringCoefficient;
import giraph.lri.lahdak.bgrap.BGRAP_Samplers.GD_SamplingBasedOnGraphDistribution;
import giraph.lri.lahdak.bgrap.BGRAP_Samplers.HD_SamplingBasedOnHighestDegree;
import giraph.lri.lahdak.bgrap.BGRAP_Samplers.RD_SamplingBasedOnRankDegree;
import giraph.lri.lahdak.bgrap.utils.BGRAP_EdgeValue;
import giraph.lri.lahdak.bgrap.utils.BGRAP_SamplingMessage;
import giraph.lri.lahdak.bgrap.utils.BGRAP_VertexValue;

/**
 * Extends the Spinner edge balanced partitioning of a graph with an
 * optimized initialization + directed edge distinction.
 * 
 * @author adnan
 * 
 */
@SuppressWarnings("unused")
public class BGRAP_EDGE_BALANCE extends BGRAP_Partitionning {
	
	/**
	 * check the labels in the neighbor of the vertex 
	 * then compute the frequency of each label while 
	 * considering the edge balance constraint
	 * @author Adnan
	 *
	 */
	public static class ComputeNewPartition extends
			// AbstractComputation<LongWritable, VertexValue, EdgeValue, SamplingMessage,
			// NullWritable> {
			AbstractComputation<IntWritable, BGRAP_VertexValue, BGRAP_EdgeValue, BGRAP_SamplingMessage, IntWritable> {
		private ShortArrayList maxIndices = new ShortArrayList();
		private Random rnd = new Random();
		//
		protected String[] demandAggregatorNames;
		protected int[] partitionFrequency;
		/**
		 * connected partitions
		 */
		private ShortArrayList pConnect;
		/**
		 * Adnan : Edge-count in each partition (
		 */
		protected long[] loads;
		/**
		 * Adnan : Vertex-count in each partition (
		 */
		private long[]  vertexCount;
		/**
		 * Adnan: the balanced capacity of a partition |E|/K, |V|/K
		 */
		private long totalEdgeCapacity;
		private long totalVertexCapacity;
		private short numberOfPartitions;
		private short repartition;
		private double additionalCapacity;
		private double lambda;
		private long totalNumEdges;
		protected boolean directedGraph;
		private float kappa;

		/**
		 * Adnan : compute the edge balance penalty function : Load/capacity
		 * 
		 * @param newPartition
		 * @return
		 */
		protected double computeEdgeBalance(int newPartition) {
			return new BigDecimal(((double) loads[newPartition]) / totalEdgeCapacity)
					.setScale(3, BigDecimal.ROUND_CEILING).doubleValue();
		}

		/*
		 * Request migration to a new partition Adnan : update in order to recompute the
		 * number of vertex
		 */
		private void requestMigration(Vertex<IntWritable, BGRAP_VertexValue, BGRAP_EdgeValue> vertex, int numberOfEdges,
				short currentPartition, short newPartition) {
			vertex.getValue().setNewPartition(newPartition);

			aggregate(demandAggregatorNames[newPartition], new LongWritable(numberOfEdges));
			loads[newPartition] += numberOfEdges;
			loads[currentPartition] -= numberOfEdges;

			// aggregate(demandAggregatorNames[newPartition], new LongWritable(1));
			//  vertexCountnewPartition] += 1;
			//  vertexCountcurrentPartition] -= 1;
		}

		/*
		 * Update the neighbor labels when they migrate
		 */
		protected void updateNeighborsPartitions(Vertex<IntWritable, BGRAP_VertexValue, BGRAP_EdgeValue> vertex,
				Iterable<BGRAP_SamplingMessage> messages) {
			for (BGRAP_SamplingMessage message : messages) {
				IntWritable otherId = new IntWritable(message.getSourceId());
				BGRAP_EdgeValue oldValue = vertex.getEdgeValue(otherId);
				
				vertex.setEdgeValue(otherId, new BGRAP_EdgeValue((short)message.getPartition(), 
						oldValue.getWeight(), oldValue.isVirtualEdge()));
				
				/*
				 * System.out.println(vertex.getId().get()+"-->"+otherId.get() + "\t" +
				 * oldValue.isVirtualEdge() + "\t" + oldValue.getWeight());
				 */
				 
			}
		}

		/*
		 * Compute the occurrences of the labels in the neighborhood Adnan : could we
		 * use an heuristic that also consider in how many edge/vertex are present the
		 * label?
		 */
		protected int computeNeighborsLabels(Vertex<IntWritable, BGRAP_VertexValue, BGRAP_EdgeValue> vertex) {
			Arrays.fill(partitionFrequency, 0);
			int totalLabels = 0, localEdges = 0;

			long externalEdges = 0;
			short partition;
			pConnect = new ShortArrayList();
			for (Edge<IntWritable, BGRAP_EdgeValue> e : vertex.getEdges()) {
				partition = e.getValue().getPartition();
				partitionFrequency[e.getValue().getPartition()] += e.getValue().getWeight();
				totalLabels += e.getValue().getWeight();

				if (directedGraph && e.getValue().isVirtualEdge()) {
					continue;
				}

				if (e.getValue().getPartition() == vertex.getValue().getCurrentPartition()) {
					localEdges++;
				} else {
					if (!pConnect.contains(partition))
						pConnect.add(partition);
				}
			}

			externalEdges = vertex.getValue().getRealOutDegree() - localEdges;
			// update cut edges stats
			aggregate(AGGREGATOR_LOCALS, new LongWritable(localEdges));
			// ADNAN : update Total EC. State
			aggregate(AGG_EDGE_CUTS, new LongWritable(externalEdges));

			return totalLabels;
		}

		/*
		 * Choose a random partition with preference to the current
		 */
		protected short chooseRandomPartitionOrCurrent(short currentPartition) {
			short newPartition;
			if (maxIndices.size() == 1) {
				newPartition = maxIndices.get(0);
			} else {
				// break ties randomly unless current
				if (maxIndices.contains(currentPartition)) {
					newPartition = currentPartition;
				} else {
					newPartition = maxIndices.get(rnd.nextInt(maxIndices.size()));
				}
			}
			return newPartition;
		}

		/*
		 * Choose deterministically on the label with preference to the current
		 */
		protected short chooseMinLabelPartition(short currentPartition) {
			short newPartition;
			if (maxIndices.size() == 1) {
				newPartition = maxIndices.get(0);
			} else {
				if (maxIndices.contains(currentPartition)) {
					newPartition = currentPartition;
				} else {
					newPartition = maxIndices.get(0);
				}
			}
			return newPartition;
		}

		/*
		 * Choose a random partition regardless
		 */
		protected short chooseRandomPartition() {
			short newPartition;
			if (maxIndices.size() == 1) {
				newPartition = maxIndices.get(0);
			} else {
				newPartition = maxIndices.get(rnd.nextInt(maxIndices.size()));
			}
			return newPartition;
		}

		/**
		 * Compute the new partition according to the neighborhood labels and the
		 * balance constraint
		 */
		protected short computeNewPartition(Vertex<IntWritable, BGRAP_VertexValue, BGRAP_EdgeValue> vertex, int totalLabels) {
			short currentPartition = vertex.getValue().getCurrentPartition();
			short newPartition = -1;
			double bestState = -Double.MAX_VALUE;
			double currentState = 0;
			maxIndices.clear();
			for (short i = 0; i < numberOfPartitions + repartition; i++) {
				// original LPA
				double LPA = ((double) partitionFrequency[i]) / totalLabels;
				// penalty function
				double PF = computeEdgeBalance(i);
				double H = lambda + LPA - lambda * PF; //RR: only use the first part for community detection
				if (i == currentPartition) {
					currentState = H;
				}
				if (H > bestState) {
					bestState = H;
					maxIndices.clear();
					maxIndices.add(i);
				} else if (H == bestState) {
					maxIndices.add(i);
				}
			}
			newPartition = chooseRandomPartitionOrCurrent(currentPartition);
			// update state stats
			aggregate(AGGREGATOR_STATE, new DoubleWritable(currentState));

			return newPartition;
		}

		@Override
		/**
		 * the computation step
		 */
		public void compute(Vertex<IntWritable, BGRAP_VertexValue, BGRAP_EdgeValue> vertex, Iterable<BGRAP_SamplingMessage> messages)
				throws IOException {
			boolean isActive = messages.iterator().hasNext();
			short currentPartition = vertex.getValue().getCurrentPartition();
			// int numberOfEdges = vertex.getNumEdges();
			int numberOfEdges = vertex.getValue().getRealOutDegree();

			// update neighbors partitions
			updateNeighborsPartitions(vertex, messages);

			// count labels occurrences in the neighborhood
			int totalLabels = computeNeighborsLabels(vertex);

			// compute the most attractive partition
			short newPartition = computeNewPartition(vertex, totalLabels);

			// request migration to the new destination
			if (newPartition != currentPartition && isActive) {
				requestMigration(vertex, numberOfEdges, currentPartition, newPartition);
			}

		}

		
		@Override
		public void preSuperstep() {
			directedGraph = getContext().getConfiguration().getBoolean(GRAPH_DIRECTED, DEFAULT_GRAPH_DIRECTED);

			if (directedGraph)
				totalNumEdges = ((LongWritable) getAggregatedValue(TOTAL_DIRECTED_OUT_EDGES)).get();
			else
				totalNumEdges = getTotalNumEdges();

			kappa = getContext().getConfiguration().getFloat(KAPPA_HANDLE, DEFAULT_KAPPA);

			additionalCapacity = getContext().getConfiguration().getFloat(ADDITIONAL_CAPACITY,
					DEFAULT_ADDITIONAL_CAPACITY);
			numberOfPartitions = (short) getContext().getConfiguration().getInt(NUM_PARTITIONS, DEFAULT_NUM_PARTITIONS);
			repartition = (short) getContext().getConfiguration().getInt(REPARTITION, DEFAULT_REPARTITION);
			lambda = getContext().getConfiguration().getFloat(LAMBDA, DEFAULT_LAMBDA);
			partitionFrequency = new int[numberOfPartitions + repartition];
			loads = new long[numberOfPartitions + repartition];
			demandAggregatorNames = new String[numberOfPartitions + repartition];
			totalEdgeCapacity = Math
					.round((totalNumEdges * (1 + additionalCapacity) / (numberOfPartitions + repartition)));

			// vertexCount = new long[numberOfPartitions + repartition];

			totalVertexCapacity = Math.round(
					(getTotalNumVertices() * (1 + additionalCapacity) / (numberOfPartitions + repartition)));
			// cache loads for the penalty function
			// Adnan : get the Vertex balance penality
			for (int i = 0; i < numberOfPartitions + repartition; i++) {
				demandAggregatorNames[i] = AGG_MIGRATION_DEMAND_PREFIX + i;
				loads[i] = ((LongWritable) getAggregatedValue(AGG_EGDES_LOAD_PREFIX + i)).get();
				//  vertexCounti] = ((LongWritable) getAggregatedValue(PARTITION_VCAPACITY_PREFIX +
				// i)).get();
			}
		}
	}
	
	/**
	 * the migration step
	 * @author Adnan
	 *
	 */
	public static class ComputeUpdatePropagatePartitions extends
			// AbstractComputation<LongWritable, VertexValue, EdgeValue, NullWritable,
			// SamplingMessage> {
			AbstractComputation<IntWritable, BGRAP_VertexValue, BGRAP_EdgeValue, IntWritable, BGRAP_SamplingMessage> {
		private Random rnd = new Random();
		protected String[] loadAggregatorNames;
		private double[] migrationProbabilities;
		private double additionalCapacity;
		protected short numberOfPartitions;
		protected short repartition;

		/**
		 * Store the current vertices-count of each partition
		 */
		protected String[] vertexCountAggregatorNames;
		
		private long totalNumEdges;
		private boolean directedGraph;

		private void migrate(Vertex<IntWritable, BGRAP_VertexValue, BGRAP_EdgeValue> vertex, short currentPartition,
				short newPartition) {
			vertex.getValue().setCurrentPartition(newPartition);
			// update partitions loads
			// int numberOfEdges = vertex.getNumEdges();
			long numberOfEdges = vertex.getValue().getRealOutDegree();

			aggregate(loadAggregatorNames[currentPartition], new LongWritable(-numberOfEdges));
			aggregate(loadAggregatorNames[newPartition], new LongWritable(numberOfEdges));

			// Adnan : update partition's vertices count
			aggregate(vertexCountAggregatorNames[currentPartition], new LongWritable(-1)); // Hung
			aggregate(vertexCountAggregatorNames[newPartition], new LongWritable(1)); // Hung

			// Adnan : to tell other that 'i am migrating'
			// Adnan : increment the total number of migration'
			aggregate(AGGREGATOR_MIGRATIONS, new LongWritable(1));
			// inform the neighbors
			BGRAP_SamplingMessage message = new BGRAP_SamplingMessage(vertex.getId().get(), newPartition);
			sendMessageToAllEdges(vertex, message);
		}

		@Override
		public void compute(Vertex<IntWritable, BGRAP_VertexValue, BGRAP_EdgeValue> vertex, 
				// Iterable<NullWritable> messages) throws IOException {
				Iterable<IntWritable> messages) throws IOException {
			if (messages.iterator().hasNext()) {
				throw new RuntimeException("messages in the migration step!");
			}
			short currentPartition = vertex.getValue().getCurrentPartition();
			short newPartition = vertex.getValue().getNewPartition();
			if (currentPartition == newPartition) {
				return;
			}
			double migrationProbability = migrationProbabilities[newPartition];
			if (rnd.nextDouble() < migrationProbability) { 						//RR: in here we can update directly the partition, no need to check for balance
				migrate(vertex, currentPartition, newPartition);
			} else {
				vertex.getValue().setNewPartition(currentPartition);
			}
		}

		@Override
		public void preSuperstep() {
			directedGraph = getContext().getConfiguration().getBoolean(GRAPH_DIRECTED, DEFAULT_GRAPH_DIRECTED);

			if (directedGraph)
				totalNumEdges = ((LongWritable) getAggregatedValue(TOTAL_DIRECTED_OUT_EDGES)).get();
			else
				totalNumEdges = getTotalNumEdges();

			additionalCapacity = getContext().getConfiguration().getFloat(ADDITIONAL_CAPACITY,
					DEFAULT_ADDITIONAL_CAPACITY);
			numberOfPartitions = (short) getContext().getConfiguration().getInt(NUM_PARTITIONS, DEFAULT_NUM_PARTITIONS);
			repartition = (short) getContext().getConfiguration().getInt(REPARTITION, DEFAULT_REPARTITION);
			long totalEdgeCapacity = Math
					.round((totalNumEdges * (1 + additionalCapacity) / (numberOfPartitions + repartition)));

			long totalVertexCapacity = Math.round(
					(getTotalNumVertices() * (1 + additionalCapacity) / (numberOfPartitions + repartition)));

			// P(A and B) = P(A)*P(B)
			migrationProbabilities = new double[numberOfPartitions + repartition];
			loadAggregatorNames = new String[numberOfPartitions + repartition];
			vertexCountAggregatorNames = new String[numberOfPartitions + repartition];
			// cache migration probabilities per destination partition
			for (int i = 0; i < numberOfPartitions + repartition; i++) {
				loadAggregatorNames[i] = AGG_EGDES_LOAD_PREFIX + i;
				vertexCountAggregatorNames[i] = AGG_VERTEX_COUNT_PREFIX + i;

				long load = ((LongWritable) getAggregatedValue(loadAggregatorNames[i])).get();
				// long vertexCount = ((LongWritable)
				// getAggregatedValue(vertexCountAggregatorNames[i])).get();

				long demand = ((LongWritable) getAggregatedValue(AGG_MIGRATION_DEMAND_PREFIX + i)).get();
				long remainingEdgeCapacity = totalEdgeCapacity - load;
				// long remainingVertexCapacity = totalVertexCapacity -  vertexCount;
				// if (demand == 0 || (remainingEdgeCapacity <= 0 && remainingVertexCapacity<=0
				// ) ) {
				if (demand == 0 || (remainingEdgeCapacity <= 0)) {
					migrationProbabilities[i] = 0;
				} else {
					migrationProbabilities[i] = ((double) (remainingEdgeCapacity)) / demand;
					// * ((double) (remainingVertexCapacity)) / demand;
					// negative score
				}
			}
		}
	}

	
	public static class ComputeFirstPartition extends ComputeNewPartition{

		/*
		 * Request migration to a new partition Adnan : update in order to recompute the
		 * number of vertex
		 */
		private void requestMigration(Vertex<IntWritable, BGRAP_VertexValue, BGRAP_EdgeValue> vertex, int numberOfEdges,
				short currentPartition, short newPartition) {
			vertex.getValue().setNewPartition(newPartition);
			aggregate(demandAggregatorNames[newPartition], new LongWritable(numberOfEdges));
			loads[newPartition] += numberOfEdges;
			//  vertexCountnewPartition] += 1;
			if (currentPartition != -1) {
				loads[currentPartition] -= numberOfEdges;
				//  vertexCountnewPartition] -= 1;
			}
		}

		/*
		 * Compute the occurrences of the labels in the neighborhood Adnan : could we
		 * use an heuristic that also consider in how many edge/vertex are present the
		 * label?
		 */
		protected int computeNeighborsLabels(Vertex<IntWritable, BGRAP_VertexValue, BGRAP_EdgeValue> vertex) {
			Arrays.fill(partitionFrequency, 0);
			int totalLabels = 0;
			int localEdges = 0;
			long externalEdges = 0;
			short p2;
			for (Edge<IntWritable, BGRAP_EdgeValue> e : vertex.getEdges()) {
				p2 = e.getValue().getPartition();
				
				if(p2==-1) continue;
				
				partitionFrequency[p2] += e.getValue().getWeight();
				totalLabels += e.getValue().getWeight();

				if (directedGraph && e.getValue().isVirtualEdge())
					continue;

				else {
					if (p2 == vertex.getValue().getCurrentPartition()) {
						localEdges++;
					}
				}
			}
			externalEdges = vertex.getValue().getRealOutDegree() - localEdges;
			// update cut edges stats
			aggregate(AGG_EDGE_CUTS, new LongWritable(externalEdges));
			aggregate(AGGREGATOR_LOCALS, new LongWritable(localEdges));

			return totalLabels;
		}

		@Override
		public void compute(Vertex<IntWritable, BGRAP_VertexValue, BGRAP_EdgeValue> vertex, Iterable<BGRAP_SamplingMessage> messages)
				throws IOException {
			boolean isActive = messages.iterator().hasNext();
			short currentPartition = vertex.getValue().getCurrentPartition();

			int numberOfEdges = vertex.getValue().getRealOutDegree();

			// update neighbors partitions
			updateNeighborsPartitions(vertex, messages);

			short newPartition = currentPartition;
			// if (currentPartition == -1) {
			// count labels occurrences in the neighborhood
			int totalLabels = computeNeighborsLabels(vertex);
			if (totalLabels > 0) {
				// compute the most attractive partition
				newPartition = computeNewPartition(vertex, totalLabels);

				// request migration to the new destination
				if (newPartition != currentPartition && isActive) {
					requestMigration(vertex, numberOfEdges, currentPartition, newPartition);
					aggregate(AGG_UPDATED_VERTICES, new LongWritable(1));
				}
			}
		}
	}

	public static class ComputeFirstUpdatePropagateStep extends ComputeUpdatePropagatePartitions{
		private Random rnd = new Random();
		private void migrate(Vertex<IntWritable, BGRAP_VertexValue, BGRAP_EdgeValue> vertex, short currentPartition,
				short newPartition) {
			vertex.getValue().setCurrentPartition(newPartition);
			// update partitions loads
			// int numberOfEdges = vertex.getNumEdges();
			long numberOfEdges = vertex.getValue().getRealOutDegree();

			if (currentPartition != -1) {
				aggregate(loadAggregatorNames[currentPartition], new LongWritable(-numberOfEdges));
				aggregate(vertexCountAggregatorNames[currentPartition], new LongWritable(-1));
			}

			aggregate(loadAggregatorNames[newPartition], new LongWritable(numberOfEdges));
			aggregate(vertexCountAggregatorNames[newPartition], new LongWritable(1));

			aggregate(AGGREGATOR_MIGRATIONS, new LongWritable(1));
			// inform the neighbors
			// Adnan : to tell other that 'i am migrating'
			BGRAP_SamplingMessage message = new BGRAP_SamplingMessage(vertex.getId().get(), newPartition);
			sendMessageToAllEdges(vertex, message);
		}

		@Override
		public void compute(Vertex<IntWritable, BGRAP_VertexValue, BGRAP_EdgeValue> vertex,
				// Iterable<NullWritable> messages) throws IOException {
				Iterable<IntWritable> messages) throws IOException {
			if (messages.iterator().hasNext()) {
				throw new RuntimeException("messages in the migration step!");
			}
			short currentPartition = vertex.getValue().getCurrentPartition();
			short newPartition = vertex.getValue().getNewPartition();

			if (newPartition == -1) {
				newPartition = (short) rnd.nextInt(numberOfPartitions + repartition);
				vertex.getValue().setNewPartition(newPartition);
			}
			if (currentPartition == newPartition) {
				return;
			}

			migrate(vertex, currentPartition, newPartition);

			// System.out.println("Vertex: " + vertex.getId().get() + " cpart: " +
			// currentPartition + " npart: " + newPartition);
		}
	}

	/**
	 * The core of the partitioning algorithm executed on the Master
	 * 
	 * @author Adnan
	 *
	 */
	public static class PartitionerMasterComputeTest extends SuperPartitionerMasterCompute {
		@Override
		public void compute() {
			balance_algorithm = "bgrap.EDGE_BALANCE";
			super.compute();
		}
	}
}
