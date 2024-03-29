/**
 * Copyright 2021 www.lri.fr
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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Locale;
import java.util.Random;
import java.util.Map.Entry;

import org.apache.giraph.aggregators.BooleanAndAggregator;
import org.apache.giraph.aggregators.BooleanOrAggregator;
import org.apache.giraph.aggregators.BooleanOverwriteAggregator;
import org.apache.giraph.aggregators.DoubleSumAggregator;
import org.apache.giraph.aggregators.IntMaxAggregator;
import org.apache.giraph.aggregators.IntOverwriteAggregator;
import org.apache.giraph.aggregators.IntSumAggregator;
import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.counters.GiraphTimers;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.AbstractComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.Task;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
// Added by Hung
import org.apache.hadoop.io.NullWritable;
import giraph.ml.grafos.okapi.common.data.LongArrayListWritable;

import giraph.lri.lahdak.bgrap.*;
import giraph.lri.lahdak.bgrap.BGRAP_Samplers.*;
import giraph.lri.lahdak.bgrap.utils.BGRAP_EdgeValue;
import giraph.lri.lahdak.bgrap.utils.HashMapAggregator;
import giraph.lri.lahdak.bgrap.utils.BGRAP_SamplingMessage;
import giraph.lri.lahdak.bgrap.utils.BGRAP_VertexValue;
import giraph.ml.grafos.okapi.common.computation.SendFriends;
import java.util.HashSet;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Computation;

import com.esotericsoftware.minlog.Log;
import com.google.common.collect.Lists;

/**
 *
 *
 */
@SuppressWarnings("unused")
public class BGRAP_Partitionning {
	// GENERAL VARIABLES
	public final static String SAVE_PATH = "bgrap.statistics.path";
	public static String DEFAULT_SAVE_PATH = System.getenv("HOME") + "/BGRAP/Results/";
	public static final String SAVE_STATS = "partition.SaveStatsIntoFile";

	// ALGORITHM PARAMETERS
	// execution and graph environment
	public static final String MAX_ITERATIONS_LP = "bgrap.MaxIterationsLP";
	protected static final int DEFAULT_MAX_ITERATIONS = 290;// 10;//290;
	public static final String GRAPH_DIRECTED = "bgrap.directed";
	public static final boolean DEFAULT_GRAPH_DIRECTED = false;
	public static final String EDGE_WEIGHT = "bgrap.weight";
	public static final byte DEFAULT_EDGE_WEIGHT = 1; // RR: Shouldn't edge weights be respected? It helps convergence
														// speed

	// initialization degreeThreshold
	protected static int outDegreeThreshold;
	public static final String COMPUTE_OUTDEGREE_THRESHOLD = "partition.OUTDEGREE_THRESHOLD"; // makes the algorithm set
																								// the threshold for
																								// initialization
	protected static final boolean DEFAULT_COMPUTE_OUTDEGREE_THRESHOLD = true;
	public static final String OUTDEGREE_THRESHOLD = "partition.OUTDEGREE_THRESHOLD"; // RR: What is the use of this?!
																						// it will initialize all nodes
	protected static final int DEFAULT_OUTDEGREE_Threshold = 1;
	public static final String MIN_OUTDEGREE_THRESHOLD = "partition.MIN_OUTDEGREE_THRESHOLD"; // RR: completely useless
	protected static final int DEFAULT_MIN_OUTDEGREE_Threshold = 25; // RR: completely useless

	// user objectives
	public static final String NUM_PARTITIONS = "bgrap.numberOfPartitions";
	protected static final int DEFAULT_NUM_PARTITIONS = 8;// 32;
	protected static final String REPARTITION = "bgrap.repartition";
	protected static final short DEFAULT_REPARTITION = 0;
	protected static final String ADDITIONAL_CAPACITY = "bgrap.additionalCapacity";
	protected static final float DEFAULT_ADDITIONAL_CAPACITY = 0.05f;

	// convergence
	protected static final String LAMBDA = "bgrap.lambda"; // constraints the penalty value => Why is it added as well
															// to H?
	protected static final float DEFAULT_LAMBDA = 1.0f;
	public static final String KAPPA_HANDLE = "partition.edgeBalanceWeight.kappa"; // for scoring, in vb gives more
																					// priority to
	// vb or ec
	protected static final float DEFAULT_KAPPA = 0.5f;
	protected static final String WINDOW_SIZE = "bgrap.windowSize";
	protected static final int DEFAULT_WINDOW_SIZE = 5;
	protected static final String CONVERGENCE_THRESHOLD = "bgrap.threshold";
	protected static final float DEFAULT_CONVERGENCE_THRESHOLD = 0.001f;

	// AGGREGATORS
	// score
	protected static final String AGGREGATOR_STATE = "AGG_STATE"; // To calculate overall solution score

	// migration
	protected static final String AGGREGATOR_MIGRATIONS = "AGG_MIGRATIONS"; // Total of migrating v (for eb and vb)
	protected static final String AGG_MIGRATION_DEMAND_PREFIX = "AGG_EDGE_MIGRATION_DEMAND_"; // only for eb
	protected static final String AGG_VERTEX_MIGRATION_DEMAND_PREFIX = "AGG_VERTEX_MIGRATION_DEMAND_"; // only for vb

	// edges
	protected static final String TOTAL_DIRECTED_OUT_EDGES = "#Total Directed Out Edges"; // initial graph out-edges

	protected static final String AGG_EGDES_LOAD_PREFIX = "AGG_LOAD_"; // Edges in a partition for balancing
	protected static final String AGG_REAL_LOAD_PREFIX = "AGG_REAL_LOAD_";
	protected static final String AGG_VIRTUAL_LOAD_PREFIX = "AGG_VIRTUAL_LOAD_";

	protected static final String AGGREGATOR_LOCALS = "AGG_LOCALS"; // Local edges (intra-partition)
	protected static final String AGG_REAL_LOCAL_EDGES = "AGG_REAL_LOCALS";
	protected static final String AGG_VIRTUAL_LOCALS = "AGG_VIRTUAL_LOCALS";

	protected static final String AGG_EDGE_CUTS = "# Edge cuts"; // External edges (a.k.a. edge-cut)
	protected static final String AGG_REAL_EDGE_CUTS = "# REAL Edge cuts";
	protected static final String AGG_VIRTUAL_EDGE_CUTS = "# VIRTUAL Edge cuts";

	// AUTRES
	// OUTDEGREE_FREQUENCY_COUNTER => not even defined, it is for the histogram

	protected static final String COUNTER_GROUP = "Partitioning Counters";
	protected static final String MIGRATIONS_COUNTER = "Migrations";
	protected static final String ITERATIONS_COUNTER = "Iterations";
	protected static final String PCT_LOCAL_EDGES_COUNTER = "Local edges (%)";
	protected static final String MAXMIN_UNBALANCE_COUNTER = "Maxmin unbalance (x1000)";
	protected static final String MAX_NORMALIZED_UNBALANCE_COUNTER = "Max normalized unbalance (x1000)";
	protected static final String SCORE_COUNTER = "Score (x1000)";

	protected static final String PARTITION_COUNTER_GROUP = "Partitioning Counters";
	// Adnan
	protected static long totalVertexNumber; // RR: changed to int
	// Adnan : ration Communication Computation

	// Adnan: some statistics about the initialization
	protected static final String AGG_INITIALIZED_VERTICES = "Initialized vertex %";
	protected static final String AGG_UPDATED_VERTICES = "Updated vertex %";
	// Adnan : the outgoing edges of initialized vertices
	protected static final String AGG_FIRST_LOADED_EDGES = "First Loaded Edges %";

	// Adnan: some statistics about the partition
	// global stat

	protected static final String AGG_UPPER_TOTAL_COMM_VOLUME = "# CV Upper Bound";

	// local stat (per partition)
	protected static final String FINAL_AGG_VERTEX_COUNT_PREFIX = "FINAL_AGG_VERTEX_CAPACITY_";
	protected static final String AGG_VERTEX_COUNT_PREFIX = "AGG_VERTEX_CAPACITY_";
	protected static final String AGG_AVG_DEGREE_PREFIX = "AGG_AVG_DEGREE_CAPACITY_";

	public static final String DEBUG = "graph.debug";

	// Adnan: some statistics about the partition

	protected static final String AGG_REAL_TOTAL_COMM_VOLUME = "REAL Communication Volume";
	protected static final String AGG_VIRTUAL_TOTAL_COMM_VOLUME = "VIRTUAL Communication Volume";

	protected static final String AGG_REAL_UPPER_TOTAL_COMM_VOLUME = "#REAL CV Upper Bound";
	protected static final String AGG_VIRTUAL_UPPER_TOTAL_COMM_VOLUME = "#VIRTUAL CV Upper Bound";

	public static final String Vertex_Balance_JSD_COUNTER = "Vertex Balance JSD";
	public static final String Edge_Balance_JSD_COUNTER = "Edge Balance JSD";
	public static final String OUTDEGREE_FREQUENCY_COUNTER = "OUTDEGREE_FREQUENCY_COUNTER_";

	public static final String BALANCE_ALGORITHM_HANDLE = "bgrap.balance";
	protected static String BALANCE_ALGORITHM = "bgrap.EDGE_BALANCE";

	////////////////////////
	///// RR VARIABLES /////
	////////////////////////

	// Variables for saving results in files
	protected static final String DELIMITER = ";";
	public static final String DATE_HANDLE = "bgrap.DATE";
	protected static String formattedDate;
	public static final String GRAPH_NAME_HANDLE = "bgrap.graph.NAME";
	protected static String GRAPH_NAME = "Unknown";
	public static final String SAVE_DD_HANDLE = "rankdegree.SAVE_DD";
	protected static boolean SAVE_DD = false;

	// RANK DEGREE SAMPLING
	public static boolean NEEDS_MORE_SAMPLE;
	public static int SAMPLE_SIZE=0, SAMPLE_CONVERGENCE_CYCLES=0;
	protected static String INITIALIZATION_TYPE;
	protected static final String DEFAULT_SAMPLING_TYPE = "InitializeSampleRD";
	public static final String INITIALIZATION_TYPE_HANDLE = "bgrap.INITIALIZATION_TYPE";

	// number of sampled vertices
	public static final String BETA_HANDLE = "rankdegree.BETA";
	protected static final float BETA_DEFAULT = 0.15f;
	protected static float BETA_P;
	protected static int BETA;
	// number of seeds
	public static final String SIGMA_HANDLE = "rankdegree.SIGMA";
	protected static final float SIGMA_DEFAULT = 0.05f;
	protected static float SIGMA_P;
	protected static int SIGMA;
	// number of neighbors taken
	public static final String TAU_HANDLE = "rankdegree.TAU";
	protected static final int TAU_DEFAULT = 3;
	protected static int TAU;

	// protected static boolean[] initializedPartition;
	protected static Random r = new Random();

	// VARIABLE NAMERS
	// for degree distribution
	public static final String VIRTUAL_DEGREE_FREQUENCY_COUNTER = "VIRTUAL_DEGREE_FREQUENCY_COUNTER_";

	// AGGREGATORS
	protected static final String AGG_VERTICES = "VERTICES";
	protected static final String AGG_MAX_DEGREE = "MAX_DEGREE";
	protected static final String AGG_DEGREE_DIST = "DEGREE_DIST";
	protected static final String AGG_SAMPLE = "SAMPLE";
	protected static final String AGG_SAMPLE_SS = "SAMPLED_IN_SUPERSTEP";
	protected static final String AGG_SAMPLE_SSR = "SAMPLED_IN_SUPERSTEP_FOR_REAL";
	protected static final String AGG_CL_COEFFICIENT = "CL_COEFFICIENT";
	protected static final String FRIENDS_COUNTER = "FRIENDS_COUNTER";

	// RESULTS
	// super steps
	protected static short sampling_ss_start = 2;
	protected static short sampling_ss_extra = 0; // super steps to guarantee partition initializing
	protected static short sampling_ss_end;
	protected static short lp_ss_end;

	// messages
	protected static long sampling_messages_start;
	protected static long sampling_messages_end;
	protected static long lp_messages_end;

	// ALGORITHM SPECIFIC
	// Hashmaps
	protected static MapWritable degreeDist = new MapWritable();

	protected static HashMap<Integer, Float> degreeProb = new HashMap<Integer, Float>();
	// protected static HashMap<Long,Double> coefMap = new HashMap<Long,Double>();

	protected static float adjustingFactorSeed = 0.0f;
	protected static float relaxingFactorPropagation = 0.0f;
	public static boolean SAMPLING_CONVERGED;
	public static final String AGG_SAMPLING_CONVERGED="bgrap.SAMPLING_CONVERGED";
	public static final String AGG_NEED_MORE_SAMPLE="bgrap.NEED_MORE_SAMPLING";

	
	//AEM
	protected static Class<?> cfp, cfm, cm, cnp, initializationClass;
	protected static Logger logger = Logger.getLogger(BGRAP_Partitionning.class);

	/**
	 * SS0: CONVERTER PROPAGATE <br>
	 * In this first superstep, each vertices send their IDs to their neighbors.
	 * This allows in a directed graph, in next superstep, to discover the vertices
	 * connected by income edges (for example the followers).
	 * 
	 * @author adnanmoe
	 *
	 */
	public static class ConverterPropagate
			extends AbstractComputation<IntWritable, BGRAP_VertexValue, BGRAP_EdgeValue, IntWritable, IntWritable> {

		@Override
		public void compute(Vertex<IntWritable, BGRAP_VertexValue, BGRAP_EdgeValue> vertex,
				Iterable<IntWritable> messages) throws IOException {
			aggregate(TOTAL_DIRECTED_OUT_EDGES, new LongWritable(vertex.getNumEdges()));
			vertex.getValue().setRealOutDegree(vertex.getNumEdges());
			sendMessageToAllEdges(vertex, vertex.getId());
			// RR:
			aggregate(AGG_VERTICES, new LongWritable(1));
		}
	}

	/**
	 * SS1: CONVERTER UPDATE EDGES<br>
	 * Here the graph is converted to an undirected weighted graph, by making all
	 * edges Bidirectional. Each vertex knows only its outcome edges, the messages
	 * received from the previous superstep allow to discover new neighbor vertices
	 * then add a new out edge if it's missing.
	 * 
	 * @author adnanmoe
	 *
	 */
	public static class ConverterUpdateEdges extends
			AbstractComputation<IntWritable, BGRAP_VertexValue, BGRAP_EdgeValue, IntWritable, BGRAP_SamplingMessage> {
		private byte edgeWeight;
		private String[] outDegreeFrequency;
		private int avgDegree;

		@Override
		public void compute(Vertex<IntWritable, BGRAP_VertexValue, BGRAP_EdgeValue> vertex,
				Iterable<IntWritable> messages) throws IOException {
			// For BGRAP
			if (INITIALIZATION_TYPE.contentEquals("BGRAP")) {
				if (vertex.getNumEdges() >= 100) {
					aggregate(outDegreeFrequency[100], new IntWritable(1));
				} else {
					aggregate(outDegreeFrequency[vertex.getNumEdges()], new IntWritable(1));
				}
			}

			int inter = 0;
			for (IntWritable other : messages) {
				BGRAP_EdgeValue edgeValue = vertex.getEdgeValue(other);
				// if edge to other vertex doesn't exist, create a virtual one for this node
				if (edgeValue == null || edgeValue.isVirtualEdge()) {
					edgeValue = new BGRAP_EdgeValue();
					edgeValue.setWeight((byte) 1);
					edgeValue.setVirtualEdge(true);
					Edge<IntWritable, BGRAP_EdgeValue> edge = EdgeFactory.create(new IntWritable(other.get()),
							edgeValue);
					vertex.addEdge(edge);
					// aggregate(AGG_REAL_LOCAL_EDGES, new LongWritable(1));
				} else {
					// else change the edge value to a default one for edgecut calculations
					edgeValue = new BGRAP_EdgeValue(); // RR: really necessary to create another edge? reuse the same!
														// just comment and see result
					edgeValue.setWeight(edgeWeight);
					edgeValue.setVirtualEdge(false);
					vertex.setEdgeValue(other, edgeValue);
				}
				inter++;
			}
			// set real in-degree
			vertex.getValue().setRealInDegree(inter);

			// RR: calculate max degree
			aggregate(AGG_MAX_DEGREE,
					new IntWritable(vertex.getValue().getRealOutDegree() + vertex.getValue().getRealInDegree()));

			// SEND MESSAGE TO KEEP ALIVE
			sendMessageToAllEdges(vertex, new BGRAP_SamplingMessage(vertex.getId().get(), -1));
		}

		// sets the edge-weight
		@Override
		public void preSuperstep() {

			edgeWeight = (byte) getContext().getConfiguration().getInt(EDGE_WEIGHT, DEFAULT_EDGE_WEIGHT);
			INITIALIZATION_TYPE = getContext().getConfiguration().get(INITIALIZATION_TYPE_HANDLE,
					DEFAULT_SAMPLING_TYPE);
			if (INITIALIZATION_TYPE.contentEquals("BGRAP")) {
				outDegreeFrequency = new String[101];
				for (int i = 0; i < 101; i++) {
					outDegreeFrequency[i] = OUTDEGREE_FREQUENCY_COUNTER + i;
				}
			}

			// RR: change SIGMA percentage to long
			totalVertexNumber = ((LongWritable) getAggregatedValue(AGG_VERTICES)).get();
			BETA_P = getContext().getConfiguration().getFloat(BETA_HANDLE, (float) BETA_DEFAULT);
			BETA = ((int) (BETA_P * totalVertexNumber)) + 1;
			// BETA = (int) Math.ceil(Double.parseDouble(new
			// Float(BETA_P*totalVertexNumber).toString()));
			SIGMA_P = getContext().getConfiguration().getFloat(SIGMA_HANDLE, (float) SIGMA_DEFAULT);
			SIGMA = ((int) (SIGMA_P * totalVertexNumber)) + 1;
			// SIGMA = (int) Math.ceil(Double.parseDouble(new
			// Float(SIGMA_P*totalVertexNumber).toString()));
			TAU = getContext().getConfiguration().getInt(TAU_HANDLE, TAU_DEFAULT);
			// System.out.println("*Total Vertices:"+totalVertexNumber);
			// System.out.println("*BETA_P:"+BETA_P);
			// System.out.println("*SIGMA_P:"+SIGMA_P);
		}
	}

	/**
	 * SS2: POTENTIAL VERTICES INITIALIZER This computation class allows to
	 * initialize the vertices basing on BGRAP_init. Here we initialize only the hub
	 * vertices having RealOutDegree higher then a user given threshold, by default
	 * the average outdegree.
	 * 
	 * @author adnanmoe
	 *
	 */
	// public static class BGRAP_Init_HubVerticesInitializer extends
	public static class BGRAP_HubVerticesInitializer extends
			AbstractComputation<IntWritable, BGRAP_VertexValue, BGRAP_EdgeValue, BGRAP_SamplingMessage, BGRAP_SamplingMessage> {
		protected String[] loadAggregatorNames;
		protected int numberOfPartitions;
		protected Random rnd = new Random();

		/**
		 * Store the current vertices-count of each partition
		 */
		protected String[] vertexCountAggregatorNames;
		protected long totalNumEdges;
		protected boolean directedGraph;
		private boolean debug;
		private int minOutDegreeThreshold;
		private int degreeThreshold;

		@Override
		public void compute(Vertex<IntWritable, BGRAP_VertexValue, BGRAP_EdgeValue> vertex,
				Iterable<BGRAP_SamplingMessage> messages) throws IOException {
			short partition = vertex.getValue().getCurrentPartition();
			// long numEdges = vertex.getNumEdges();
			long numOutEdges = vertex.getNumEdges();
			if (directedGraph) {
				numOutEdges = vertex.getValue().getRealOutDegree();
			}

			if ((partition == -1 && vertex.getValue().getRealOutDegree() > outDegreeThreshold) || partition == -2) { // RR:
																														// steps
				// initialize only hub vertices
				partition = (short) rnd.nextInt(numberOfPartitions);

				aggregate(loadAggregatorNames[partition], new LongWritable(numOutEdges));
				aggregate(vertexCountAggregatorNames[partition], new LongWritable(1));

				vertex.getValue().setCurrentPartition(partition);
				vertex.getValue().setNewPartition(partition);
				BGRAP_SamplingMessage message = new BGRAP_SamplingMessage(vertex.getId().get(), partition);
				sendMessageToAllEdges(vertex, message);

				aggregate(AGG_INITIALIZED_VERTICES, new LongWritable(1));
				aggregate(AGG_FIRST_LOADED_EDGES, new LongWritable(numOutEdges));
			}

			aggregate(AGG_UPPER_TOTAL_COMM_VOLUME, new LongWritable(Math.min(numberOfPartitions, numOutEdges)));
		}

		@Override
		public void preSuperstep() {
			directedGraph = getContext().getConfiguration().getBoolean(GRAPH_DIRECTED, DEFAULT_GRAPH_DIRECTED);

			totalNumEdges = ((LongWritable) getAggregatedValue(TOTAL_DIRECTED_OUT_EDGES)).get();
			// totalVertexNumber = getTotalNumVertices(); //RR: changed it because the
			// method is only the number of active vertices on previous SS
			totalVertexNumber = ((LongWritable) getAggregatedValue(AGG_VERTICES)).get();

			degreeThreshold = getContext().getConfiguration().getInt(OUTDEGREE_THRESHOLD, DEFAULT_OUTDEGREE_Threshold);
			minOutDegreeThreshold = getContext().getConfiguration().getInt(MIN_OUTDEGREE_THRESHOLD,
					DEFAULT_MIN_OUTDEGREE_Threshold);

			if (getContext().getConfiguration().getBoolean(COMPUTE_OUTDEGREE_THRESHOLD,
					DEFAULT_COMPUTE_OUTDEGREE_THRESHOLD)) {
				outDegreeThreshold = (int) Math.ceil((double) totalNumEdges / (double) totalVertexNumber);
			} else {
				outDegreeThreshold = degreeThreshold;
			}

			numberOfPartitions = getContext().getConfiguration().getInt(NUM_PARTITIONS, DEFAULT_NUM_PARTITIONS);
			loadAggregatorNames = new String[numberOfPartitions];
			vertexCountAggregatorNames = new String[numberOfPartitions];
			for (int i = 0; i < numberOfPartitions; i++) {
				loadAggregatorNames[i] = AGG_EGDES_LOAD_PREFIX + i;
				vertexCountAggregatorNames[i] = AGG_VERTEX_COUNT_PREFIX + i;
			}
			debug = getContext().getConfiguration().getBoolean(DEBUG, false);
			// System.out.println("PreSuperstep Initializer : outDegreeThreshold=" +
			// outDegreeThreshold);
			// aggregate(OUTDEGREE_THRESHOLD, new LongWritable(outDegreeThreshold));
		}
	}

	/**
	 * SS2: DEFAULT VERTICES INITIALIZER This computation class initializes each
	 * vertex with a random label, w.r.t. to the number of desired partitions.
	 * 
	 * @author Adnan EL MOUSSAWI
	 *
	 */
	public static class BGRAP_DefaultInitializer extends
			AbstractComputation<IntWritable, BGRAP_VertexValue, BGRAP_EdgeValue, BGRAP_SamplingMessage, BGRAP_SamplingMessage> {
		protected String[] loadAggregatorNames;
		protected int numberOfPartitions;
		protected Random rnd = new Random();

		/**
		 * Store the current vertices-count of each partition
		 */
		protected String[] vertexCountAggregatorNames;
		protected long totalNumEdges;
		protected boolean directedGraph;
		private int minOutDegreeThreshold;
		private int degreeThreshold;

		@Override
		public void compute(Vertex<IntWritable, BGRAP_VertexValue, BGRAP_EdgeValue> vertex,
				Iterable<BGRAP_SamplingMessage> messages) throws IOException {
			short partition = vertex.getValue().getCurrentPartition();
			// long numEdges = vertex.getNumEdges();
			long numOutEdges = vertex.getNumEdges();
			if (directedGraph) {
				numOutEdges = vertex.getValue().getRealOutDegree();
			}

			if (partition < 0) {
				partition = (short) rnd.nextInt(numberOfPartitions);

				aggregate(loadAggregatorNames[partition], new LongWritable(numOutEdges));
				aggregate(vertexCountAggregatorNames[partition], new LongWritable(1));

				vertex.getValue().setCurrentPartition(partition);
				vertex.getValue().setNewPartition(partition);
				BGRAP_SamplingMessage message = new BGRAP_SamplingMessage(vertex.getId().get(), partition);
				sendMessageToAllEdges(vertex, message);

				aggregate(AGG_INITIALIZED_VERTICES, new LongWritable(1));
				aggregate(AGG_FIRST_LOADED_EDGES, new LongWritable(numOutEdges));
			}

			aggregate(AGG_UPPER_TOTAL_COMM_VOLUME, new LongWritable(Math.min(numberOfPartitions, numOutEdges)));
		}

		@Override
		public void preSuperstep() {
			directedGraph = getContext().getConfiguration().getBoolean(GRAPH_DIRECTED, DEFAULT_GRAPH_DIRECTED);

			totalNumEdges = ((LongWritable) getAggregatedValue(TOTAL_DIRECTED_OUT_EDGES)).get();
			// totalVertexNumber = getTotalNumVertices(); //RR: changed it because the
			// method is only the number of active vertices on previous SS
			totalVertexNumber = ((LongWritable) getAggregatedValue(AGG_VERTICES)).get();

			numberOfPartitions = getContext().getConfiguration().getInt(NUM_PARTITIONS, DEFAULT_NUM_PARTITIONS);
			loadAggregatorNames = new String[numberOfPartitions];
			vertexCountAggregatorNames = new String[numberOfPartitions];
			for (int i = 0; i < numberOfPartitions; i++) {
				loadAggregatorNames[i] = AGG_EGDES_LOAD_PREFIX + i;
				vertexCountAggregatorNames[i] = AGG_VERTEX_COUNT_PREFIX + i;
			}
		}
	}

	/**
	 * SS2: REPARTITIONER This computation class allows to adapt an exiting
	 * partition to some system changes, by resizing the partition.
	 *
	 */
	public static class Repartitioner extends
			AbstractComputation<IntWritable, BGRAP_VertexValue, BGRAP_EdgeValue, BGRAP_SamplingMessage, BGRAP_SamplingMessage> { // RR:Change
																																	// to
																																	// SamplingMessage
		private String[] loadAggregatorNames;
		private int numberOfPartitions;
		private short repartition;
		private double migrationProbability;

		/**
		 * Store the current vertices-count of each partition
		 */
		private String[] vertexCountAggregatorNames;
		private boolean directedGraph;
		private long totalNumEdges;

		@Override
		public void compute(Vertex<IntWritable, BGRAP_VertexValue, BGRAP_EdgeValue> vertex,
				Iterable<BGRAP_SamplingMessage> messages) // RR:Change to SamplingMessage
				throws IOException {
			short partition;
			short currentPartition = vertex.getValue().getCurrentPartition();

			long numEdges;
			if (directedGraph)
				numEdges = vertex.getValue().getRealOutDegree();
			else
				numEdges = vertex.getNumEdges();
			// down-scale
			if (repartition < 0) {
				if (currentPartition >= numberOfPartitions + repartition) {
					partition = (short) r.nextInt(numberOfPartitions + repartition);
				} else {
					partition = currentPartition;
				}
				// up-scale
			} else if (repartition > 0) {
				if (r.nextDouble() < migrationProbability) {
					partition = (short) (numberOfPartitions + r.nextInt(repartition));
				} else {
					partition = currentPartition;
				}
			} else {
				throw new RuntimeException("Repartitioner called with " + REPARTITION + " set to 0");
			}
			aggregate(loadAggregatorNames[partition], new LongWritable(numEdges));
			aggregate(vertexCountAggregatorNames[partition], new LongWritable(1));

			vertex.getValue().setCurrentPartition(partition);
			vertex.getValue().setNewPartition(partition);

			aggregate(AGG_UPPER_TOTAL_COMM_VOLUME,
					new LongWritable(Math.min(numberOfPartitions + repartition, numEdges)));

			BGRAP_SamplingMessage message = new BGRAP_SamplingMessage(vertex.getId().get(), partition);
			sendMessageToAllEdges(vertex, message);
		}

		@Override
		public void preSuperstep() {
			directedGraph = getContext().getConfiguration().getBoolean(GRAPH_DIRECTED, DEFAULT_GRAPH_DIRECTED);

			if (directedGraph)
				totalNumEdges = ((LongWritable) getAggregatedValue(TOTAL_DIRECTED_OUT_EDGES)).get();
			else
				totalNumEdges = getTotalNumEdges();

			numberOfPartitions = getContext().getConfiguration().getInt(NUM_PARTITIONS, DEFAULT_NUM_PARTITIONS);
			repartition = (short) getContext().getConfiguration().getInt(REPARTITION, DEFAULT_REPARTITION);
			migrationProbability = ((double) repartition) / (repartition + numberOfPartitions);
			loadAggregatorNames = new String[numberOfPartitions + repartition];
			vertexCountAggregatorNames = new String[numberOfPartitions + repartition];
			for (int i = 0; i < numberOfPartitions + repartition; i++) {
				loadAggregatorNames[i] = AGG_EGDES_LOAD_PREFIX + i;
				vertexCountAggregatorNames[i] = AGG_VERTEX_COUNT_PREFIX + i;
			}
		}
	}

	/**
	 * Reset the partition of each vertex
	 *
	 */
	public static class ResetPartition
			// AbstractComputation<LongWritable, VertexValue, EdgeValue, LongWritable,
			// BGRAP_PartitionMessage>
			extends
			AbstractComputation<IntWritable, BGRAP_VertexValue, BGRAP_EdgeValue, IntWritable, BGRAP_SamplingMessage> {// RR:Change
																														// to
																														// SamplingMessage
		@Override
		public void compute(Vertex<IntWritable, BGRAP_VertexValue, BGRAP_EdgeValue> vertex,
				Iterable<IntWritable> messages) throws IOException {
			short partition = -1;

			vertex.getValue().setCurrentPartition(partition);
			vertex.getValue().setNewPartition(partition);

			BGRAP_SamplingMessage message = new BGRAP_SamplingMessage(vertex.getId().get(), partition); // RR:Change to
																										// SamplingMessage
			sendMessageToAllEdges(vertex, message);
		}
	}

	/**
	 * Frequent label computation step.
	 *
	 */
	public static class ComputeNewPartition extends
			AbstractComputation<LongWritable, BGRAP_VertexValue, BGRAP_EdgeValue, BGRAP_SamplingMessage, LongWritable> {// RR:Change
																														// to
																														// SamplingMessage

		@Override
		public void compute(Vertex<LongWritable, BGRAP_VertexValue, BGRAP_EdgeValue> vertex,
				Iterable<BGRAP_SamplingMessage> messages)// RR:Change to SamplingMessage
				throws IOException {
		}

	}

	/**
	 * Label update and propagate step.
	 * 
	 */
	public static class UpdatePropagatePartition extends
			AbstractComputation<LongWritable, BGRAP_VertexValue, BGRAP_EdgeValue, LongWritable, BGRAP_SamplingMessage> {// RR:Change
																														// to
																														// SamplingMessage

		@Override
		public void compute(Vertex<LongWritable, BGRAP_VertexValue, BGRAP_EdgeValue> vertex,
				Iterable<LongWritable> messages) throws IOException {
		}
	}

	/**
	 * Compute some statistics about the quality of the partition.
	 * 
	 * @author Adnan EL MOUSSAWI
	 *
	 */
	public static class ComputeGraphPartitionStatistics extends
			AbstractComputation<IntWritable, BGRAP_VertexValue, BGRAP_EdgeValue, BGRAP_SamplingMessage, IntWritable> {// RR:Change
																														// to
																														// SamplingMessage
		private byte edgeWeight;
		private boolean graphDirected;
		private byte newReverseEdgesWeight;

		private ShortArrayList maxIndices = new ShortArrayList();
		//
		private String[] demandAggregatorNames;
		private int[] partitionFrequency;
		/**
		 * connected partitions
		 */
		private ShortArrayList pConnect;

		/**
		 * Adnan: the balanced capacity of a partition |E|/K, |V|/K
		 */
		private long totalEdgeCapacity;
		private long totalVertexCapacity;
		private short totalNbPartitions;
		private double additionalCapacity;
		private long totalNumEdges;
		private boolean directedGraph;
		private String[] realLoadAggregatorNames;
		private String[] finalVertexCounts;
		private String[] virtualLoadAggregatorNames;
		private boolean debug;

		@Override
		public void compute(Vertex<IntWritable, BGRAP_VertexValue, BGRAP_EdgeValue> vertex,
				Iterable<BGRAP_SamplingMessage> messages)// RR:Change to SamplingMessage
				throws IOException {
			short p = vertex.getValue().getCurrentPartition();
			long realLocalEdges = 0, realInterEdges = 0, virtualLocalEdges = 0, virtualInterEdges = 0, realCV = 0;
			short p2;

			pConnect = new ShortArrayList();

			if (debug) {
				// System.out.println(vertex.getId() + "\t" +
				// vertex.getValue().getRealOutDegree() + "\t"
				// + vertex.getValue().getRealInDegree());
			}

			// Adnan : update partition's vertices count
			aggregate(finalVertexCounts[p], new LongWritable(1));
			aggregate(realLoadAggregatorNames[p], new LongWritable(vertex.getValue().getRealOutDegree()));
			aggregate(virtualLoadAggregatorNames[p], new LongWritable(vertex.getNumEdges()));

			for (Edge<IntWritable, BGRAP_EdgeValue> e : vertex.getEdges()) {
				p2 = e.getValue().getPartition();

				if (debug) {
					// System.out.println(vertex.getId() + "-->" + e.getTargetVertexId() + "\t"
					// + e.getValue().isVirtualEdge() + "\t" + e.getValue().getWeight());
				}

				if (p2 == p) {
					if (!e.getValue().isVirtualEdge()) {
						realLocalEdges++;
					}
					virtualLocalEdges++;
				} else {
					if (!pConnect.contains(p2)) {
						pConnect.add(p2);
						if (!e.getValue().isVirtualEdge())
							realCV++;
					}
				}

			}
			realInterEdges = vertex.getValue().getRealOutDegree() - realLocalEdges;
			virtualInterEdges = vertex.getNumEdges() - virtualLocalEdges;
			// update cut edges stats
			aggregate(AGG_REAL_LOCAL_EDGES, new LongWritable(realLocalEdges));
			// ADNAN : update Total Comm Vol. State
			aggregate(AGG_REAL_EDGE_CUTS, new LongWritable(realInterEdges));
			// ADNAN : update Total Comm Vol. State
			aggregate(AGG_REAL_TOTAL_COMM_VOLUME, new LongWritable(realCV));

			aggregate(AGG_REAL_UPPER_TOTAL_COMM_VOLUME,
					new LongWritable(Math.min(totalNbPartitions, vertex.getValue().getRealOutDegree())));

			// update cut edges stats
			aggregate(AGG_VIRTUAL_LOCALS, new LongWritable(virtualLocalEdges));
			// ADNAN : update Total Comm Vol. State
			aggregate(AGG_VIRTUAL_EDGE_CUTS, new LongWritable(virtualInterEdges));
			// ADNAN : update Total Comm Vol. State
			aggregate(AGG_VIRTUAL_TOTAL_COMM_VOLUME, new LongWritable(pConnect.size()));

			aggregate(AGG_VIRTUAL_UPPER_TOTAL_COMM_VOLUME,
					new LongWritable(Math.min(totalNbPartitions, vertex.getNumEdges())));
		}

		@Override
		public void preSuperstep() {
			directedGraph = getContext().getConfiguration().getBoolean(GRAPH_DIRECTED, DEFAULT_GRAPH_DIRECTED);
			debug = getContext().getConfiguration().getBoolean(DEBUG, false);

			additionalCapacity = getContext().getConfiguration().getFloat(ADDITIONAL_CAPACITY,
					DEFAULT_ADDITIONAL_CAPACITY);
			totalNbPartitions = (short) (getContext().getConfiguration().getInt(NUM_PARTITIONS, DEFAULT_NUM_PARTITIONS)
					+ getContext().getConfiguration().getInt(REPARTITION, DEFAULT_REPARTITION));

			realLoadAggregatorNames = new String[totalNbPartitions];
			virtualLoadAggregatorNames = new String[totalNbPartitions];
			finalVertexCounts = new String[totalNbPartitions];
			for (int i = 0; i < totalNbPartitions; i++) {
				realLoadAggregatorNames[i] = AGG_REAL_LOAD_PREFIX + i;
				virtualLoadAggregatorNames[i] = AGG_VIRTUAL_LOAD_PREFIX + i;
				finalVertexCounts[i] = FINAL_AGG_VERTEX_COUNT_PREFIX + i;
			}
		}
	}

	/**
	 * The core of the partitioning algorithm executed on the Master
	 * 
	 * @author Adnan
	 *
	 */
	public static class SuperPartitionerMasterCompute extends DefaultMasterCompute {
		protected LinkedList<Double> states;
		protected String[] loadAggregatorNames;
		protected int maxIterations;
		protected int numberOfPartitions;
		protected double convergenceThreshold;
		protected short repartition;
		protected int windowSize;
		protected double maxMinLoad;
		protected double maxNormLoad;
		protected double score;
		// Added by Adnan
		protected long totalMigrations;

		protected String[] realLoadAggregatorNames;
		protected String[] virtualLoadAggregatorNames;
		protected String[] vertexCountAggregatorNames;
		protected String[] finalVertexCounts;
		protected String[] outDegreeFrequency;

		protected long[] realEdgeLoads;
		protected long[] virtualEdgeLoads;
		protected long[] vertexLoads;
		protected double[] realAvgDegrees;
		protected double[] virtualAvgDegrees;

		protected double realEdgeMaxMinLoad;
		protected double realEdgeMaxNormLoad;
		protected double realEdgeBalanceJSD;

		protected double virtualEdgeMaxMinLoad;
		protected double virtualEdgeMaxNormLoad;
		protected double virtualEdgeBalanceJSD;

		protected double realMaxMinAvgDegree;
		protected double realMaxNormAvgDegree;
		protected double realAvgDegreeBalanceJSD;

		protected double virtualMaxMinAvgDegree;
		protected double virtualMaxNormAvgDegree;
		protected double virtualAvgDegreeBalanceJSD;

		protected double vertexMaxMinLoad;
		protected double vertexMaxNormLoad;
		protected double vertexBalanceJSD;

		// AEM: get the balance algorithm
		protected String balance_algorithm;
		//protected String initialization_class_name;

		// RR:
		protected String[] vertexCountAggregatorNamesSampling;

		/**
		 * if True, the statistics of the partition will be stored in a local file.
		 * (Please set the file name in saveXXX() methods)
		 */
		protected boolean isSaveStatsIntoFile;

		protected static boolean algorithmFinished = false;

		protected static int lastSuperStep = Integer.MAX_VALUE;

		/**
		 * Print partition Stat. at a given superstep
		 * 
		 * @param superstep superstep number
		 */
		protected void printStats(int superstep) {
			if (superstep > 2) {
				long migrations = ((LongWritable) getAggregatedValue(AGGREGATOR_MIGRATIONS)).get();
				long localEdges = ((LongWritable) getAggregatedValue(AGGREGATOR_LOCALS)).get();

				// Local edges
				// long realLocalEdges = ((LongWritable)
				// getAggregatedValue(AGG_REAL_LOCAL_EDGES)).get();
				// getContext().getCounter("current", "LE -
				// "+getSuperstep()).increment(realLocalEdges);

				switch (superstep % 2) {
				case 0:
					// System.out.println(((double) localEdges) / getTotalNumEdges() + " local
					// edges");
					long minLoad = Long.MAX_VALUE;
					long maxLoad = -Long.MAX_VALUE;
					for (int i = 0; i < numberOfPartitions + repartition; i++) {
						long load = ((LongWritable) getAggregatedValue(loadAggregatorNames[i])).get();
						if (load < minLoad) {
							minLoad = load;
						}
						if (load > maxLoad) {
							maxLoad = load;
						}
					}
					double expectedLoad = ((double) getTotalNumEdges()) / (numberOfPartitions + repartition);
					// System.out.println((((double) maxLoad) / minLoad) + " max-min unbalance");
					// System.out.println(((maxLoad) / expectedLoad) + " maximum normalized load");
					break;
				case 1:
					// System.out.println(migrations + " migrations");
					break;
				}
			}
		}

		/**
		 * Update the partition Stat.
		 * 
		 * @paramsuperstep
		 */
		protected void updateStats() {
			totalMigrations += ((LongWritable) getAggregatedValue(AGGREGATOR_MIGRATIONS)).get();
			long minLoad = Long.MAX_VALUE;
			long maxLoad = -Long.MAX_VALUE;
			int k = numberOfPartitions + repartition;
			double expectedLoad = ((double) getTotalNumEdges()) / (k);

			double sumEdges = 0, sumVertex = 0;
			for (int i = 0; i < k; i++) {
				long load = ((LongWritable) getAggregatedValue(loadAggregatorNames[i])).get();
				// long compute = ((LongWritable)
				// getAggregatedValue(vertexCountAggregatorNames[i])).get();
				if (load < minLoad) {
					minLoad = load;
				}
				if (load > maxLoad) {
					maxLoad = load;
				}

				double loadProb = ((double) load) / getTotalNumEdges();
				sumEdges += -loadProb * Math.log(loadProb);

				// double computeProb = ((double) compute)/ getTotalNumVertices();
				// sumVertex += -computeProb * Math.log(computeProb);
			}

			maxMinLoad = ((double) maxLoad) / minLoad;
			maxNormLoad = (maxLoad) / expectedLoad;
			score = ((DoubleWritable) getAggregatedValue(AGGREGATOR_STATE)).get();
		}

		/**
		 * check if the algorithm has converged
		 * 
		 * @param superstep the number of the current superstep
		 * @return
		 */
		protected boolean algorithmConverged(int superstep) {
			double newState = ((DoubleWritable) getAggregatedValue(AGGREGATOR_STATE)).get();
			boolean converged = false;
			if (superstep > sampling_ss_end + 3 + windowSize) {
				double best = Collections.max(states);
				double step = Math.abs(1 - newState / best);
				converged = step < convergenceThreshold;
				states.removeFirst();
			}
			states.addLast(newState);

			return converged;
		}

		/**
		 * Update the partition quality measures (evaluations measures)
		 */
		protected void updatePartitioningQuality() {
			int k = numberOfPartitions + repartition;

			long totalNumRealEdges = ((LongWritable) getAggregatedValue(TOTAL_DIRECTED_OUT_EDGES)).get();

			long realMinLoad = Long.MAX_VALUE;
			long realMaxLoad = -Long.MAX_VALUE;
			double realExpectedLoad = ((double) totalNumRealEdges) / (k);

			long virtualMinLoad = Long.MAX_VALUE;
			long virtualMaxLoad = -Long.MAX_VALUE;
			double virtualExpectedLoad = ((double) getTotalNumEdges()) / (k);

			double realMinAvgDegree = Double.MAX_VALUE;
			double realMaxAvgDegree = -Double.MAX_VALUE;
			double realExpectedAvgDegree = ((double) totalNumRealEdges) / getTotalNumVertices();

			double virtualMinAvgDegree = Double.MAX_VALUE;
			double virtualMaxAvgDegree = -Double.MAX_VALUE;
			double virtualExpectedAvgDegree = ((double) getTotalNumEdges()) / getTotalNumVertices();

			long vertexMinLoad = Long.MAX_VALUE;
			long vertexMaxLoad = -Long.MAX_VALUE;
			double vertexExpectedLoad = ((double) getTotalNumVertices()) / (k);

			realEdgeBalanceJSD = 0;
			virtualEdgeBalanceJSD = 0;
			realAvgDegreeBalanceJSD = 0;
			virtualAvgDegreeBalanceJSD = 0;
			vertexBalanceJSD = 0;

			double u = ((double) 1 / k), m;

			double avgrealnorm = 0;
			double avgvirnorm = 0;
			realEdgeLoads = new long[k];
			virtualEdgeLoads = new long[k];
			vertexLoads = new long[k];
			realAvgDegrees = new double[k];
			virtualAvgDegrees = new double[k];

			for (int i = 0; i < k; i++) {

				realEdgeLoads[i] = ((LongWritable) getAggregatedValue(realLoadAggregatorNames[i])).get();
				virtualEdgeLoads[i] = ((LongWritable) getAggregatedValue(virtualLoadAggregatorNames[i])).get();
				vertexLoads[i] = ((LongWritable) getAggregatedValue(finalVertexCounts[i])).get();

				realAvgDegrees[i] = ((double) realEdgeLoads[i] / vertexLoads[i]);
				virtualAvgDegrees[i] = ((double) virtualEdgeLoads[i] / vertexLoads[i]);

				avgrealnorm += realAvgDegrees[i];
				avgvirnorm += virtualAvgDegrees[i];
			}

			for (int i = 0; i < k; i++) {

				if (realEdgeLoads[i] < realMinLoad) {
					realMinLoad = realEdgeLoads[i];
				}
				if (realEdgeLoads[i] > realMaxLoad) {
					realMaxLoad = realEdgeLoads[i];
				}

				if (virtualEdgeLoads[i] < virtualMinLoad) {
					virtualMinLoad = virtualEdgeLoads[i];
				}
				if (virtualEdgeLoads[i] > virtualMaxLoad) {
					virtualMaxLoad = virtualEdgeLoads[i];
				}

				// avg
				if (realAvgDegrees[i] < realMinAvgDegree) {
					realMinAvgDegree = realAvgDegrees[i];
				}
				if (realAvgDegrees[i] > realMaxAvgDegree) {
					realMaxAvgDegree = realAvgDegrees[i];
				}

				if (virtualAvgDegrees[i] < virtualMinAvgDegree) {
					virtualMinAvgDegree = virtualAvgDegrees[i];
				}
				if (virtualAvgDegrees[i] > virtualMaxAvgDegree) {
					virtualMaxAvgDegree = virtualAvgDegrees[i];
				}

				if (vertexLoads[i] < vertexMinLoad) {
					vertexMinLoad = vertexLoads[i];
				}
				if (vertexLoads[i] > vertexMaxLoad) {
					vertexMaxLoad = vertexLoads[i];
				}

				/*
				 * System.out.println("partition " + i); System.out.println("realAvgDegree " +
				 * realAvgDegree); System.out.println("virtualAvgDegree " + virtualAvgDegree);
				 * System.out.println("realLoad " + realLoad); System.out.println("virtualLoad "
				 * + virtualLoad); System.out.println("vertexCount " + vertexCount);
				 */

				double realLoadProb = ((double) realEdgeLoads[i]) / totalNumRealEdges;
				m = (realLoadProb + u) / 2;
				realEdgeBalanceJSD += realLoadProb * Math.log(realLoadProb / m) + u * Math.log(u / m);

				double virtualLoadProb = ((double) virtualEdgeLoads[i]) / getTotalNumEdges();
				m = (virtualLoadProb + u) / 2;
				virtualEdgeBalanceJSD += virtualLoadProb * Math.log(virtualLoadProb / m) + u * Math.log(u / m);

				// avg
				double realAvgDegreeProb = (realAvgDegrees[i]) / (avgrealnorm);
				m = (realAvgDegreeProb + u) / 2;
				realAvgDegreeBalanceJSD += realAvgDegreeProb * Math.log(realAvgDegreeProb / m) + u * Math.log(u / m);

				double virtualAvgDegreeLoadProb = (virtualAvgDegrees[i]) / (avgvirnorm);
				m = (virtualAvgDegreeLoadProb + u) / 2;
				virtualAvgDegreeBalanceJSD += virtualAvgDegreeLoadProb * Math.log(virtualAvgDegreeLoadProb / m)
						+ u * Math.log(u / m);

				// JensenShannonDivergence
				double vertexCountProb = ((double) vertexLoads[i]) / getTotalNumVertices();
				m = (vertexCountProb + u) / 2;
				vertexBalanceJSD += vertexCountProb * Math.log(vertexCountProb / m) + u * Math.log(u / m);
			}

			realEdgeMaxNormLoad = (realMaxLoad) / realExpectedLoad;
			realEdgeMaxMinLoad = ((double) realMaxLoad) / realMinLoad;
			realEdgeBalanceJSD = realEdgeBalanceJSD / (2 * Math.log(2));

			virtualEdgeMaxNormLoad = (virtualMaxLoad) / virtualExpectedLoad;
			virtualEdgeMaxMinLoad = ((double) virtualMaxLoad) / virtualMinLoad;
			virtualEdgeBalanceJSD = virtualEdgeBalanceJSD / (2 * Math.log(2));

			// avg
			realMaxNormAvgDegree = (realMaxAvgDegree) / realExpectedAvgDegree;
			realMaxMinAvgDegree = (realMaxAvgDegree) / realMinAvgDegree;
			realAvgDegreeBalanceJSD = realAvgDegreeBalanceJSD / (2 * Math.log(2));

			virtualMaxNormAvgDegree = (virtualMaxAvgDegree) / virtualExpectedAvgDegree;
			virtualMaxMinAvgDegree = (virtualMaxAvgDegree) / virtualMinAvgDegree;
			virtualAvgDegreeBalanceJSD = virtualAvgDegreeBalanceJSD / (2 * Math.log(2));

			vertexMaxNormLoad = (vertexMaxLoad) / vertexExpectedLoad;
			vertexMaxMinLoad = ((double) vertexMaxLoad) / vertexMinLoad;
			vertexBalanceJSD = vertexBalanceJSD / (2 * Math.log(2));

			//System.out.println("llegamos hasta aqui");
			System.out.println(isSaveStatsIntoFile);
		}

		/**
		 * Initialize the algorithm parameters (number of partitions, etc.)
		 *
		 * Initialize the Aggregators (global/shared variables) needed to store the
		 * state of the partition (edge cuts, number of edges/vertices, etc.).
		 *
		 * @throws InstantiationException
		 * @throws IllegalAccessException
		 */
		protected void defineAdditionalAggregators() throws InstantiationException, IllegalAccessException {
			// DEFAULT_NUM_PARTITIONS = getConf().getMaxWorkers()*getConf().get();
			maxIterations = getContext().getConfiguration().getInt(MAX_ITERATIONS_LP, DEFAULT_MAX_ITERATIONS);

			numberOfPartitions = getContext().getConfiguration().getInt(NUM_PARTITIONS, DEFAULT_NUM_PARTITIONS);

			repartition = (short) getContext().getConfiguration().getInt(REPARTITION, DEFAULT_REPARTITION);

			isSaveStatsIntoFile = getContext().getConfiguration().getBoolean(SAVE_STATS, true);

			// Create aggregators for each partition
			realLoadAggregatorNames = new String[numberOfPartitions + repartition];
			virtualLoadAggregatorNames = new String[numberOfPartitions + repartition];
			vertexCountAggregatorNames = new String[numberOfPartitions + repartition];
			finalVertexCounts = new String[numberOfPartitions + repartition];

			for (int i = 0; i < numberOfPartitions + repartition; i++) {
				realLoadAggregatorNames[i] = AGG_REAL_LOAD_PREFIX + i;
				registerPersistentAggregator(realLoadAggregatorNames[i], LongSumAggregator.class);

				virtualLoadAggregatorNames[i] = AGG_VIRTUAL_LOAD_PREFIX + i;
				registerPersistentAggregator(virtualLoadAggregatorNames[i], LongSumAggregator.class);

				vertexCountAggregatorNames[i] = AGG_VERTEX_COUNT_PREFIX + i;
				registerPersistentAggregator(vertexCountAggregatorNames[i], LongSumAggregator.class);

				finalVertexCounts[i] = FINAL_AGG_VERTEX_COUNT_PREFIX + i;
				registerPersistentAggregator(finalVertexCounts[i], LongSumAggregator.class);
			}

			// Added by Adnan
			registerPersistentAggregator(AGG_REAL_UPPER_TOTAL_COMM_VOLUME, LongSumAggregator.class);
			registerPersistentAggregator(AGG_VIRTUAL_UPPER_TOTAL_COMM_VOLUME, LongSumAggregator.class);

			registerPersistentAggregator(TOTAL_DIRECTED_OUT_EDGES, LongSumAggregator.class);

			registerAggregator(AGG_REAL_TOTAL_COMM_VOLUME, LongSumAggregator.class);
			registerAggregator(AGG_VIRTUAL_TOTAL_COMM_VOLUME, LongSumAggregator.class);

			registerAggregator(AGG_REAL_EDGE_CUTS, LongSumAggregator.class);
			registerAggregator(AGG_VIRTUAL_EDGE_CUTS, LongSumAggregator.class);

			registerAggregator(AGG_REAL_LOCAL_EDGES, LongSumAggregator.class);
			registerAggregator(AGG_VIRTUAL_LOCALS, LongSumAggregator.class);

			if (INITIALIZATION_TYPE.contentEquals("BGRAP_init")) {
				outDegreeFrequency = new String[101];
				for (int i = 0; i < 101; i++) {
					outDegreeFrequency[i] = OUTDEGREE_FREQUENCY_COUNTER + i;
					registerPersistentAggregator(outDegreeFrequency[i], IntSumAggregator.class);

				}
			}
		}

		@Override
		public void initialize() throws InstantiationException, IllegalAccessException {
			// Set Logger level()
			logger.setLevel(Level.WARN);

			maxIterations = getContext().getConfiguration().getInt(MAX_ITERATIONS_LP, DEFAULT_MAX_ITERATIONS);

			// DEFAULT_NUM_PARTITIONS = getConf().getMaxWorkers()*getConf().get();

			numberOfPartitions = getContext().getConfiguration().getInt(NUM_PARTITIONS, DEFAULT_NUM_PARTITIONS);
			convergenceThreshold = getContext().getConfiguration().getFloat(CONVERGENCE_THRESHOLD,
					DEFAULT_CONVERGENCE_THRESHOLD);
			repartition = (short) getContext().getConfiguration().getInt(REPARTITION, DEFAULT_REPARTITION);
			windowSize = getContext().getConfiguration().getInt(WINDOW_SIZE, DEFAULT_WINDOW_SIZE);
			states = Lists.newLinkedList();
			// Create aggregators for each partition
			loadAggregatorNames = new String[numberOfPartitions + repartition];
			vertexCountAggregatorNames = new String[numberOfPartitions + repartition];
			vertexCountAggregatorNamesSampling = new String[numberOfPartitions + repartition]; // RR:

			for (int i = 0; i < numberOfPartitions + repartition; i++) {
				loadAggregatorNames[i] = AGG_EGDES_LOAD_PREFIX + i;
				registerPersistentAggregator(loadAggregatorNames[i], LongSumAggregator.class);
				registerAggregator(AGG_VERTEX_MIGRATION_DEMAND_PREFIX + i, LongSumAggregator.class);
				registerAggregator(AGG_MIGRATION_DEMAND_PREFIX + i, LongSumAggregator.class); // added by Hung

				vertexCountAggregatorNames[i] = AGG_VERTEX_COUNT_PREFIX + i;
				registerPersistentAggregator(vertexCountAggregatorNames[i], LongSumAggregator.class); // Hung

				// RR:
				vertexCountAggregatorNamesSampling[i] = AGG_VERTEX_COUNT_PREFIX + i + "_SAMPLING";
				registerAggregator(vertexCountAggregatorNamesSampling[i], LongSumAggregator.class); // Hung

			}

			registerAggregator(AGGREGATOR_STATE, DoubleSumAggregator.class); // RR: maybe float?
			registerAggregator(AGGREGATOR_LOCALS, LongSumAggregator.class);
			registerAggregator(AGGREGATOR_MIGRATIONS, LongSumAggregator.class); // RR: used for e and v, maybe create
																				// one for each and use int in V and
																				// long in E?

			// Added by Adnan
			registerAggregator(AGG_UPDATED_VERTICES, LongSumAggregator.class); // Hung
			registerAggregator(AGG_INITIALIZED_VERTICES, LongSumAggregator.class);
			registerAggregator(AGG_FIRST_LOADED_EDGES, LongSumAggregator.class);
			registerPersistentAggregator(AGG_UPPER_TOTAL_COMM_VOLUME, LongSumAggregator.class);
			registerPersistentAggregator(TOTAL_DIRECTED_OUT_EDGES, LongSumAggregator.class);
			registerAggregator(AGG_EDGE_CUTS, LongSumAggregator.class);
			registerPersistentAggregator(AGG_SAMPLING_CONVERGED, BooleanOverwriteAggregator.class);
			registerPersistentAggregator(AGG_NEED_MORE_SAMPLE, BooleanAndAggregator.class);
			

			setupBGRAPInitializationClass();
			setupBGRAPCoreClasses();

			defineAdditionalAggregators();

			formattedDate = getContext().getConfiguration().get(DATE_HANDLE, "None");
			if (formattedDate == "None") {
				LocalDateTime myDateObj = LocalDateTime.now();
				DateTimeFormatter myFormatObj = DateTimeFormatter.ofPattern("MMdd_HHmmss");
				formattedDate = myDateObj.format(myFormatObj);
			}
			System.out.println("Start the machine: "+NEEDS_MORE_SAMPLE);
		}


		protected void setupBGRAPInitializationClass() throws InstantiationException, IllegalAccessException {			
			// Added by Hung
			registerPersistentAggregator(AGG_CL_COEFFICIENT, HashMapAggregator.class);
			registerAggregator(FRIENDS_COUNTER, LongSumAggregator.class);

			// RR:
			INITIALIZATION_TYPE = getContext().getConfiguration().get(INITIALIZATION_TYPE_HANDLE,
					DEFAULT_SAMPLING_TYPE);
			SAVE_DD = getContext().getConfiguration().getBoolean(SAVE_DD_HANDLE, false); 

			registerAggregator(AGG_VERTICES, LongSumAggregator.class);
			registerPersistentAggregator(AGG_MAX_DEGREE, IntMaxAggregator.class);
			registerPersistentAggregator(AGG_SAMPLE, IntSumAggregator.class);
			registerAggregator(AGG_SAMPLE_SS, IntSumAggregator.class);
			registerAggregator(AGG_SAMPLE_SSR, IntSumAggregator.class);
			if (SAVE_DD || INITIALIZATION_TYPE.contentEquals("InitializeSampleHD")
					|| INITIALIZATION_TYPE.contentEquals("InitializeSampleGD"))
				registerPersistentAggregator(AGG_DEGREE_DIST, HashMapAggregator.class);
			
			NEEDS_MORE_SAMPLE = true;
			switch (INITIALIZATION_TYPE) {
			case "InitializeSampleRD":
				initializationClass = RD_SamplingBasedOnRankDegree.class;
				break;
			case "InitializeSampleHD":
				initializationClass = HD_SamplingBasedOnHighestDegree.class;
				break;
			case "InitializeSampleCC":
				initializationClass = CC_SamplingBasedOnClusteringCoefficient.class;
				break;
			case "InitializeSampleGD":
				initializationClass = GD_SamplingBasedOnGraphDistribution.class;
				break;
			case "BGRAP_init":
				NEEDS_MORE_SAMPLE = false;
				setAggregatedValue(AGG_NEED_MORE_SAMPLE, new BooleanWritable(false));
				sampling_ss_end = 2;
				initializationClass = BGRAP_HubVerticesInitializer.class;
				break;
			default:
				logger.warn("The initialization class is not set, the initialization computation "
						+ "class is set the default LP initilisation class "
						+ BGRAP_DefaultInitializer.class.getSimpleName());
				NEEDS_MORE_SAMPLE = false;
				setAggregatedValue(AGG_NEED_MORE_SAMPLE, new BooleanWritable(false));				
				sampling_ss_end = 2;
				initializationClass = BGRAP_DefaultInitializer.class;
				break;
			}
		}
		
		
		protected void setupBGRAPCoreClasses(){
			String parentClass;
			// AEM
			balance_algorithm = getContext().getConfiguration().get(BALANCE_ALGORITHM_HANDLE, BALANCE_ALGORITHM);
			switch (balance_algorithm) {
			case "bgrap.EDGE_BALANCE":
				cfp = BGRAP_EDGE_BALANCE.ComputeFirstPartition.class;
				cfm = BGRAP_EDGE_BALANCE.ComputeFirstUpdatePropagateStep.class;
				cm = BGRAP_EDGE_BALANCE.ComputeUpdatePropagatePartitions.class;
				cnp = BGRAP_EDGE_BALANCE.ComputeNewPartition.class;
				break;

			case "bgrap.VERTEX_BALANCE":
				parentClass = "BGRAP_VERTEX_BALANCE";
				cfp = BGRAP_VERTEX_BALANCE.ComputeFirstPartition.class;
				cfm = BGRAP_VERTEX_BALANCE.ComputeFirstUpdatePropagateStep.class;
				cm = BGRAP_VERTEX_BALANCE.ComputeUpdatePropagatePartitions.class;
				cnp = BGRAP_VERTEX_BALANCE.ComputeNewPartition.class;
				break;

			default:
				parentClass = "BGRAP_EDGE_BALANCE";
				cfp = BGRAP_EDGE_BALANCE.ComputeFirstPartition.class;
				cfm = BGRAP_EDGE_BALANCE.ComputeFirstUpdatePropagateStep.class;
				cm = BGRAP_EDGE_BALANCE.ComputeUpdatePropagatePartitions.class;
				cnp = BGRAP_EDGE_BALANCE.ComputeNewPartition.class;
				break;
			}

		}

		@SuppressWarnings({ "unchecked", "rawtypes" })
		@Override
		public void compute() {
			
			int superstep = (int) getSuperstep();
			
			if (!algorithmFinished) {
				switch (superstep) {
				case 0:
					System.out.println("MC0: ConverterPropagate");
					setComputation(ConverterPropagate.class);
					break;
				case 1:
					System.out.println("*MC1: ConverterUpdateEdges");
					setComputation(ConverterUpdateEdges.class);
					break;
				case 2:
					if (repartition != 0) {
						NEEDS_MORE_SAMPLE = false;
						setAggregatedValue(AGG_NEED_MORE_SAMPLE, new BooleanWritable(false));
						setComputation(Repartitioner.class);
					} else {
						sampling_messages_start = getContext().getCounter("Giraph Stats", "Aggregate sent messages")
								.getValue();
						
						if (BETA < getContext().getConfiguration().getInt(NUM_PARTITIONS, DEFAULT_NUM_PARTITIONS)) {
							System.out.println(
									"*WARNING: BETA is smaller than number of partitions wanted. Setting BETA to "
											+ numberOfPartitions + ".");
							BETA = numberOfPartitions;
						}
						// Set sampling execution
						System.out.println("*MC2: " + INITIALIZATION_TYPE);
						System.out.println("*SS" + superstep + ":SAMPLING BETA" + BETA);
						System.out.println(":SAMPLING SIGMA" + SIGMA);
						System.out.println(":SAMPLING TAU" + TAU);

						setComputation((Class<? extends AbstractComputation>) initializationClass);

					}
					break;
				default:// if superstep > 2: either continue sampling if needed or switch to BGRAP
					NEEDS_MORE_SAMPLE =((BooleanWritable) getAggregatedValue(AGG_NEED_MORE_SAMPLE)).get();
					System.out.println("NEEDS_MORE_SAMPLE: "+NEEDS_MORE_SAMPLE);
					if (NEEDS_MORE_SAMPLE) {// the sampling didn't finished
						int currentSampleSize = ((IntWritable) getAggregatedValue(AGG_SAMPLE)).get();
						
						sampling_ss_end = (short) superstep;
						if (currentSampleSize > BETA) {
							sampling_ss_extra += 1;
						}
						
						System.out.println("*MC" + superstep + ": " + initializationClass.getSimpleName() 
							+ " SampleSize=" + currentSampleSize);
						
						SAMPLING_CONVERGED = checkSamplingConvergence(currentSampleSize);
						System.out.println("SAMPLING_CONVERGED: "+SAMPLING_CONVERGED);
						setComputation((Class<? extends AbstractComputation>) initializationClass);
												
						
					} else {// the sampling is finished, switch to bgrap
						if (superstep == sampling_ss_end + 1) {
							sampling_messages_end = getContext().getCounter("Giraph Stats", "Aggregate sent messages")
									.getValue();
							getContext().getCounter("Partitioning Initialization", AGG_INITIALIZED_VERTICES)
									.increment(((LongWritable) getAggregatedValue(AGG_INITIALIZED_VERTICES)).get());
							System.out.println("*MC"+superstep+": CFP");
							setComputation((Class<? extends AbstractComputation>) cfp);
						} else if (superstep == sampling_ss_end + 2) {

							getContext().getCounter("Partitioning Initialization", AGG_UPDATED_VERTICES)
									.increment(((LongWritable) getAggregatedValue(AGG_UPDATED_VERTICES)).get()); // Hung
							System.out.println("*MC"+superstep+": CFM");
							setComputation((Class<? extends AbstractComputation>) cfm);
						} else {
							switch ((superstep - sampling_ss_end) % 2) {
							case 0:
								System.out.println("*MC"+superstep+": CM");
								setComputation((Class<? extends AbstractComputation>) cm);
								break;
							case 1:
								System.out.println("*MC"+superstep+": CNP");
								setComputation((Class<? extends AbstractComputation>) cnp);
								break;
							}
						}
					}
					break;
				}

				boolean hasConverged = false;
				if (superstep > sampling_ss_end + 3) {
					if ((superstep - sampling_ss_end) % 2 == 0) {
						hasConverged = algorithmConverged(superstep);
					}
				}
				printStats(superstep);
				updateStats();

				// LP iteration = 2 super-steps, LP process start after 3 super-steps
				if (hasConverged || superstep >= (maxIterations * 2 + sampling_ss_end)) {
					lp_ss_end = (short) superstep;
					lp_messages_end = getContext().getCounter("Giraph Stats", "Aggregate sent messages").getValue();

					System.out.println("Halting computation: " + hasConverged);
					algorithmFinished = true;
					lastSuperStep = superstep;
				}
			} else {
				if (superstep == lastSuperStep + 1) {
					System.out.println("*MC" + superstep + ": CGPS");
					setComputation(ComputeGraphPartitionStatistics.class);
				} else {
					System.out.println("Finish stats.");
					haltComputation();
					updatePartitioningQuality(); // RR: review datatypes
					saveTimersStats(); // RR: review datatypes
					// savePartitionStats();
					saveRealStats();
					// saveVirtualStats(file);
					saveDegreeDistribution(); // RR: check if it works with the hashmap
				}
			}

		}
		
		
		private boolean checkSamplingConvergence(int currentSampleSize) {
			System.out.println(SAMPLE_SIZE);
			if (SAMPLE_SIZE != currentSampleSize) {
				SAMPLE_CONVERGENCE_CYCLES = 0;
				SAMPLE_SIZE = currentSampleSize;
				setAggregatedValue(AGG_SAMPLING_CONVERGED, new BooleanWritable(false));
				return false;
			} else {
				SAMPLE_CONVERGENCE_CYCLES++;
				if (SAMPLE_CONVERGENCE_CYCLES == 3) {
					logger.warn("WARNING: Sampling has converged and cannot propagate more. "
						+ "switch to BGRAP.");
					//SAMPLING_CONVERGED = true;
					setAggregatedValue(AGG_SAMPLING_CONVERGED, new BooleanWritable(true));
					//NEEDS_MORE_SAMPLE = false;
					return true;
				}
				else return false;
			}
		}

		@SuppressWarnings("deprecation")
		protected void saveTimersStats() {
			long initializingTime = 0; // time to initialize nodes (+ time to make sure all partitions are initialized
										// + time to initialize sampling)
			long samplingTime = 0; // time to run RD cycles
			long firstLPIteration = 0;
			long LPTime = 0;
			long totalLPTime = 0;

			long totalSamplingSupersteps = 0; // RD cycle (request degree, receive degree, select sampled neighbors)
			long totalLPSupersteps = 0;
			float avgSampling = 0;
			float avgLP = 0;

			if (repartition != 0) {
				initializingTime = getContext().getCounter("Giraph Timers", "Superstep 2 Repartitioner (ms)")
						.getValue();
				firstLPIteration = getContext().getCounter("Giraph Timers", "Superstep 3 ComputeFirstPartition (ms)")
						.getValue()
						+ getContext().getCounter("Giraph Timers", "Superstep 4 ComputeFirstUpdatePropagateStep (ms)")
								.getValue();
				LPTime = getLPTime(5);
				totalLPTime = firstLPIteration + LPTime;
				totalLPSupersteps = getSuperstep() - 4;
				avgLP = (float) totalLPTime / (totalLPSupersteps / 2);
			} else {
				switch (INITIALIZATION_TYPE) {
				case "InitializeSampleRD":
					initializingTime = getSamplingInitTime();
					samplingTime = getSamplingTime(2);
					totalSamplingSupersteps = sampling_ss_end - sampling_ss_extra - 1;
					avgSampling = (float) samplingTime / (totalSamplingSupersteps / 3);
					break;

				case "InitializeSampleHD":
					initializingTime = getContext()
							.getCounter("Giraph Timers", "Superstep 2 " + initializationClass.getSimpleName() + " (ms)")
							.getValue() + getSamplingInitTime();
					samplingTime = getSamplingTime(3);
					totalSamplingSupersteps = sampling_ss_end - sampling_ss_extra - 2;
					avgSampling = (float) samplingTime / (totalSamplingSupersteps / 3);
					break;
				case "InitializeSampleCC":
					initializingTime = getContext()
							.getCounter("Giraph Timers", "Superstep 2 " + initializationClass.getSimpleName() + " (ms)")
							.getValue() + getSamplingInitTime();
					samplingTime = getSamplingTime(3);
					totalSamplingSupersteps = sampling_ss_end - sampling_ss_extra - 2;
					avgSampling = (float) samplingTime / (totalSamplingSupersteps / 3);
					break;

				case "InitializeSampleGD":
					initializingTime = getContext()
							.getCounter("Giraph Timers", "Superstep 2 " + initializationClass.getSimpleName() + " (ms)")
							.getValue() + getSamplingInitTime();
					samplingTime = getSamplingTime(3);
					totalSamplingSupersteps = sampling_ss_end - sampling_ss_extra - 2;
					avgSampling = (float) samplingTime / (totalSamplingSupersteps / 3);
					break;
				default:
					initializingTime = getContext()
							.getCounter("Giraph Timers", "Superstep 2 " + initializationClass.getSimpleName() + " (ms)")
							.getValue();
					break;
				}

				firstLPIteration = getContext().getCounter("Giraph Timers",
						"Superstep " + (sampling_ss_end + 1) + " ComputeFirstPartition (ms)").getValue()
						+ getContext()
								.getCounter("Giraph Timers",
										"Superstep " + (sampling_ss_end + 2) + " ComputeFirstUpdatePropagateStep (ms)")
								.getValue();
				LPTime = getLPTime(sampling_ss_end + 3);
				totalLPTime = firstLPIteration + LPTime;
				totalLPSupersteps = getSuperstep() - (sampling_ss_end + 2);
				avgLP = (float) totalLPTime / (totalLPSupersteps / 2);
			}

			getContext().getCounter(PARTITION_COUNTER_GROUP, "Total LP (ms)").increment((totalLPTime));
			getContext().getCounter(PARTITION_COUNTER_GROUP, MIGRATIONS_COUNTER).increment(totalMigrations);
			getContext().getCounter(PARTITION_COUNTER_GROUP, SCORE_COUNTER).increment((long) (1000 * score));
			long totalRealEdges = ((LongWritable) getAggregatedValue(TOTAL_DIRECTED_OUT_EDGES)).get();

			String date = new SimpleDateFormat("yyyy-MM-dd--hh:mm:ss").format(new Date());

			if (isSaveStatsIntoFile) {
				try {
					String filename = getContext().getConfiguration().get(SAVE_PATH, DEFAULT_SAVE_PATH)
							+ "/TimeComparison.csv";

					String header = "JobId;Date;Balance;Sampling;Graph;"
							+ "Total Vertices;Total Edges;Real Edges;Real Edges ratio;"
							+ "nbWorkers;nbPart;OUT DEGREE THRESHOLD;BETA;SIGMA;TAU;SAMPLING_ERROR;"
							+ "INITIALIZE TIME;SETUP TIME;INPUT TIME (not included in total);SHUTDOWN TIME;TOTAL GIRAPH TIMER;"
							+ "Initializing algorithm time;LP time;Avg. LP cycle time;Sampling time;Avg. Sampling cycle time;TOTAL TIME;"
							+ "Sampling start;Sampling end;Sampling cycles;Sampling extra;"
							+ "LP start;LP end;LP cycles;"
							+ "INITIAL MESSAGES;SAMPLING MESSAGES;LP MESSAGES;LP migration;SHUTDOWN MESSAGES;TOTAL MESSAGES;"
							+ "VIRTUAL_MEMORY_BYTES;PHYSICAL_MEMORY_BYTES\n";
							//+ "MAP_VIRTUAL_MEMORY_BYTES_MAX;MAP_PHYSICAL_MEMORY_BYTES_MAX\n";
					FileWriter file;
					if (!(new File(filename).exists())) {// create new file
						file = new FileWriter(filename, false);
						file.write(header);
					} else {// append existing file
						file = new FileWriter(filename, true);
					}

					// JOB INFO
					// header: JobId;Date;Balance;Sampling;Graph;
					file.write(getContext().getJobID() + DELIMITER);// JobId
					file.write(date + DELIMITER);// Date
					file.write(balance_algorithm + DELIMITER);// Balance
					file.write(initializationClass.getSimpleName() + DELIMITER);// Sampling
					file.write(getContext().getConfiguration().get(GRAPH_NAME_HANDLE, GRAPH_NAME) + DELIMITER);// Graph

					// GRAPH INFO
					// header: Total Vertices;Total Edges;Real Edges;Real Edges ratio;
					file.write(getTotalNumVertices() + DELIMITER);// Total Vertices
					file.write(getTotalNumEdges() + DELIMITER);// Total Edges
					file.write(totalRealEdges + DELIMITER);// Real Edges
					file.write(String.format(Locale.US,"%.3f", ((float) totalRealEdges) / getTotalNumEdges()) + DELIMITER);// Real
																													// Edges
																													// ratio

					// BGRAP PARAMETERS
					// header: nbWorkers;nbPart;OUT DEGREE THRESHOLD;BETA;SIGMA;TAU;SAMPLING_ERROR;
					file.write(getConf().getMaxWorkers() + ";");// nbWorkers
					file.write(numberOfPartitions + repartition + ";");// nbPart
					file.write(outDegreeThreshold + DELIMITER);// OUT DEGREE THRESHOLD
					file.write(BETA_P + DELIMITER);// BETA
					file.write(SIGMA_P + DELIMITER);// SIGMA
					file.write(TAU + DELIMITER);// TAU
					file.write(SAMPLING_CONVERGED + DELIMITER);// SAMPLING_ERROR

					// GIRAPH TIMERS COUNTER
					// header: INITIALIZE TIME;SETUP TIME;INPUT TIME (not included in total);
					// SHUTDOWN TIME;TOTAL GIRAPH TIMER;
					file.write(getContext().getCounter("Giraph Timers", "Initialize (ms)").getValue() + DELIMITER);// INITIALIZE
																													// TIME
					file.write(getContext().getCounter("Giraph Timers", "Setup (ms)").getValue() + DELIMITER);// SETUP
																												// TIME
					file.write(getContext().getCounter("Giraph Timers", "Input superstep (ms)").getValue() + DELIMITER);// INPUT
																														// TIME
																														// (not
																														// included
																														// in
																														// total)
					file.write(getContext().getCounter("Giraph Timers", "Shutdown (ms)").getValue() + DELIMITER);// SHUTDOWN
																													// TIME
					file.write(getContext().getCounter("Giraph Timers", "Total (ms)").getValue() + DELIMITER);// TOTAL
																												// GIRAPH
																												// TIMER

					// BGRAP TIME COUNTERS
					// header: Initializing algorithm time;LP time;Avg. LP cycle time;
					// Sampling time;Avg. Sampling cycle time;TOTAL TIME;
					file.write(initializingTime + DELIMITER);// Initializing algorithm time
					// file.write("1LP TIME"+DELIMITER+firstLPIteration +"\n");
					// file.write("LP TIME"+DELIMITER+LPTime +"\n");
					file.write(totalLPTime + DELIMITER);// LP time
					file.write(avgLP + DELIMITER);// Avg. LP cycle time
					file.write(samplingTime + DELIMITER);// Sampling time
					file.write(avgSampling + DELIMITER);// Avg. Sampling cycle time
					file.write((totalLPTime + samplingTime + initializingTime) + DELIMITER);// TOTAL TIME

					// SAMPLING SUPERSTEPS COUNTERS
					// header: Sampling start;Sampling end;Sampling cycles;Sampling extra;
					file.write(sampling_ss_start + DELIMITER);// Sampling start
					file.write(sampling_ss_end + DELIMITER);// Sampling end
					file.write((int) (totalSamplingSupersteps) / 3 + DELIMITER);// Sampling cycles
					file.write(sampling_ss_extra + DELIMITER);// Sampling extra

					// LABEL PROPAGATION/UPDATE SUPERSTEPS COUNTERS
					// header: LP start;LP end;LP cycles;
					file.write((sampling_ss_end + 1) + DELIMITER);// LP start
					file.write(lp_ss_end + DELIMITER);// LP end
					file.write((int) (totalLPSupersteps) / 2 + DELIMITER);// LP cycles

					// MESSAGES
					// header: INITIAL MESSAGES;SAMPLING MESSAGES;LP MESSAGES;
					// LP migration;SHUTDOWN MESSAGES;TOTAL MESSAGES;
					long totalMessages = getContext().getCounter("Giraph Stats", "Aggregate sent messages").getValue();
					file.write(sampling_messages_start + DELIMITER);// INITIAL MESSAGES
					file.write((sampling_messages_end - sampling_messages_start) + DELIMITER);// SAMPLING MESSAGES
					file.write((lp_messages_end - sampling_messages_end) + DELIMITER);// LP MESSAGES
					file.write(totalMigrations + DELIMITER);// LP migration
					file.write((totalMessages - lp_messages_end) + DELIMITER);// SHUTDOWN MESSAGES
					file.write(totalMessages + DELIMITER);// TOTAL MESSAGES

					// MEMORY
					// header: VIRTUAL_MEMORY_BYTES;PHYSICAL_MEMORY_BYTES;
					// MAP_VIRTUAL_MEMORY_BYTES_MAX;MAP_PHYSICAL_MEMORY_BYTES_MAX (hadoop 3.x)
					file.write(getContext().getCounter(Task.Counter.VIRTUAL_MEMORY_BYTES).getValue() + DELIMITER);// VIRTUAL_MEMORY_BYTES
					file.write(getContext().getCounter(Task.Counter.PHYSICAL_MEMORY_BYTES).getValue() + DELIMITER);// PHYSICAL_MEMORY_BYTES
					// Hdoop 3.x
					// file.write(getContext().getCounter(Task.Counter.MAP_VIRTUAL_MEMORY_BYTES_MAX).getValue() + DELIMITER);// MAP_VIRTUAL_MEMORY_BYTES_MAX
					// file.write(getContext().getCounter(Task.Counter.MAP_PHYSICAL_MEMORY_BYTES_MAX).getValue() + DELIMITER);// MAP_PHYSICAL_MEMORY_BYTES_MAX
					file.write("\n");

					// AE:
					file.flush();
					file.close();

				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		/**
		 * A method to store the partition stat. based on real edges
		 */
		protected void saveRealStats() {
			long totalRealEdges = ((LongWritable) getAggregatedValue(TOTAL_DIRECTED_OUT_EDGES)).get();

			getContext().getCounter(PARTITION_COUNTER_GROUP, "R Maxmin unbalance (x1000)")
					.increment((long) (1000 * realEdgeMaxMinLoad));
			getContext().getCounter(PARTITION_COUNTER_GROUP, "R MaxNorm unbalance (x1000)")
					.increment((long) (1000 * realEdgeMaxNormLoad));

			// Local edges
			long realLocalEdges = ((LongWritable) getAggregatedValue(AGG_REAL_LOCAL_EDGES)).get();
			getContext().getCounter(PARTITION_COUNTER_GROUP, "#real LE").increment(realLocalEdges);
			double realLocalEdgesPct = ((double) realLocalEdges) / totalRealEdges;
			getContext().getCounter(PARTITION_COUNTER_GROUP, "#real LE (%%)")
					.increment((long) (100 * realLocalEdgesPct));

			// Edge cuts
			long realEdgeCut = ((LongWritable) getAggregatedValue(AGG_REAL_EDGE_CUTS)).get();
			getContext().getCounter(PARTITION_COUNTER_GROUP, "#real EC").increment(realEdgeCut);
			double realEdgeCutPct = ((double) realEdgeCut) / totalRealEdges;
			getContext().getCounter(PARTITION_COUNTER_GROUP, "#real EC (%%)").increment((long) (100 * realEdgeCutPct));

			// Communication volume
			long realComVolume = ((LongWritable) getAggregatedValue(AGG_REAL_TOTAL_COMM_VOLUME)).get();
			long realComVolumeNormalisationFactor = ((LongWritable) getAggregatedValue(
					AGG_REAL_UPPER_TOTAL_COMM_VOLUME)).get();
			double realComVolumePct = ((double) realComVolume) / realComVolumeNormalisationFactor;
			getContext().getCounter(PARTITION_COUNTER_GROUP, "real CV (%%)").increment((long) (100 * realComVolumePct));

			getContext().getCounter(PARTITION_COUNTER_GROUP, Vertex_Balance_JSD_COUNTER)
					.increment((long) (1000 * vertexBalanceJSD));

			getContext().getCounter(PARTITION_COUNTER_GROUP, Edge_Balance_JSD_COUNTER)
					.increment((long) (1000 * realEdgeBalanceJSD));

			String date = new SimpleDateFormat("yyyy-MM-dd--hh:mm:ss").format(new Date());

			if (isSaveStatsIntoFile) {
				try {
					String filename = getContext().getConfiguration().get(SAVE_PATH, DEFAULT_SAVE_PATH)
							+ "/RealStatsComparisons.csv";

					String header = "JobId;Date;Balance;Sampling;Graph;"
							+ "Total Vertices;Total Edges;Real Edges;Real Edges ratio;"
							+ "nbWorkers;nbPart;OUT DEGREE THRESHOLD;BETA;SIGMA;TAU;SAMPLING_ERROR;"
							+ "RealEdge MaxNormLOAD;RealEdge MaxMinLOAD;RealEdge JSD;RealEdge KLU;"
							+ "Vertex MaxNormLOAD;Vertex MaxMinLOAD;Vertex JSD;Vertex KLU;"
							+ "Real AvgDegree MaxNormLOAD;Real AvgDegree MaxMinLOAD;Real AvgDegree JSD;Real AvgDegree KLU;"
							+ "Real EC PCT;Real CommVol PCT;Real LocalEdge PCT;Real EC;Real CommVol;Real CommVol NormF;Real LocalEdge;"
							+ "SCORE\n";
					FileWriter file;
					if (!(new File(filename).exists())) {// create new file
						file = new FileWriter(filename, false);
						file.write(header);
					} else {// append existing file
						file = new FileWriter(filename, true);
					}

					// JOB INFO
					// header: JobId;Date;Balance;Sampling;Graph;
					file.write(getContext().getJobID() + DELIMITER);// JobId
					file.write(date + DELIMITER);// Date
					file.write(balance_algorithm + DELIMITER);// Balance
					file.write(initializationClass.getSimpleName() + DELIMITER);// Sampling
					file.write(getContext().getConfiguration().get(GRAPH_NAME_HANDLE, GRAPH_NAME) + DELIMITER);// Graph

					// GRAPH INFO
					// header: Total Vertices;Total Edges;Real Edges;Real Edges ratio;
					file.write(getTotalNumVertices() + DELIMITER);// Total Vertices
					file.write(getTotalNumEdges() + DELIMITER);// Total Edges
					file.write(totalRealEdges + DELIMITER);// Real Edges
					file.write(String.format(Locale.US,"%.3f", ((float) totalRealEdges) / getTotalNumEdges()) + DELIMITER);// Real
																													// Edges
																													// ratio

					// BGRAP PARAMETERS
					// header: nbWorkers;nbPart;OUT DEGREE THRESHOLD;BETA;SIGMA;TAU;SAMPLING_ERROR;
					file.write(getConf().getMaxWorkers() + ";");// nbWorkers
					file.write(numberOfPartitions + repartition + ";");// nbPart
					file.write(outDegreeThreshold + DELIMITER);// OUT DEGREE THRESHOLD
					file.write(BETA_P + DELIMITER);// BETA
					file.write(SIGMA_P + DELIMITER);// SIGMA
					file.write(TAU + DELIMITER);// TAU
					file.write(SAMPLING_CONVERGED + DELIMITER);// SAMPLING_ERROR

					// REAL EDGES STATISTICS
					// header: RealEdge MaxNormLOAD;RealEdge MaxMinLOAD;RealEdge JSD;RealEdge KLU
					file.write(String.format(Locale.US, "%3f", realEdgeMaxNormLoad) + DELIMITER); // RealEdge MaxNormLOAD
					file.write(String.format(Locale.US, "%3f", realEdgeMaxMinLoad) + DELIMITER); // RealEdge MaxMinLOAD
					file.write(String.format(Locale.US, "%6f", realEdgeBalanceJSD) + DELIMITER); // RealEdge JSD
					file.write(String.format(Locale.US, "%6f", ArraysUtils.divergenceKLU(realEdgeLoads)) + DELIMITER); // RealEdge
																												// KLU

					// VERTEX STATISTICS
					// header: Vertex MaxNormLOAD;Vertex MaxMinLOAD;Vertex JSD;Vertex KLU
					file.write(String.format(Locale.US, "%3f", vertexMaxNormLoad) + DELIMITER); // Vertex MaxNormLOAD
					file.write(String.format(Locale.US, "%3f", vertexMaxMinLoad) + DELIMITER); // Vertex MaxMinLOAD
					file.write(String.format(Locale.US, "%6f", vertexBalanceJSD) + DELIMITER); // Vertex JSD
					file.write(String.format(Locale.US, "%6f", ArraysUtils.divergenceKLU(vertexLoads)) + DELIMITER); // "Vertex
																											// KLU

					// AVERAGE DEGREE STATISTICS
					// header: Real AvgDegree MaxNormLOAD;Real AvgDegree MaxMinLOAD;
					// Real AvgDegree JSD;Real AvgDegree KLU
					file.write(String.format(Locale.US, "%3f", realMaxNormAvgDegree) + DELIMITER); // Real AvgDegree MaxNormLOAD
					file.write(String.format(Locale.US, "%3f", realMaxMinAvgDegree) + DELIMITER); // Real AvgDegree MaxMinLOAD
					file.write(String.format(Locale.US, "%6f", realAvgDegreeBalanceJSD) + DELIMITER); // Real AvgDegreeB JSD
					file.write(String.format(Locale.US, "%6f", ArraysUtils.divergenceKLU(realAvgDegrees)) + DELIMITER); // Real
																												// AvgDegree
																												// KLU

					// EDGE-CUT & COMMUNICATION VOLUME
					// header: Real EC PCT;Real CommVol PCT;Real LocalEdge PCT;
					// Real EC;Real CommVol;Real CommVol NormF;Real LocalEdge;
					file.write(String.format(Locale.US, "%3f", realEdgeCutPct) + DELIMITER); // Real EC PCT
					file.write(String.format(Locale.US, "%3f", realComVolumePct) + DELIMITER); // Real CommVol PCT
					file.write(String.format(Locale.US, "%3f", realLocalEdgesPct) + DELIMITER); // Real LocalEdge PCT
					file.write(realEdgeCut + DELIMITER); // Real EC
					file.write(realComVolume + DELIMITER); // Real CommVol
					file.write(realComVolumeNormalisationFactor + DELIMITER); // Real CommVol NormF
					file.write(realLocalEdges + DELIMITER); // Real LocalEdge
					file.write(score + DELIMITER); // "Real Score
					file.write("\n");

					// AE:
					file.flush();
					file.close();

				} catch (IOException e) {
					e.printStackTrace();
				}
				;
			}
		}

		/**
		 * A method to store the partition statistics based on virtual edges
		 */
		protected void saveVirtualStats() {
			
			long totalRealEdges = ((LongWritable) getAggregatedValue(TOTAL_DIRECTED_OUT_EDGES)).get();

			getContext().getCounter(PARTITION_COUNTER_GROUP, "V Maxmin unbalance (x1000)")
					.increment((long) (1000 * virtualEdgeMaxMinLoad));
			getContext().getCounter(PARTITION_COUNTER_GROUP, "V MaxNorm unbalance (x1000)")
					.increment((long) (1000 * virtualEdgeMaxNormLoad));

			// Local edges
			long virtualLocalEdges = ((LongWritable) getAggregatedValue(AGG_VIRTUAL_LOCALS)).get();
			getContext().getCounter(PARTITION_COUNTER_GROUP, "#virtual local E").increment(virtualLocalEdges);
			double virtualLocalEdgesPct = ((double) virtualLocalEdges) / getTotalNumEdges();
			getContext().getCounter(PARTITION_COUNTER_GROUP, "virtual local E (%%)")
					.increment((long) (100 * virtualLocalEdgesPct));

			// Edge cuts
			long virtualEdgeCut = ((LongWritable) getAggregatedValue(AGG_VIRTUAL_EDGE_CUTS)).get();
			getContext().getCounter(PARTITION_COUNTER_GROUP, "#virtrual EC").increment(virtualEdgeCut);
			double virtualEdgeCutPct = ((double) virtualEdgeCut) / getTotalNumEdges();
			getContext().getCounter(PARTITION_COUNTER_GROUP, "virtrual EC (%%)")
					.increment((long) (100 * virtualEdgeCutPct));

			// Communication volume
			long virtualComVolume = ((LongWritable) getAggregatedValue(AGG_VIRTUAL_TOTAL_COMM_VOLUME)).get();
			long virtualComVolumeNormalisationFactor = ((LongWritable) getAggregatedValue(
					AGG_VIRTUAL_UPPER_TOTAL_COMM_VOLUME)).get();
			double virtualComVolumePct = ((double) virtualComVolume) / virtualComVolumeNormalisationFactor;
			getContext().getCounter(PARTITION_COUNTER_GROUP, "virtrual CV (%%)")
					.increment((long) (100 * virtualComVolumePct));

			getContext().getCounter(PARTITION_COUNTER_GROUP, Vertex_Balance_JSD_COUNTER)
					.increment((long) (1000 * vertexBalanceJSD));

			getContext().getCounter(PARTITION_COUNTER_GROUP, Edge_Balance_JSD_COUNTER)
					.increment((long) (1000 * virtualEdgeBalanceJSD));

			String date = new SimpleDateFormat("yyyy-MM-dd--hh:mm:ss").format(new Date());

			if (isSaveStatsIntoFile) {
				try {
					String filename = getContext().getConfiguration().get(SAVE_PATH, DEFAULT_SAVE_PATH)
							+ "/VirtualStatsComparisons.csv";

					String header = "JobId;Date;Balance;Sampling;Graph;"
							+ "VirtualEdge MaxNormLOAD;VirtualEdge MaxMinLOAD;VirtualEdge JSD;VirtualEdge KLU;"
							+ "Vertex MaxNormLOAD;Vertex MaxMinLOAD;Vertex JSD;Vertex KLU;"
							+ "Virtual AvgDegree MaxNormLOAD;Virtual AvgDegree MaxMinLOAD;Virtual AvgDegree JSD;Virtual AvgDegree KLU;"
							+ "Virtual EC PCT;Virtual CommVol PCT;Virtual LocalEdge PCT;Virtual EC;Virtual CommVol;Virtual CommVol NormF;Virtual LocalEdge\n";
					FileWriter file;
					if (!(new File(filename).exists())) {// create new file
						file = new FileWriter(filename, false);
						file.write(header);
					} else {// append existing file
						file = new FileWriter(filename, true);
					}

					// JOB INFO
					// header: JobId;Date;Balance;Sampling;Graph;
					file.write(getContext().getJobID() + DELIMITER);// JobId
					file.write(date + DELIMITER);// Date
					file.write(balance_algorithm + DELIMITER);// Balance
					file.write(initializationClass.getSimpleName() + DELIMITER);// Sampling
					file.write(getContext().getConfiguration().get(GRAPH_NAME_HANDLE, GRAPH_NAME) + DELIMITER);// Graph

					// EDGE STATISTICS
					// header: VirtualEdge MaxNormLOAD;VirtualEdge MaxMinLOAD;VirtualEdge
					// JSD;VirtualEdge KLU
					file.write(String.format(Locale.US, "%3f", virtualEdgeMaxNormLoad) + DELIMITER); //
					file.write(String.format(Locale.US, "%3f", virtualEdgeMaxMinLoad) + DELIMITER); //
					file.write(String.format(Locale.US, "%6f", virtualEdgeBalanceJSD) + DELIMITER); //
					file.write(String.format(Locale.US, "%6f", ArraysUtils.divergenceKLU(virtualEdgeLoads)) + DELIMITER); //

					// VERTEX STATISTICS
					// header: Vertex MaxNormLOAD;Vertex MaxMinLOAD;Vertex JSD;Vertex KLU
					file.write(String.format(Locale.US, "%3f", vertexMaxNormLoad) + DELIMITER); //
					file.write(String.format(Locale.US, "%3f", vertexMaxMinLoad) + DELIMITER); //
					file.write(String.format(Locale.US, "%6f", vertexBalanceJSD) + DELIMITER); //
					file.write(String.format(Locale.US, "%6f", ArraysUtils.divergenceKLU(vertexLoads)) + DELIMITER); //

					// AVERAGE DEGREE STATISTICS
					// header: Virtual AvgDegree MaxNormLOAD;Virtual AvgDegree MaxMinLOAD;
					// Virtual AvgDegree JSD;Virtual AvgDegree KLU
					file.write(String.format(Locale.US, "%3f", virtualMaxNormAvgDegree) + DELIMITER); //
					file.write(String.format(Locale.US, "%3f", virtualMaxMinAvgDegree) + DELIMITER); //
					file.write(String.format(Locale.US, "%6f", virtualAvgDegreeBalanceJSD) + DELIMITER); //
					file.write(ArraysUtils.divergenceKLU(virtualAvgDegrees) + DELIMITER); //

					// EDGE-CUT COMMUNICATION VOLUME
					// header: Virtual EC PCT;Virtual CommVol PCT;Virtual LocalEdge PCT;
					// Virtual EC;Virtual CommVol;Virtual CommVol NormF;Virtual LocalEdge
					file.write(String.format(Locale.US, "%3f", virtualEdgeCutPct) + DELIMITER); //
					file.write(String.format(Locale.US, "%3f", virtualComVolumePct) + DELIMITER); //
					file.write(String.format(Locale.US, "%3f", virtualLocalEdgesPct) + DELIMITER); //
					file.write(virtualEdgeCut + DELIMITER); //
					file.write(virtualComVolume + DELIMITER); //
					file.write(virtualLocalEdges + DELIMITER); //
					file.write(virtualComVolumeNormalisationFactor + DELIMITER); //
					file.write("\n");

					// AE:
					file.flush();
					file.close();

				} catch (IOException e) {
					e.printStackTrace();
				}
				;
			}
		}

		/**
		 * A method to store additional statistics.
		 */
		protected void savePartitionStats() {
			int k = numberOfPartitions + repartition;
			long vertices = 0, edges = 0, vedges = 0;
			String delimiter_temp = "_";
			String str1 = "", str2 = "", str3 = "";
			for (int i = 0; i < k; i++) {
				str1 += realEdgeLoads[i] + delimiter_temp;
				edges += realEdgeLoads[i];
				str2 += virtualEdgeLoads[i] + delimiter_temp;
				vedges += virtualEdgeLoads[i];
				str3 += vertexLoads[i] + delimiter_temp;
				vertices += vertexLoads[i];
			}

			try {

				String date = new SimpleDateFormat("yyyy-MM-dd--hh:mm:ss").format(new Date());
				String filename = getContext().getConfiguration().get(SAVE_PATH, DEFAULT_SAVE_PATH)
						+ "/VirtualStatsComparisons.csv";

				FileWriter file = new FileWriter(filename, true);

				file.write("JOB ID" + DELIMITER + getContext().getJobID() + "\n");
				file.write("JOB NAME" + DELIMITER + getContext().getJobName() + "\n");
				file.write("WORKERS" + DELIMITER + getConf().getMaxWorkers() + "\n");
				file.write("BGRAP ALGORITHM" + DELIMITER + balance_algorithm + "\n");
				file.write("INITILIZATION ALGORITHM" + DELIMITER + initializationClass.getSimpleName() + "\n");
				file.write("GRAPH" + DELIMITER + getContext().getConfiguration().get(GRAPH_NAME_HANDLE, GRAPH_NAME)
						+ "\n");
				file.write("VERTICES" + DELIMITER + vertices + "\n");
				file.write("REAL EDGES" + DELIMITER + edges + "\n");
				file.write("VIRTUAL EDGES" + DELIMITER + vedges + "\n");
				file.write("PARITIONS" + DELIMITER + k + "\n");
				file.write("REAL LOADS" + DELIMITER + str1 + "\n");
				file.write("VIRTUAL LOADS" + DELIMITER + str2 + "\n");
				file.write("VIRTUAL COUNTS" + DELIMITER + str3 + "\n");
				file.write("REAL KLU" + DELIMITER + (float) ArraysUtils.divergenceKLU(realEdgeLoads) + "\n");
				file.write("VIRTUAL KLU" + DELIMITER + (float) ArraysUtils.divergenceKLU(virtualEdgeLoads) + "\n");
				file.write("VIRTUAL COUNT KLU" + DELIMITER + (float) ArraysUtils.divergenceKLU(vertexLoads) + "\n");
				file.write("Real AvgDegree KLU" + DELIMITER + (float) ArraysUtils.divergenceKLU(realAvgDegrees) + "\n");
				file.write(
						"VIRTUAL AVGD KLU" + DELIMITER + (float) ArraysUtils.divergenceKLU(virtualAvgDegrees) + "\n");
				file.write("\n");

				// AE:
				file.flush();
				file.close();

			} catch (IOException e) {
				e.printStackTrace();
			}
			;
		}

		protected long getSamplingInitTime() {
			long SITime = 0;
			for (int i = sampling_ss_end - sampling_ss_extra - 1; i <= sampling_ss_end; i++) {
				SITime += getContext()
						.getCounter("Giraph Timers", "Superstep " + i + " " + initializationClass.getSimpleName() + " (ms)")
						.getValue();
			}
			return SITime;
		}

		protected long getSamplingTime(int samplingStart) {
			long STime = 0;
			for (int i = samplingStart; i < sampling_ss_end - sampling_ss_extra - 1; i++) {
				STime += getContext()
						.getCounter("Giraph Timers", "Superstep " + i + " " + initializationClass.getSimpleName() + " (ms)")
						.getValue();
			}
			return STime;
		}

		protected long getLPTime(int LPStart) {
			long LPTime = 0;
			for (int i = LPStart; i < getSuperstep() - 1; i += 2) {
				LPTime += getContext().getCounter("Giraph Timers", "Superstep " + i + " ComputeNewPartition (ms)")
						.getValue()
						+ getContext().getCounter("Giraph Timers",
								"Superstep " + (i + 1) + " ComputeUpdatePropagatePartitions (ms)").getValue();
			}
			return LPTime;
		}

		protected void saveDegreeDistribution() {
			if (SAVE_DD) {
				try {
					String graphName = getContext().getConfiguration().get(GRAPH_NAME_HANDLE, GRAPH_NAME);
					String filename = getContext().getConfiguration().get(SAVE_PATH, DEFAULT_SAVE_PATH) + graphName
							+ "-GDD.csv";
					FileWriter file = new FileWriter(filename, true);

					for (Entry<Writable, Writable> entry : degreeDist.entrySet()) {
						file.write(((IntWritable) entry.getKey()).get() + DELIMITER
								+ ((IntWritable) entry.getValue()).get() + "\n");
					}

					// AE:
					file.flush();
					file.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

}
