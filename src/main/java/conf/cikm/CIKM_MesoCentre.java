package conf.cikm;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.io.formats.GiraphFileInputFormat;
import org.apache.giraph.job.GiraphJob;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;

import giraph.lri.lahdak.bgrap.BGRAP_Partitionning;
import giraph.lri.lahdak.bgrap.PartitionQualityComputation;
import giraph.lri.lahdak.bgrap.utils.BGRAP_EdgeValue;
import giraph.lri.lahdak.bgrap.utils.ShortBooleanHashMapsEdgesInOut_IntVertexID;
import giraph.lri.lahdak.bgrap.utils.BGRAP_VertexValue;
import giraph.lri.lahdak.data.format.bgrap.BGRAP_IntVertexInputFormat;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * implemented with Giraph 1.3.0 and Hadoop 3.2.2
 * 
 * @author adnan
 *
 */
public class CIKM_MesoCentre extends MesoCentreClusterConfiguration {
	private String giraphOutputRep = "/bgrap",
			// giraphVertexInputRep = hdfsNode+"/giraph_data/input/VertexFormat", // doesn't
			// work : Giraph bug 904 : https://issues.apache.org/jira/browse/GIRAPH-904
			giraphVertexInputRep = "/input";
	// giraphEdgeInputRep = hdfsNode+"/giraph_data/input/edge_format";

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new CIKM_MesoCentre(), data);
	}

	private int k;
	boolean done = false;
	private float kappa;

	String[] balanceAlgorithms = new String[] { 
			"bgrap.VERTEX_BALANCE", 
			"bgrap.EDGE_BALANCE" };
	String[] initAlgorithms = new String[] { 
			//"BGRAP_init",
			//"InitializeSampleGD",
			"InitializeSampleHD",
			"InitializeSampleRD",
			//"InitializeSampleCC",
	};

	static String[] data = new String[] {
			//"BerkeleyStanf", 
			//"WikiTalk",
			"Flixster",
			//"DelaunaySC",
			//"Pokec",
			//"LastFM",
			//"LiveJournal",
			//"Orkut",
			//"sk-2005"
			//"Graph500",
			//"Twitter"
	};

	@Override
	public int run(String[] argDataNames) {
		String filename, localOutputFolderleName = System.getenv("HOME") + "/PaperResult/CIKM-results";
		File f = new File(localOutputFolderleName);

		if (!f.exists())
			f.mkdirs();

		if (argDataNames.length == 0) {
			argDataNames = data.clone();
		}

		setWorkersNumber(1);
		setCoresNumber(64);
		setMaxMemoryByContainer(256 * 1024);
		/*
		 * The required MAP capability is more than the supported max container
		 * capability in the cluster. Killing the Job. mapResourceRequest: <memory:8192,
		 * vCores:4> maxContainerCapability:<memory:7680, vCores:4>
		 */
		// setConfigFromFiles();

		String hdfsOutputDirectory = getHdfsNode() + giraphOutputRep;
		// String outputPath = "./data/output/WikiTalk_graph_out";
		// int j, d, dStart = 0, maxK = 2, maxRun = 1;
		//graph 500 
		//int j, d, maxK = 64, maxRun = 10, runStart = 1, kStart = 2, dStart = 0, bAlgoStart = 1, initAlgoStart = 0;
		//Twitter
		int j, d, maxK = 64, maxRun = 10, runStart = 1, kStart = 2, dStart = 0, bAlgoStart = 0, initAlgoStart = 0;
		//problem convergence flixter on HD sampling, it's still loop even after 4 cycles
		kappa = 0.5f;
		
		for (int initIndex = initAlgoStart; initIndex < initAlgorithms.length; initIndex++) {
			for (j = bAlgoStart; j < balanceAlgorithms.length; j++) {
				for (d = dStart; d < argDataNames.length; d++) {
					for (k = kStart; k <= maxK; k = k * 2) {
						boolean flag = false;
						filename = argDataNames[d];
						System.out.println(balanceAlgorithms[j] + ";" + initAlgorithms[initIndex] + ";" + filename);
						System.out.println("k: " + k);
						for (int run = runStart; run <= maxRun; run++) {
							boolean restart=true;
							try {
								updateRessourcesForGiraphOnYarn("localhost", "hdfs://localhost:9099", getMaxMemoryByContainer(), getCoresNumber());
								
								System.out.print("run: " + run + " --- " + (new Date()).toString());

								GiraphJob partitioningJob = new GiraphJob(initConfig(filename, balanceAlgorithms[j]),
										balanceAlgorithms[j] + ";" + initAlgorithms[initIndex] + ";" + filename);

								if (getHadoopDataFileSystem().exists(new Path(hdfsOutputDirectory))) {
									getHadoopDataFileSystem().delete(new Path(hdfsOutputDirectory), true);
								}

								FileOutputFormat.setOutputPath(partitioningJob.getInternalJob(),
										new Path(hdfsOutputDirectory));
								partitioningJob.getConfiguration().setInt(BGRAP_Partitionning.NUM_PARTITIONS, k);
								if (!(filename.startsWith("R") || filename.startsWith("Y")))
									partitioningJob.getConfiguration().setBoolean(BGRAP_Partitionning.GRAPH_DIRECTED,
											true);
								partitioningJob.getConfiguration().setFloat(BGRAP_Partitionning.KAPPA_HANDLE, kappa);
								partitioningJob.getConfiguration().setFloat(BGRAP_Partitionning.SIGMA_HANDLE, 0.1f);
								partitioningJob.getConfiguration().setBoolean(BGRAP_Partitionning.SAVE_STATS, true);
								partitioningJob.getConfiguration().set(BGRAP_Partitionning.SAVE_PATH,
										localOutputFolderleName);
								partitioningJob.getConfiguration().set(BGRAP_Partitionning.GRAPH_NAME_HANDLE, filename);
								partitioningJob.getConfiguration().set(BGRAP_Partitionning.INITIALIZATION_TYPE_HANDLE,
										initAlgorithms[initIndex]);
								partitioningJob.getConfiguration().set(BGRAP_Partitionning.BALANCE_ALGORITHM_HANDLE,
										balanceAlgorithms[j]);
								partitioningJob.getConfiguration().setInt(BGRAP_Partitionning.MAX_ITERATIONS_LP, 200);

								configureGiraph(partitioningJob.getConfiguration());
								org.apache.hadoop.mapreduce.counters.Limits.init(getConf());

								flag = partitioningJob.run(true);

								if (flag) {
									// 10 sec until mapred clean
									System.out.print("...");
									TimeUnit.SECONDS.sleep(3);
									System.out.println("finish");
									
									restart = false;
								}
							} catch (IOException | InterruptedException | ClassNotFoundException e) {
								//e.printStackTrace();
								if(restart){
									System.out.print("[ERROR] in run (" + run + "), ");
									String cmd = "stop-dfs.sh;start-dfs.sh;";
									run--;
									try {
										System.out.print("Try to Restart HADOOP CLUSTER,");
										TimeUnit.SECONDS.sleep(10);
										Runtime rt = Runtime.getRuntime();
										Process pr = rt.exec(cmd);
										pr.waitFor();
										BufferedReader buf = new BufferedReader(new InputStreamReader(pr.getInputStream()));
										String line = "";
										while ((line = buf.readLine()) != null) {
											System.out.println(line);
										}
									} catch (IOException | InterruptedException e1) {
										// TODO Auto-generated catch block
										// e1.printStackTrace();
									}
									System.out.println("almost done!");
								}
							}
						} // end of run loop
						runStart = 1;
					} // end of k loop
					kStart = 2;
				} // end of data loop
				dStart = 0;
			} // end of algoInit loop
			bAlgoStart = 0;
		} // end of bgrap loop
		initAlgoStart = 0;
		return 0;

	}

	// InternalVertexRunner.runWithInMemoryOutput
	protected GiraphConfiguration initConfig(String filename, String algo) throws IOException {

		GiraphConfiguration giraphConf = new GiraphConfiguration(getConf());

		String inputPath = getHdfsNode() + giraphVertexInputRep + "/" + filename;

		if (getHadoopDataFileSystem().exists(new Path(inputPath + ".vertex"))) {
			inputPath += ".vertex";
			GiraphFileInputFormat.addVertexInputPath(giraphConf, new Path(inputPath));
		} else {
			/*
			 * for (int i = 0; i <= 10; i++)
			 * GiraphFileInputFormat.addVertexInputPath(giraphConf, new Path(inputPath +
			 * "Twitter_split_" + i + ".vertex"));
			 */
			GiraphFileInputFormat.addVertexInputPath(giraphConf, new Path(inputPath));
		}

		GiraphConfiguration.VERTEX_INPUT_FORMAT_CLASS.set(giraphConf, BGRAP_IntVertexInputFormat.class);
		GiraphConfiguration.VERTEX_OUTPUT_FORMAT_CLASS.set(giraphConf,
				PartitionQualityComputation.IntIdEmptyOutputFormat.class);
		giraphConf.setInt("bgrap.weight", 2);
		
		
		GiraphConfiguration.VERTEX_VALUE_CLASS.set(giraphConf, BGRAP_VertexValue.class);
		GiraphConfiguration.EDGE_VALUE_CLASS.set(giraphConf, BGRAP_EdgeValue.class);
		// this class automatically create missed undirected edges and cause the lose of
		// bi-direction information
		// GiraphConfiguration.VERTEX_EDGES_CLASS.set(giraphConf,
		// OpenHashMapEdgesInOut.class);
		GiraphConfiguration.VERTEX_EDGES_CLASS.set(giraphConf, ShortBooleanHashMapsEdgesInOut_IntVertexID.class);
		//// must used in this way
		GiraphConfiguration.INPUT_VERTEX_EDGES_CLASS.set(giraphConf, ShortBooleanHashMapsEdgesInOut_IntVertexID.class);

		giraphConf.setLocalTestMode(true);
		//GiraphConfiguration.LOCAL_TEST_MODE.set(giraphConf, true);
		// giraphConf.setCheckpointFrequency(100);
		giraphConf.setWorkerConfiguration(1, getWorkersNumber(), 100);
		GiraphConfiguration.SPLIT_MASTER_WORKER.set(giraphConf, false);
		GiraphConfiguration.IS_PURE_YARN_JOB.set(giraphConf, false);
		//GiraphConfiguration.USE_OUT_OF_CORE_GRAPH.set(giraphConf, true);
		//GiraphConfiguration.OUT_OF_CORE_DATA_ACCESSOR.set(giraphConf, true);
		
		//giraphConf.setBoolean("giraph.useOutOfCoreMessage",  true);
		//giraphConf.setBoolean("giraph.useOutOfCoreGraph",  true);
		//giraphConf.setBoolean("giraph.useBigDataIOForMessages",  true);
		//giraphConf.setBoolean("giraph.useMessageSizeEncoding",  true);
		
		// getConf().setInt("mapreduce.shuffle.max.threads", getCoresNumber())
		// getConf().setInt("mapreduce.map.cpu.vcores", 8);//

		// InMemoryVertexOutputFormat.initializeOutputGraph(giraphConf);

		GiraphConfiguration.MASTER_COMPUTE_CLASS.set(giraphConf,
				BGRAP_Partitionning.SuperPartitionerMasterCompute.class);
		GiraphConfiguration.COMPUTATION_CLASS.set(giraphConf, BGRAP_Partitionning.ConverterPropagate.class);

		// FacebookConfiguration fb = new FacebookConfiguration();
		// FacebookConfiguration.MAPPER_CORES.set(giraphConf, 8);
		// fb.configure(giraphConf);

		return giraphConf;
	}
}
