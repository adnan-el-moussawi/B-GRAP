package conf.cikm;

import static org.apache.hadoop.mapreduce.MRJobConfig.COUNTERS_MAX_KEY;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.URI;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;

/**
 * This class allows to create an empty GiraphJob with the configuration of the
 * Hadoop cluster
 */
public class GiraphClusterConfiguration implements Tool {
	private Configuration conf = new Configuration();
	/**
	 * the path of the HADOOP_HOME configuration folder
	 */
	static final String confPath = System.getenv("HADOOP_CONF_DIR");
	/**
	 * HDFS Node address
	 */
	private String hdfsNode;
	/**
	 * Hadoop master address
	 */
	private String masterIp;
	private FileSystem hadoopDataFileSystem;

	private static final int MAPREDUCE_TASK_TIMEOUT = 3600 * 1000;//ms
	
	/**
	 * the maximum memory to use (by default  Gigabytes)
	 */
	private int maxMemoryByContainer = 4*1024;// 4 GigaBytes
	private int coresNumber=4;
	private int workersNumber=1;
	
	/**
	 * the percent of memory (maxMemoryByContainer) to use by 
	 * "mapred.reduce.child.java.opts and "mapred.map.child.java.opts
	 */
	private float memory_percent_child=0.8f; // the percent of memory (maxMemoryByContainer) to use
	private float memory_percent_parent=1; // 100%

	public void updateRessourcesForGiraphOnYarn(String master, String hdfs, int memory, int cores) throws IOException {
		setMasterIp(master);
		setHdfsNode(hdfs);
		setMaxMemoryByContainer(memory);
		setCoresNumber(cores);
		configureHDFS();
		configureMAPRED();
		configureYARN();
	}

	/**
	 * set the configuration of the hadoop cluster
	 * 
	 * @throws IOException
	 */
	public void configureHDFS() throws IOException {
		// configure manually
		getConf().set("hadoop.job.ugi", "adnanmoe");
		getConf().set("dfs.web.ugi", "adnanmoe");
		// let Hadoop create its own tmp directories
		// getConf().set("hadoop.tmp.dir", System.getenv("HOME") +
		// "/AppHadoop/tmp/"
		// +System.getenv("USER") + "_" + System.getenv("HOSTNAME"));
		getConf().set("dfs.permissions", "false");
		getConf().set("dfs.namenode.name.dir", System.getenv("HOME") + "/AppHadoop/data_hadoop3/namenode");
		getConf().set("dfs.datanode.data.dir", System.getenv("HOME") + "/AppHadoop/data_hadoop3/datanode");
		//getConf().set("fs.default.name", hdfsNode);// for hadoop 2fs.defaultFS
		getConf().set("fs.defaultFS", hdfsNode);// for hadoop 1, 3
		
		// to get access to HDFS system
		conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

		setHadoopDataFileSystem(FileSystem.get(URI.create(getHdfsNode()), conf));
	}
	
	/**
	 * set the configuration of the hadoop cluster
	 * 
	 * @throws IOException
	 */
	public void configureMAPRED() throws IOException {
		// mapred.xml
		getConf().set("mapreduce.jobhistory.address", masterIp + ":20020");
		getConf().set("mapred.job.tracker", masterIp + ":8023");
		// to avoid java.lang.OutOfMemoryError exception (Java heap space)
		// -Xms256m -Xmx8g
		// getConf().set("mapred.child.java.opts", "-Xmx12288m");
		// getConf().set("mapred.child.java.opts", "-Xmx" + (int) ((memory_percent_parent) * getMaxMemoryByContainer()) + "m");
		
		//hadoop 1.x
		/*
		getConf().set("mapred.map.child.java.opts", "-Xmx" + (int) (memory_percent_child * getMaxMemoryByContainer()) + "m");
		getConf().set("mapred.reduce.child.java.opts", "-Xmx" + (int) (memory_percent_child * getMaxMemoryByContainer()) + "m");
		getConf().setInt("mapred.job.map.memory.mb", getMaxMemoryByContainer());
		getConf().setInt("mapred.job.reduce.memory.mb", getMaxMemoryByContainer());
		*/
		
		//hadoop 3.x
		getConf().set("mapreduce.map.java.opts", "-Xmx" + (int) (memory_percent_child * getMaxMemoryByContainer()) + "m");
		getConf().set("mapreduce.reduce.java.opts", "-Xmx" + (int) (memory_percent_child * getMaxMemoryByContainer()) + "m");
		getConf().setInt("mapreduce.map.memory.mb", getMaxMemoryByContainer());
		getConf().setInt("mapreduce.reduce.memory.mb", getMaxMemoryByContainer());
		getConf().setInt("mapreduce.map.cpu.vcores", getCoresNumber());
		getConf().setInt("mapreduce.reduce.cpu.vcores", getCoresNumber());
		/*
		getConf().setInt("mapreduce.job.maps", getCoresNumber());
		getConf().setInt("mapreduce.job.reduces", getCoresNumber());
		getConf().setInt("mapreduce.tasktracker.map.tasks.maximum", getCoresNumber());
		getConf().setInt("mapreduce.tasktracker.reduce.tasks.maximum", getCoresNumber());
		*/
		getConf().setInt("mapreduce.job.running.map.limit", -1);
		getConf().setInt("mapreduce.tasktracker.indexcache.mb", 128);
		getConf().setInt("mapreduce.task.io.sort.mb", 1792);
	
		// set higher value when dealing with large data sets
		getConf().setInt("mapreduce.task.timeout", MAPREDUCE_TASK_TIMEOUT);// in
																	// millisecond
		getConf().setInt("mapreduce.job.counters.limit", 1000);//hadoop 3
		getConf().setInt(COUNTERS_MAX_KEY, 1000);
		
		// System.out.println(conf.get("mapred.task.partition"));
		// getConf().set("mapred.task.partition", "8");
	
		// Xms256m -Xmx4g -XX:-UseGCOverheadLimit -XX:GCTimeRatio=19
		// -XX:+UseParallelOldGC -XX:+UseParallelGC
	
		// because fs.default.name is unknown
		// getConf().set("fs.defaultFS", hdfsNode);
	
		// for large file
		// to avoid java.lang.OutOfMemoryError exception: GC overhead limit
		// exceeded
		// add this parameter to the JVM or to the program launcher
		// -XX:-UseGCOverheadLimit
		// conf.setBoolean("mapreduce.job.committer.task.cleanup.needed",
		// false);
		conf.setBoolean("mapred.job.tracker.persist.jobstatus.active", true);
	}

	/**
	 * set the configuration of the hadoop cluster
	 * 
	 * @throws IOException
	 */
	public void configureYARN() throws IOException {
		// mapred.xml
		getConf().set("yarn.resourcemanager.hostname", masterIp);
		getConf().set("mapreduce.framework.name", "yarn");
		getConf().set("mapreduce.application.classpath", 
				"$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/*, "
				+ "$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/lib/*, "
				+ "$HADOOP_HOME/user_lib/*");
		//			+ ":$HADOOP_HOME/user_lib/giraph_lib/*");
		
		//yarn.xml
		getConf().set("yarn.nodemanager.aux-services", "mapreduce_shuffle");
		getConf().set("yarn.nodemanager.aux-services.mapreduce_shuffle.class", 
				"org.apache.hadoop.mapred.ShuffleHandler");
		
		String hadoop_classpath_env="HADOOP_CLASSPATH=$HADOOP_CLASSPATH:$HADOOP_HOME/user_lib/*";
		getConf().set("mapreduce.map.env", hadoop_classpath_env);
		getConf().set("mapreduce.reduce.env", hadoop_classpath_env);
		getConf().set("mapreduce.admin.user.env", hadoop_classpath_env);
		getConf().set("yarn.app.mapreduce.am.env", hadoop_classpath_env);
		getConf().set("yarn.app.mapreduce.am.admin.user.env", hadoop_classpath_env);
		
		getConf().setInt("yarn.scheduler.minimum-allocation-vcores", 1);
		getConf().setInt("yarn.scheduler.maximum-allocation-vcores", getCoresNumber());
		getConf().setInt("yarn.scheduler.minimum-allocation-mb", 7168);
		getConf().setInt("yarn.scheduler.maximum-allocation-mb", (int) (memory_percent_parent * getMaxMemoryByContainer()));
		getConf().setInt("yarn.nodemanager.resource.cpu-vcores", getCoresNumber());
		getConf().setInt("mapreduce.map.resource.memory-mb", (int) (memory_percent_parent * getMaxMemoryByContainer()));
		getConf().setInt("mapreduce.reduce.resource.memory-mb", (int) (memory_percent_parent * getMaxMemoryByContainer()));
		getConf().setInt("mapreduce.map.resource.vcores", getCoresNumber());
		getConf().setInt("mapreduce.reduce.resource.vcores", getCoresNumber());
		
		//getConf().set("yarn.nodemanager.localizer.address", "${yarn.nodemanager.hostname}:8047");
		
	}

	/**
	 * read configuration from the $HADOOP_HOME/config/
	 * @throws IOException 
	 */
	public void setConfigFromFiles() throws IOException {
		File confDir = new File(confPath);
		if (confDir.exists() && confDir.isDirectory() && confDir.list().length > 0) {
			String[] confFiles = confDir.list(new FilenameFilter() {
				public boolean accept(File dir, String name) { // TODO
					// Auto-generated method stub
					if (name.endsWith(".xml"))
						return true;
					else
						return false;
				}
			});
			for (int i = 0; i < confFiles.length; i++) {
				System.out.print(confFiles[i]+"\t");
				conf.addResource(new Path(confPath + "/" + confFiles[i]));
			}
			// conf.set("hadoop.job.history.location",
			// "${hadoop.tmp.dir}/history/${user.name}");
			conf.set("dfs.web.ugi", "adnanmoe");
			//setHdfsNode(conf.get("fs.default.name")); //hadoop 1.x
			setHdfsNode(conf.get("fs.defaultFS"));
			setCoresNumber(conf.getInt("yarn.nodemanager.resource.cpu-vcores", getCoresNumber()));
			
			System.out.println(getHdfsNode());
			setHadoopDataFileSystem(FileSystem.get(URI.create(getHdfsNode()), conf));
		}
	}
	
	/**
	 * set additional configurations parameters
	 * 
	 * @param giraphConf
	 */
	protected void configureGiraph(GiraphConfiguration giraphConf) {
	
		// Multithreading
		/*
		 * To minimize network usage when reading input splits, each worker can
		 * prioritize splits that reside on its host.
		 */
		// GiraphConfiguration.USE_INPUT_SPLIT_LOCALITY.set(giraphConf, true);
		/**
		 * allows several vertexWriters to be created and written to in parallel
		 */
		GiraphConfiguration.VERTEX_OUTPUT_FORMAT_THREAD_SAFE.set(giraphConf, true);
		/**
		 * Number of threads for input/output split loading
		 */
		int numThreads = 1;
		GiraphConfiguration.NUM_INPUT_THREADS.set(giraphConf, 8);
		GiraphConfiguration.NUM_OUTPUT_THREADS.set(giraphConf, numThreads);
		GiraphConfiguration.NUM_COMPUTE_THREADS.set(giraphConf, getCoresNumber());
	
		// Message exchange tuning
		/**
		 * let a vertex receiving messages up to the limitation of the worker's
		 * heap space
		 */
		// GiraphConfiguration.USE_BIG_DATA_IO_FOR_MESSAGES.set(giraphConf, true);
		/*
		 * encodes one message for multiple vertices in the same destination
		 * partition. This encoding is useful for graph algorithms that
		 * broadcast identical messages to a large number of vertices
		 */
		// GiraphConfiguration.MESSAGE_ENCODE_AND_STORE_TYPE.set(giraphConf,
		// MessageEncodeAndStoreType.EXTRACT_BYTEARRAY_PER_PARTITION);
	
		// Out-of-core processing
		// GiraphConfiguration.USE_OUT_OF_CORE_GRAPH.set(giraphConf, true);
		// GiraphConfiguration.MAX_PARTITIONS_IN_MEMORY.set(giraphConf, 128);
		//giraphConf.setBoolean("giraph.useOutOfCoreMessages", true)
	
		// giraphConf.setBoolean("giraph.jmap.histo.enable", true);
		giraphConf.setBoolean("giraph.oneToAllMsgSending", true);
		/*
		 * giraph.serverReceiveBufferSize=integer, default 524288: You want to
		 * increase the buffer size when your application sends a lot of data;
		 * for example, many and/or large messages. This avoids that the server
		 * is constantly moving little pieces of data around
		 */
		int m = 100;
		giraphConf.setInt("giraph.serverReceiveBufferSize", m * 1024 * 1024); // m*1MByte
		giraphConf.setInt("giraph.clientSendBufferSize", m * 1024 * 1024); // m*1MByte
	
	}

	public FileSystem getHadoopDataFileSystem() {
		return hadoopDataFileSystem;
	}

	public void setHadoopDataFileSystem(FileSystem hadoopDataFileSystem) {
		this.hadoopDataFileSystem = hadoopDataFileSystem;
	}

	public int getCoresNumber() {
		return coresNumber;
	}

	public void setCoresNumber(int coresNumber) {
		this.coresNumber = coresNumber;
	}

	public int getWorkersNumber() {
		return workersNumber;
	}

	public void setWorkersNumber(int workersNumber) {
		this.workersNumber = workersNumber;
	}

	public int getMaxMemoryByContainer() {
		return maxMemoryByContainer;
	}

	public void setMaxMemoryByContainer(int maxMemoryByContainer) {
		this.maxMemoryByContainer = maxMemoryByContainer;
	}

	public String getHdfsNode() {
		return hdfsNode;
	}

	public void setHdfsNode(String hdfsNode) {
		this.hdfsNode = hdfsNode;
	}

	public String getMasterIp() {
		return masterIp;
	}

	public void setMasterIp(String masterIp) {
		this.masterIp = masterIp;
	}

	@Override
	public Configuration getConf() {
		// TODO Auto-generated method stub
		return conf;
	}

	@Override
	public void setConf(Configuration arg0) {
		// TODO Auto-generated method stub
		conf = arg0;
	}

	@Override
	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		return 0;
	}

}
