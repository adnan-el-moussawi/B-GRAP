package giraph.lri.lahdak.data.format.bgrap;

import java.io.IOException;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import giraph.lri.lahdak.bgrap.utils.BGRAP_EdgeValue;
import giraph.lri.lahdak.bgrap.utils.BGRAP_VertexValue;


/**
 * Simple text-based {@link org.apache.giraph.io.VertexInputFormat} for
 * unweighted graphs with int ids.
 *
 * Each line consists of: vertex_partition
 * (for example : 1250_1
 * 
 * @author Adnan EL MOUSSAWI
 * @author rrojas
 *
 */
public class BGRAP_IdVertexIdPartitionOuputFormat
		extends TextVertexOutputFormat<IntWritable, BGRAP_VertexValue, BGRAP_EdgeValue> {
	/** Specify the output delimiter */
	public static final String LINE_TOKENIZE_VALUE = "output.delimiter";
	/** Default output delimiter */
	public static final String LINE_TOKENIZE_VALUE_DEFAULT = ",";

	public TextVertexWriter createVertexWriter(TaskAttemptContext context) {
		return new VertexValueWriter();
	}

	protected class VertexValueWriter extends TextVertexWriterToEachLine {
		/** Saved delimiter */
		private String delimiter;

		@Override
		public void initialize(TaskAttemptContext context) throws IOException, InterruptedException {
			super.initialize(context);
			Configuration conf = context.getConfiguration();
			delimiter = conf.get(LINE_TOKENIZE_VALUE, LINE_TOKENIZE_VALUE_DEFAULT);
		}

		@Override
		protected Text convertVertexToLine(Vertex<IntWritable, BGRAP_VertexValue, BGRAP_EdgeValue> vertex)
				throws IOException {
			//);
			short partition = vertex.getValue().getCurrentPartition();
			if(partition!=-1)
				return new Text(vertex.getId().get() + delimiter
						+ partition); //+delimiter
					//+ (vertex.getValue().getRealInDegree() + vertex.getValue().getRealOutDegree()));
			else
				return new Text("");
		}
	}
}
