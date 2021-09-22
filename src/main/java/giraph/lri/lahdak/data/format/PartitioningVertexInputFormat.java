package giraph.lri.lahdak.data.format;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.giraph.io.formats.TextVertexValueInputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import giraph.lri.lahdak.bgrap.utils.BGRAP_EdgeValue;
import giraph.lri.lahdak.bgrap.utils.BGRAP_VertexValue;

public class PartitioningVertexInputFormat
		extends TextVertexValueInputFormat<PartitionedLongWritable, BGRAP_VertexValue, BGRAP_EdgeValue> {
	private static final Pattern SEPARATOR = Pattern.compile("[\t ]");

	@Override
	public LongShortTextVertexValueReader createVertexValueReader(InputSplit split, TaskAttemptContext context)
			throws IOException {
		return new LongShortTextVertexValueReader();
	}

	public class LongShortTextVertexValueReader extends TextVertexValueReaderFromEachLineProcessed<String[]> {
		private PartitionedLongWritable id;

		@Override
		protected String[] preprocessLine(Text line) throws IOException {
			id = new PartitionedLongWritable(line.toString());
			return SEPARATOR.split(line.toString());
		}

		@Override
		protected PartitionedLongWritable getId(String[] data) throws IOException {
			return id;
		}

		@Override
		protected BGRAP_VertexValue getValue(String[] data) throws IOException {
			BGRAP_VertexValue value = new BGRAP_VertexValue();
			if (data.length > 1) {
				short partition = Short.parseShort(data[1]);
				value.setCurrentPartition(partition);
				value.setNewPartition(partition);
			}
			return value;
		}
	}
}
