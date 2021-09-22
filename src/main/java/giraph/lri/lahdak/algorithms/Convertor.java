package giraph.lri.lahdak.algorithms;

import java.io.IOException;

import org.apache.giraph.graph.AbstractComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.LongWritable;

import giraph.lri.lahdak.bgrap.utils.BGRAP_EdgeValue;
import giraph.lri.lahdak.bgrap.utils.BGRAP_VertexValue;

public class Convertor
			extends AbstractComputation<LongWritable, BGRAP_VertexValue, BGRAP_EdgeValue, LongWritable, LongWritable> {

		@Override
		public void compute(Vertex<LongWritable, BGRAP_VertexValue, BGRAP_EdgeValue> vertex, Iterable<LongWritable> messages)
				throws IOException {
			vertex.voteToHalt();
		}
	}