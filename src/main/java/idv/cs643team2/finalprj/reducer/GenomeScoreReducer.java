package idv.cs643team2.finalprj.reducer;
/**
 * ReducerMultiOutput.java
 * 
 * This is the reducer class
 */

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import idv.cs643team2.finalprj.Main;
 
public class GenomeScoreReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	private Text emitkey = new Text();
	private DoubleWritable emitvalue = new DoubleWritable();
	
	private Text eKey = new Text();
	private DoubleWritable eValue = new DoubleWritable();	
	private Map<String, Double> myTags = new HashMap <String, Double> ();
	private Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context myContext;

	/**
	 * Reduce method
	 */
	@Override
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
			throws IOException, InterruptedException {
			double sum = 0;

			for (DoubleWritable value : values)
				sum = sum + value.get();

			String path_tagId = key.toString();
			String tagId = path_tagId.split("\\*")[1];

			emitkey.set((tagId));
			emitvalue.set(sum);

			context.write(emitkey, emitvalue);
			Main.TP.collectTags(tagId, sum);			
	}
}
