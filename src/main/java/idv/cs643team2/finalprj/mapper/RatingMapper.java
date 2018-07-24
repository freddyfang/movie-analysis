package idv.cs643team2.finalprj.mapper;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Write out user rating based on movie id from ratings.csv
 * 
 * @author FreddyFang
 */
public class RatingMapper extends Mapper<Object, Text, LongWritable, Text>{
	
	private LongWritable outKey = new LongWritable();
	private Text outVal = new Text();
	
	@Override
	protected void map(Object key, Text value, Mapper<Object, Text, LongWritable, Text>.Context context)
			throws IOException, InterruptedException {
		
		if (value == null || value.toString().startsWith("userId")) {
			return;
		}
		
		String[] values = value.toString().split(",");
		outKey.set(Long.parseLong(values[1]));
		outVal.set(values[2]);
		
		context.write(outKey, outVal);
	}
	
}
