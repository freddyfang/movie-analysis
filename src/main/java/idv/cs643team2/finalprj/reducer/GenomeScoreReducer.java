package idv.cs643team2.finalprj.reducer;

/**
 * GenomeScoreReducer.java
 * 
 * Export results to a text file with tagId, its name, and its scores
 * across all movies
 * 
 * @author FreddyFang
 */

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
 
public class GenomeScoreReducer extends Reducer<Text, Text, Text, Text> {
	private Text tKey = new Text();
	private Text tValue = new Text();	

	/**
	 * Reduce method
	 */
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
	{
		double sum = 0;
		String tagValue = "";
		String tagId = key.toString();
		
		for(Text value : values)
		{
			if(value.toString().matches(".*[a-zA-Z/\\?]+.*"))
				tagId = tagId + "\t" + value.toString().replace("removeMe_", "");
			else if(value.toString().matches("\\d+.*\\d*"))
			{
				sum = sum + Double.parseDouble(value.toString());		
				tagValue = sum + "";
			}
		}
		
		tKey.set(tagId);
		tValue.set(tagValue);

		context.write(tKey, tValue);			
	}
}
