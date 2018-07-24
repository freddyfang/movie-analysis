package idv.cs643team2.finalprj.mapper;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Write out average rating and number of user who rate this movie based on genre
 * 
 * @author FreddyFang
 */
public class GenreAvgRatingMapper extends Mapper<Object, Text, Text, Text> {
	
	private Text outKey = new Text();
	private Text outVal = new Text();
	
	@Override
	protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		
		if (value == null || value.getLength() == 0) {
			return;
		}
		
		String[] split = value.toString()
				.split(":")[1].trim()
				.split(",");
		
		String avgRating = split[0];
		String numOfRatings = split[1];
		outVal.set(String.format("%s,%s", avgRating, numOfRatings));

		String[] genres = split[2].split("\\|");
		for (String genre : genres) {
			outKey.set(genre);
			context.write(outKey, outVal);
		}
	}

}
