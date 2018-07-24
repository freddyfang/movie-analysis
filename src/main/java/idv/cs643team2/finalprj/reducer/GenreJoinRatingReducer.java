package idv.cs643team2.finalprj.reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalDouble;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Calculate the average rating from RatingMapper and join it with the genres from GenreMapper 
 * and then write them out with number of user who rate this movie
 * 
 * @author FreddyFang
 */
public class GenreJoinRatingReducer extends Reducer<LongWritable, Text, LongWritable, Text>{
	
	private Text outVal = new Text();
	
	@Override
	protected void reduce(LongWritable key, Iterable<Text> values, Reducer<LongWritable, Text, LongWritable, Text>.Context context)
			throws IOException, InterruptedException {
		
		List<Double> ratings = new ArrayList<>();
		String genres = null;
		
		for (Text txtVal : values) {
			Object val = parse(txtVal);
			
			if (val instanceof Double) {
				ratings.add((Double) val);
			} else {
				genres = (String) val;
			}
		}
		
		OptionalDouble opt = ratings.stream()
				.mapToDouble(x -> x.doubleValue())
				.average();

		if (!opt.isPresent()) {
			return;
		}
		
		double avgRating = opt.getAsDouble();
		int numOfRatings = ratings.size();
		StringBuilder builder = new StringBuilder()
				.append(avgRating)
				.append(",")
				.append(numOfRatings)
				.append(",")
				.append(genres);
		
		outVal.set(builder.toString());
		context.write(key, outVal);
	}
	
	private Object parse(Text txtVal) {
		String val = txtVal.toString();
		
		try {
			return Double.parseDouble(val);
		} catch(NumberFormatException e) {	
		}
		
		return val;
	}
}
