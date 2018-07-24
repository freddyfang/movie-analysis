package idv.cs643team2.finalprj.reducer;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Write out average rating, number of movies in this genre and number of user who rate the movies in 
 * this genre based on movie genre.
 * 
 * @author FreddyFang
 */
public class GenreAvgRatingReducer extends Reducer<Text, Text, Text, Text> {
	
	private Text outVal = new Text();
	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		
		double totalAvgRating = 0;
		int totalNumOfRatings = 0;
		int numOfMovies = 0;
		
		for (Text value : values) {
			String[] split = value.toString().split(",");
			double avgRating = Double.parseDouble(split[0]);
			int numOfRatings = Integer.parseInt(split[1]);
			
			totalAvgRating += avgRating;
			totalNumOfRatings += numOfRatings;
			numOfMovies++;
		}
		
		if (numOfMovies == 0) {
			return;
		}
		
		StringBuilder builder = new StringBuilder()
				.append(totalAvgRating / numOfMovies)
				.append(",")
				.append(numOfMovies)
				.append(",")
				.append(totalNumOfRatings);
		
		outVal.set(builder.toString());
		context.write(key, outVal);
	}

}
