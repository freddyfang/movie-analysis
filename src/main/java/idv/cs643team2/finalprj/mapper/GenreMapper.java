package idv.cs643team2.finalprj.mapper;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Write out genres based on movie id from movies.csv
 * 
 * @author FreddyFang
 */
public class GenreMapper extends Mapper<Object, Text, LongWritable, Text>{
	
	private LongWritable outKey = new LongWritable();
	private Text outVal = new Text();
	
	@Override
	protected void map(Object key, Text value, Mapper<Object, Text, LongWritable, Text>.Context context)
			throws IOException, InterruptedException {
		
		if (value == null || value.toString().startsWith("movieId")) {
			return;
		}
		
		String valStr = value.toString();
		int firstComma = valStr.indexOf(",");
		int lastComma = valStr.lastIndexOf(",");
		
		/*
		 * Movie title sometimes contains comma, thus directly split 
		 * line with comma is not applicable
		 */
		String movieId = valStr.substring(0, firstComma);
		String genres = valStr.substring(lastComma + 1, valStr.length());
		
		outKey.set(Long.parseLong(movieId));
		outVal.set(genres);
		
		context.write(outKey, outVal);
	}

}
