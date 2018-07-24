package idv.cs643team2.finalprj;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import idv.cs643team2.finalprj.mapper.GenreAvgRatingMapper;
import idv.cs643team2.finalprj.mapper.GenreMapper;
import idv.cs643team2.finalprj.mapper.RatingMapper;
import idv.cs643team2.finalprj.reducer.GenreAvgRatingReducer;
import idv.cs643team2.finalprj.reducer.GenreJoinRatingReducer;

public class Main extends Configured implements Tool {
	
	@Override
	public int run(String[] args) throws Exception {
		String task = args[0];
		String inputDir = args[1];
		String outputDir = args[2];
		
		// Change key value separator in the output files from tab into ":"  
		getConf().set(TextOutputFormat.SEPERATOR, ":");
		switch(task) {
			case "gr":
				return runGenreRatingTask(inputDir, outputDir);
		}
		
		return 0;
	}
	
	/*--------------------------------------------------------
	 * Tasks
	 --------------------------------------------------------*/
	private int runGenreRatingTask(String inputDir, String outputDir) throws IOException {
		JobControl jobControl = new JobControl("GenreRatingJobs");
		
		// Join the movie rating and genres
		Path movies = new Path(inputDir, "movies.csv");
		Path ratings = new Path(inputDir, "ratings.csv");
		Path tmpOutput = new Path(outputDir, "genre_join_rating");
		Job job1 = genreJoinRatingJob(new Path[] {movies, ratings}, tmpOutput);
		ControlledJob ctrlJob1 = new ControlledJob(job1.getConfiguration());
		ctrlJob1.setJob(job1);
		
		// Calculate rating based on movie genre
		Job job2 = genreAvgRatingJob(tmpOutput, new Path(outputDir, "genre_rating"));
		ControlledJob ctrlJob2 = new ControlledJob(job2.getConfiguration());
		ctrlJob2.setJob(job2);
		
		ctrlJob2.addDependingJob(ctrlJob1);
		jobControl.addJob(ctrlJob1);
		jobControl.addJob(ctrlJob2);
		new Thread(jobControl).start();
		
		while (!jobControl.allFinished()) {
			System.out.println("Running jobs: " + jobControl.getRunningJobList().size());
			
			try {
				Thread.sleep(3000);
			} catch (InterruptedException e) {
				// do nothing
			}
		}
		
		return 0;
	}
	
	/*--------------------------------------------------------
	 * Jobs
	 --------------------------------------------------------*/
	private Job genreJoinRatingJob(Path[] inputs, Path output) throws IOException {
		Configuration conf = getConf();
		Job job = Job.getInstance(conf, "GenreJoinRating");

		job.setJarByClass(Main.class);
		
		job.setReducerClass(GenreJoinRatingReducer.class);

		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		
		MultipleInputs.addInputPath(job, inputs[0], TextInputFormat.class, GenreMapper.class);
		MultipleInputs.addInputPath(job, inputs[1], TextInputFormat.class, RatingMapper.class);

		removeIfExist(output);
		FileOutputFormat.setOutputPath(job, output);

		return job;
	}
	
	private Job genreAvgRatingJob(Path input, Path output) throws IOException {
		Configuration conf = getConf();
		Job job = Job.getInstance(conf, "GenreAvgRating");

		job.setJarByClass(Main.class);
		
		job.setMapperClass(GenreAvgRatingMapper.class);
		job.setReducerClass(GenreAvgRatingReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, input);

		removeIfExist(output);
		FileOutputFormat.setOutputPath(job, output);

		return job;
	}
	
	/*--------------------------------------------------------
	 * Helpers
	 --------------------------------------------------------*/
	private void removeIfExist(Path path) {
		if (path == null) {
			return;
		}
		
		File file = new File(path.toString());
		if (file.isDirectory() && file.exists()) {
			FileUtils.deleteQuietly(file);
		}
	}
	
	/*--------------------------------------------------------
	 * Main
	 --------------------------------------------------------*/
	public static void main(String[] args) throws Exception {
		// The first argument denotes the task to execute, the second argument
		// denotes input directory and the last argument denotes output directory
		args = new String[] {"gr", "src/main/resources", "src/main/resources/output"};
		
		int exitCode = ToolRunner.run(new Main(), args);
		System.exit(exitCode);
	}
}
