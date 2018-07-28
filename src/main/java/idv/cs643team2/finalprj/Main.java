package idv.cs643team2.finalprj;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
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
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import idv.cs643team2.finalprj.mapper.GenomeScoreMapper;
import idv.cs643team2.finalprj.mapper.GenreAvgRatingMapper;
import idv.cs643team2.finalprj.mapper.GenreMapper;
import idv.cs643team2.finalprj.mapper.RatingMapper;
import idv.cs643team2.finalprj.reducer.GenomeScoreReducer;
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
			case "gt":	// genome tags
				return startGenomeTagsJob(inputDir, outputDir);	
			case "all":
			{
				int exitCode = runGenreRatingTask(inputDir, outputDir);
				if(exitCode == 1)	// failed
					return exitCode;
				else
					return startGenomeTagsJob(inputDir, outputDir);
			}
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
		Path tmpOutput1 = new Path(outputDir, "genre_join_rating");
		Path tmpOutput2 = new Path(outputDir, "genre_rating");
		
		FileSystem fileSys = FileSystem.get(new Configuration());
        fileSys.delete(tmpOutput1, true);
        fileSys.delete(tmpOutput2, true);
                
		Job job1 = genreJoinRatingJob(new Path[] {movies, ratings}, tmpOutput1);
		ControlledJob ctrlJob1 = new ControlledJob(job1.getConfiguration());
		ctrlJob1.setJob(job1);
		
		// Calculate rating based on movie genre
		Job job2 = genreAvgRatingJob(tmpOutput1, tmpOutput2);
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
	
	private static int startGenomeTagsJob(String inputDir, String output)
    {
    	try 
    	{
    		System.out.println("Start Job: Genome Tags");
    		
    		Configuration conf = new Configuration();
    		Job myJob = Job.getInstance(conf, "GenomeTags");	// the job
            
            // Manage the output directory
            // Delete the output directory if already exists
            FileSystem fileSys = FileSystem.get(new Configuration());
            output = output + "/tags_genome";
            fileSys.delete(new Path(output), true);
            
            FileInputFormat.addInputPath(myJob, new Path(inputDir + "//genome-scores.csv"));	// input
            FileInputFormat.addInputPath(myJob, new Path(inputDir + "//genome-tags.csv"));	// input
            
            FileOutputFormat.setOutputPath(myJob, new Path(output));	// output	
            
            // configure the job
            myJob.setMapperClass(GenomeScoreMapper.class);
            myJob.setReducerClass(GenomeScoreReducer.class);
            
            myJob.setMapOutputKeyClass(Text.class);
            myJob.setMapOutputValueClass(Text.class);
            
            myJob.setOutputKeyClass(Text.class);
            myJob.setOutputValueClass(Text.class); 
            
            myJob.setInputFormatClass(TextInputFormat.class);
            myJob.setOutputFormatClass(TextOutputFormat.class);
            LazyOutputFormat.setOutputFormatClass(myJob, TextOutputFormat.class);

            myJob.setJarByClass(Main.class);
			return myJob.waitForCompletion(true) ? 0 : 1;
		} 
    	catch (IOException | ClassNotFoundException | InterruptedException e) 
    	{
			e.printStackTrace();
		}	
    	
    	return 1;
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
		//args = new String[] {"gr", "src/main/resources", "src/main/resources/output"};
		//args = new String [] {"gt", "src/main/resources/samples", "src/main/resources/output"};
		//args = new String [] {"all", "/home/mcash/Desktop/resources", "/home/mcash/Desktop/output"};
		
		int exitCode = ToolRunner.run(new Main(), args);	
		
		System.out.println("Job(s) is completed. System exit code: " + exitCode);
		System.exit(exitCode);
	}
}
