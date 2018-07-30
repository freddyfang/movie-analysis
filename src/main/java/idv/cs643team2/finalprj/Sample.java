package idv.cs643team2.finalprj;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

public class Sample {

	/**
	 * Sample the input file with given conditions and output it to a new file.
	 * If the condition is null, then all data would be collected.
	 * 
	 * @param inputFile
	 * @param outputDir
	 * @param condition
	 */
	public static void sample(String inputFile, String outputDir, Condition condition) {
		File input = new File(inputFile);
		if (!input.isFile()) {
			throw new IllegalArgumentException(inputFile + "is not a file!");
		}

		String filename = input.getName();
		File output = new File(outputDir, filename);
		if (output.exists()) {
			FileUtils.deleteQuietly(output);
		}

		BufferedReader reader = null;
		BufferedWriter writer = null;
		try {
			reader = new BufferedReader(new FileReader(input));
			writer = new BufferedWriter(new FileWriter(output));

			boolean isHeader = true;
			String line = null;
			while ((line = reader.readLine()) != null) {
				if (isHeader) {
					// Ignore header
					isHeader = false;
					continue;
				}

				if (condition == null || condition.satisfied(line)) {
					writer.write(line);
					writer.newLine();
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeQuietly(reader);
			IOUtils.closeQuietly(writer);
		}
	}

	/**
	 * Sample the input file with the given percentage and output it to a new file.
	 * That is, suppose the original dataset contains 100 data, with factor 0.6, about 60 data 
	 * would be selected.
	 * 
	 * @param inputFile
	 * @param outputDir
	 * @param percentage
	 */
	public static void sample(String inputFile, String outputDir, final double percentage) {
		if (percentage <= 0 || percentage > 1) {
			throw new IllegalArgumentException("Percentage must be > 0 and <= 1: " + percentage);
		}

		sample(inputFile, outputDir, new Condition() {

			@Override
			public boolean satisfied(String line) {
				return Math.random() <= percentage;
			}
		});
	}

	/*--------------------------------------------------------
	 * Inner class
	 --------------------------------------------------------*/
	public interface Condition {
		boolean satisfied(String line);
	}

	/*--------------------------------------------------------
	 * Main
	 --------------------------------------------------------*/
	public static void main(String[] args) {
		String inputDir = "src/main/resources";
		String outputDir = "src/main/resources/samples";

		// Sample movies.csv
		sample(inputDir + "/movies.csv", outputDir, 0.001);

		// Sample ratings.csv based on the generated movies.csv
		final Set<Long> movieIds = idSet(outputDir + "/movies.csv", 0);
		sample(inputDir + "/ratings.csv", outputDir, new Condition() {

			@Override
			public boolean satisfied(String line) {
				if (line == null || line.isEmpty()) {
					return false;
				}

				String[] split = line.split(",");
				if (split.length < 1) {
					return false;
				}

				long movieId = Long.parseLong(split[1]);
				return movieIds.contains(movieId);
			}
		});

		// Sample genome-tags.csv
		sample(inputDir + "/genome-tags.csv", outputDir, 0.02);

		// Sample genome-scores.csv based on the generated genome-tags.csv
		// Even if choosing genome-scores by few tag ids, the output dataset is 
		// still too large, thus we need to filter out some data randomly
		final Set<Long> tagIds = idSet(outputDir + "/genome-tags.csv", 0);
		final double factor = 0.1;
		sample(inputDir + "/genome-scores.csv", outputDir, new Condition() {

			@Override
			public boolean satisfied(String line) {
				if (line == null || line.isEmpty()) {
					return false;
				}

				String[] split = line.split(",");
				if (split.length < 1) {
					return false;
				}

				long tagId = Long.parseLong(split[1]);
				return tagIds.contains(tagId) && Math.random() <= factor;
			}
		});
	}

	private static Set<Long> idSet(String path, int index) {
		File file = new File(path);
		Set<Long> result = new HashSet<>();
		if (!file.isFile()) {
			return result;
		}

		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(file));

			String line = null;
			while ((line = reader.readLine()) != null) {
				String[] split = line.split(",");
				if (split.length < 1) {
					continue;
				}

				try {
					long id = Long.parseLong(split[index]);
					result.add(id);
				} catch (NumberFormatException e) {
					// do nothing
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeQuietly(reader);
		}

		return result;
	}

}
