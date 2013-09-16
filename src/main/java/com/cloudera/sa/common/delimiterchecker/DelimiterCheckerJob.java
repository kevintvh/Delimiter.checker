package com.cloudera.sa.common.delimiterchecker;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Hello world!
 *
 */
public class DelimiterCheckerJob 
{
	protected static final String DELIMITER_CHAR_PROPERTY_NAME = "custom.delimiter.char";
	protected static final String EXPECTED_COLUMN_COUNT_PROPERTY_NAME = "custom.expected.column.count";
	protected static final String INVALID_RECORD_OUTPUT_PATH_PROPERTY_NAME = "custom.invalid.record.output.path";
	
    public static void main( String[] args ) throws IOException, InterruptedException, ClassNotFoundException
    {
    	if (args.length < 3 || args.length > 6)
		{
			System.out.println("DelimiterChecker <inputPath> <delimiterChar> <expectedColumnCount> <ValidRecordOutputPath> <OptionalInValidRecordOutputPath>");
			System.out.println();
			System.out.println("Example of filtering only good records: ");
			System.out.println("DelimiterChecker tmp/file.txt , 5 tmp/goodrecords ");
			System.out.println();
			System.out.println("Example of filtering good record but also output bad records: ");
			System.out.println("DelimiterChecker tmp/file.txt , 5 tmp/goodRecords tmp/badRecords");
			System.out.println();
			System.out.println("Remember is your delimiter is | use \\| in the command line because \\| is a special character");
			
			return;
		}

    	Job job = new Job();
    	
		//Get values from args
		String inputPath = args[0];
		String delimiterChar = args[1];
		String columnCount = args[2];
		String validRecordOutputPath = args[3];
		if (args.length == 5) {
			String optionalInValidRecordOutputPath = args[4];
			job.getConfiguration().set(INVALID_RECORD_OUTPUT_PATH_PROPERTY_NAME, optionalInValidRecordOutputPath);
		} else {
			job.getConfiguration().set(INVALID_RECORD_OUTPUT_PATH_PROPERTY_NAME, "");
		}
		
		job.getConfiguration().set(DELIMITER_CHAR_PROPERTY_NAME, delimiterChar);
		job.getConfiguration().set(EXPECTED_COLUMN_COUNT_PROPERTY_NAME, columnCount);
		
		//Create job
		
		job.setJarByClass(DelimiterCheckerJob.class);
		job.setJobName("DelimiterChecker:" + inputPath);
		//Define input format and path
		job.setInputFormatClass(TextInputFormat.class);
		
		String[] paths = inputPath.split(",");
		for (String p: paths) {
			TextInputFormat.addInputPath(job, new Path(p));
		}
		
		//Define output format and path
		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job, new Path(validRecordOutputPath));
		
		// Define the mapper and reducer
		job.setMapperClass(CustomMapper.class);

		// Define the key and value format
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setNumReduceTasks(0);

		FileSystem hdfs = FileSystem.get(new Configuration());
		hdfs.delete(new Path(validRecordOutputPath), true);
		
		// Exit
		job.waitForCompletion(true);

    }
    
    public static class CustomMapper extends Mapper<Writable, Text, Text, Text>
	{
    	static char delimiter;
    	public static String lineSeparator = System.getProperty("line.separator");
    	
    	int expectedNumberOfColumns = 0;
    	BufferedWriter invalidWriter = null;
		Text newKey = new Text();
		Text newValue = new Text();

		@Override
		public void setup(Context context) throws IOException {
			delimiter = context.getConfiguration().get(DELIMITER_CHAR_PROPERTY_NAME).charAt(0);
			expectedNumberOfColumns = Integer.parseInt(context.getConfiguration().get(EXPECTED_COLUMN_COUNT_PROPERTY_NAME));
			String optionalInvalidRecordOutputPath = context.getConfiguration().get(INVALID_RECORD_OUTPUT_PATH_PROPERTY_NAME);
			if (optionalInvalidRecordOutputPath != null && optionalInvalidRecordOutputPath.isEmpty() == false) {
				FileSystem hdfs = FileSystem.get(context.getConfiguration());
				invalidWriter = new BufferedWriter( new OutputStreamWriter(hdfs.create(new Path(optionalInvalidRecordOutputPath + "/part-m-" + context.getTaskAttemptID().getTaskID().getId()))));
			}
			
		}
		
		@Override
		public void cleanup(Context context) throws IOException {
			if (invalidWriter != null) {
				invalidWriter.close();
			}
		}
		
		@Override
		public void map(Writable key, Text value, Context context) throws IOException, InterruptedException
		{
			if (countColumns(value.toString()) != expectedNumberOfColumns) {
				context.getCounter("Results", "Invalid Records").increment(1);
				if (invalidWriter != null) {
					invalidWriter.append(value.toString() + lineSeparator);
				}
			} else {
				context.getCounter("Results", "Valid Records").increment(1);
				context.write(value, newValue);
			}
		}
		
		public static void setDelimiter(char d) {
			delimiter = d;
		}
		
		public static int countColumns(String line) {
		
			int index = line.indexOf(delimiter);
			int counter = 1;
			while (index > -1) {
				index = line.indexOf(delimiter, index + 1);
				counter++;
			}
			return counter;
		}
		
	}

}
