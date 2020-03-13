package exercise4;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//hadoop jar BD-201-mapreduce.jar exercise4.Ex4AverageWordLength /bigdata/dataset/capra mapreduce/ex4capra/output
//hdfs dfs -cat mapreduce/ex4capra/output/* | head -n 30
public class Ex4AverageWordLength {

	public static class Ex4Mapper extends Mapper<Object, Text, Text, IntWritable> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				String line = itr.nextToken();
				context.write(new Text(line.substring(0, 1)), new IntWritable(line.length()));
			}
		}
	}

	public static class Ex4Reducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			int count = 0;
			for (IntWritable val : values) {
				sum += val.get();
				count++;
			}
			context.write(key, new DoubleWritable((double) sum / count));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Average word length by initial letter");
		job.setJarByClass(Ex4AverageWordLength.class);
		if (args.length > 2) {
			if (Integer.parseInt(args[2]) >= 0) {
				job.setNumReduceTasks(Integer.parseInt(args[2]));
			}
		} else {
			job.setNumReduceTasks(1);
		}

		Path inputPath = new Path(args[0]), outputPath = new Path(args[1]);
		FileSystem fs = FileSystem.get(new Configuration());

		if (fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}

		job.setMapperClass(Ex4Mapper.class);
		job.setReducerClass(Ex4Reducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}