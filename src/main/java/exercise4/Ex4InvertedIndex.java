package exercise4;
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//hadoop jar BD-201-mapreduce.jar exercise4.Ex4InvertedIndex /bigdata/dataset/capra mapreduce/ex42capra/output
//hdfs dfs -cat mapreduce/ex42capra/output/* | head -n 30
public class Ex4InvertedIndex {

	public static class Ex4Mapper extends Mapper<Object, Text, Text, LongWritable> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				context.write(new Text(itr.nextToken()), (LongWritable) key);
			}
		}
	}

	public static class Ex4Reducer extends Reducer<Text, LongWritable, Text, Text> {

		public void reduce(Text key, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException {
			TreeSet<Long> offsets = new TreeSet<>();
			for (LongWritable val : values) {
				offsets.add(val.get());
			}
			context.write(key, new Text(offsets.toString()));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Average word length by initial letter");
		job.setJarByClass(Ex4InvertedIndex.class);
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
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}