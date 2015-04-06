package x95102003;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class HtmlPageRank {
	public static class mapper extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text v, Context context)
				throws IOException, InterruptedException {
			StringTokenizer token = new StringTokenizer(v.toString());
			String site = token.nextToken();
			double pageRank = Double.parseDouble(token.nextToken().toString());
			double linkCount = token.countTokens();
			double offerLink = pageRank / linkCount;
			while (token.hasMoreTokens()) {
				String outcome = token.nextToken();
				context.write(new Text(outcome), new Text(site));
				context.write(new Text(site),
						new Text(String.valueOf(offerLink)));
			}
		}
	}

	public static class reducer extends
			Reducer<Text, Iterable<Text>, Text, Text> {
		private static final double BETA = 0.85;

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			String appendString = new String();
			double pagerank = 0;
			for (Text v : values) {
				try {
					double offerLink = Double.parseDouble(v.toString());
					pagerank += offerLink;
				} catch (Exception e) {
					appendString += v.toString() + " ";
				}
			}

			pagerank = pagerank * BETA + (1 - BETA);
			String reInput = String.valueOf(pagerank) + " " + appendString;
			context.write(key, new Text(reInput));
		}
	}

	public static void setPageRank(String input, String output, int iter)
			throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "Page Rank" + iter);

		job.setJarByClass(HtmlPageRank.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(mapper.class);
		job.setReducerClass(reducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		job.waitForCompletion(true);
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		final int ITER = 20;
		if (args.length == 2) {
			String input = args[0];
			String output = args[1];
			for (int i = 0; i < ITER; i++) {
				if (i == 0) {
					output = "pageRank_o1/";
				} else if (i % 2 == 0) {
					input = "pageRank_o1/p*";
					output = "pageRank_o2/";
				} else {
					input = "pageRank_o2/p*";
					output = "pageRank_o1/";
				}
				try {
					setPageRank(input, output, i);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					System.out.println("Wrong");
					e.printStackTrace();
				}
			}
		} else {
			System.out.println("input format wrong with argument");
		}
	}
}
