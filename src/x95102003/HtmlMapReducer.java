package x95102003;

import java.io.IOException;
import java.net.URL;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class HtmlMapReducer {
	public static String getDomainName(String checkString){
		URL url;
		try {
			url = new URL(checkString);
			return url.getHost().toString();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			return "";
		}
	}
	public static String parseHtml(String token) {
		if (token.startsWith("href=")) {
			String patternRgx = "http[s]?://.*\"";
			Matcher matcher = Pattern.compile(patternRgx).matcher(token);
			if (matcher.find()) {
				return getDomainName(matcher.group());
			}
		}
		return "";
	}
	public static class HtmlParseMapper extends
			Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text v, Context context)
				throws IOException, InterruptedException {
			String values[] = v.toString().split("@#\\*#@");
			String beginURL = getDomainName(values[0]);
			for (int i = 1; i < values.length; i++) {
				StringTokenizer tokenizer = new StringTokenizer(values[i]);
				while (tokenizer.hasMoreTokens()) {
					String html = tokenizer.nextToken();
					String outLink = parseHtml(html);
					if (outLink.length() > 0 && !outLink.equals(beginURL)) {
						context.write(new Text(beginURL), new Text(outLink));
					}
				}
			}

			}
		}

	public static class HtmlParseReducer extends
			Reducer<Text, Iterable<Text>, Text, Text> {
		final int initPageRank = 1;

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			String appedString = new String();
			for (Text v : values) {
				appedString += v.toString() + " ";
			}
			String mapperInput = String.valueOf(initPageRank) + " "
					+ appedString;
			context.write(key, new Text(mapperInput));
		}
	}

	public static void main(String args[]) throws IOException,
			ClassNotFoundException, InterruptedException {
		if (args.length != 2) {
			System.out
					.println("Error input format with arguments please type with two args");
		}

		Configuration conf = new Configuration();

		Job job = new Job(conf, "Html Parse");

		job.setJarByClass(HtmlMapReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapperClass(HtmlParseMapper.class);
		job.setReducerClass(HtmlParseReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
	}
}
