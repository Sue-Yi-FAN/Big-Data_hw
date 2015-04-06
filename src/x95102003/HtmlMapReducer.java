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

import x95102003.HtmlParser;
import x95102003.HtmlPageRank.mapper;
import x95102003.HtmlPageRank.reducer;
public class HtmlMapReducer {
	public static class HtmlParseMapper extends Mapper<LongWritable, Text, Text, Text>{
		public void map(LongWritable key, Text v, Context context) throws IOException, InterruptedException{
			StringTokenizer token = new StringTokenizer(v.toString());
			String outcomeURL="";
			String incomeURL ="";
			while(token.hasMoreTokens()){
				String webCont = token.nextToken();
				if(HtmlParser.getDomainName(webCont).length() > 0){
					outcomeURL = HtmlParser.getDomainName(webCont);
				}
				if(HtmlParser.parseHtml(webCont).length() > 0){
					incomeURL = HtmlParser.parseHtml(incomeURL);
				}
				if(incomeURL.length() > 0 && outcomeURL.length() > 0){
					context.write(new Text(incomeURL), new Text(outcomeURL));
				}
				
			}
		}
	}
	public static class HtmlParseReducer extends Reducer<Text, Iterable<Text>, Text, Text>{
		final int initPageRank = 1;
		public void reduce(Text key,Iterable<Text> values, Context context) throws IOException, InterruptedException{
			String appedString = new String();
			for (Text v : values){
				appedString += v.toString()+ " ";
			}
			String mapperInput = String.valueOf(initPageRank) + " " + appedString;
			context.write(key, new Text(mapperInput));
		}
	}
	public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException{
		Configuration conf = new Configuration();

		Job job = new Job(conf, "Html Parse");

		job.setJarByClass(HtmlPageRank.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(mapper.class);
		job.setReducerClass(reducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
	}
}
