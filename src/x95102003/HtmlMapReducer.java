package x95102003;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class HtmlMapReducer {
	public static class HtmlParseMapper extends Mapper<LongWritable, Text, Text, Text>{
		public void map(LongWritable key, Text v, Context context){
			
		}
	}
	public static class HtmlParseReducer extends Reducer<Text, Text, Text, Text>{
		public void reduce(Text key, Text v, Context context){
			
		}
	}
}
