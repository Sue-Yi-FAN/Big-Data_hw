package x95102003;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;
public class HtmlMapReduceTest {
	MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
	  ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
	  MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;
	 
	  @Before
	  public void setUp() {
//	    SMSCDRMapper mapper = new SMSCDRMapper();
//	    SMSCDRReducer reducer = new SMSCDRReducer();
//	    mapDriver = MapDriver.newMapDriver(mapper);
//	    reduceDriver = ReduceDriver.newReduc		eDriver(reducer);
//	    mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
	  }			
	 
	  @Test
	  public void testMapper() {
	    mapDriver.withInput(new LongWritable(), new Text(
	        "655209;1;796764372490213;804422938115889;6"));
	    mapDriver.withOutput(new Text("6"), new IntWritable(1));
	    try {
			mapDriver.runTest();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	  }
	 
	  @Test
	  public void testReducer() {
	    List<IntWritable> values = new ArrayList<IntWritable>();
	    values.add(new IntWritable(1));
	    values.add(new IntWritable(1));
	    reduceDriver.withInput(new Text("6"), values);
	    reduceDriver.withOutput(new Text("6"), new IntWritable(2));
	    try {
			reduceDriver.runTest();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	  }
	   
	  @Test
	  public void testMapReduce() {
	    mapReduceDriver.withInput(new LongWritable(), new Text(
	              "655209;1;796764372490213;804422938115889;6"));
	    List<IntWritable> values = new ArrayList<IntWritable>();
	    values.add(new IntWritable(1));
	    values.add(new IntWritable(1));
	    mapReduceDriver.withOutput(new Text("6"), new IntWritable(2));
	    try {
			mapReduceDriver.runTest();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	  }
}
