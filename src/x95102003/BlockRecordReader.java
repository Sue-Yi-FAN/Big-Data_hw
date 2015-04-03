package x95102003;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

public class BlockRecordReader extends RecordReader<LongWritable, Text> {
	private LineReader in;
	private LongWritable key;
	private Text value = new Text();
	private long start = 0;
	private long end = 0;
	private long pos = 0;
	private int maxLineLength;

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		if (in != null) {
			in.close();
		}

	}

	@Override
	public LongWritable getCurrentKey() throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		return key;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return (start == end) ? 0.0f : Math.min(1.0f, (pos - start)
				/ (float) (end - start));
	}
	private String getDomainName(String string){
		URL url;
		try {
			url = new URL(string);
			return  url.getHost().toString();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			return "";
		}
		
		
		
	}
	@Override
	public void initialize(InputSplit genericSplit, TaskAttemptContext context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		FileSplit split = (FileSplit) genericSplit;
		final Path file = split.getPath();
		Configuration conf = context.getConfiguration();
		this.maxLineLength = conf.getInt("mapred.linerecordreader.maxlength",
				Integer.MAX_VALUE);
		FileSystem fs = file.getFileSystem(conf);
		start = split.getStart();
		end = start + split.getLength();
		boolean skipFirstLine = false;
		FSDataInputStream filein = fs.open(split.getPath());

		if (start != 0) {
			skipFirstLine = true;
			--start;
			filein.seek(start);
		}
		in = new LineReader(filein, conf);
		if (skipFirstLine) {
			start += in.readLine(new Text(), 0,
					(int) Math.min((long) Integer.MAX_VALUE, end - start));
		}
		this.pos = start;

	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		if (key == null) {
			key = new LongWritable();
		}
		key.set(pos);
		if (value == null) {
			value = new Text();
		}
		value.clear();
		final Text endline = new Text("\n");
		int newSize = 0;
		String urlkey;
		Text v = new Text();
		while (pos < end){
			newSize = in.readLine(v, maxLineLength,
					Math.max((int) Math.min(Integer.MAX_VALUE, end - pos),
							maxLineLength));
			urlkey = v.toString();
			if(v.toString().trim().length() > 0){
				if(getDomainName(urlkey).length() > 0){
					break;
				}
			}
			pos += newSize;
		}
		while (pos < end) {
			newSize = in.readLine(v, maxLineLength,
					Math.max((int) Math.min(Integer.MAX_VALUE, end - pos),
							maxLineLength));
			value.append(v.getBytes(), 0, v.getLength());
			value.append(endline.getBytes(), 0, endline.getLength());
			if (newSize == 0) {
				break;
			}
			if (v.getLength() <= 1) {
				newSize = 0;
				break;
			}
			pos += newSize;
			if (newSize < maxLineLength) {
				break;
			}
		}
		if (newSize == 0) {
			key = null;
			value = null;
			return false;
		} else {
			return true;
		}
	}

}
