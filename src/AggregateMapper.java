import java.io.IOException;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;

public class AggregateMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private final static IntWritable one = new IntWritable(1);
	
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		

		String line = value.toString();
		String[] cols = line.split(";");

		Text outputKey = new Text(cols[0]+"\t"+cols[5]+"\t"+cols[4]);
		
		
		context.write(outputKey, one);

	}
}
