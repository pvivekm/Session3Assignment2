package mapreduce.assignment2;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Task2Mapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	protected void map(LongWritable key,Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		String[] array = value.toString().split("[|]");
		Text k = new Text(key.toString());
		Text company = new Text(array[0]);
		Text product = new Text(array[1]);
		if(!("NA".equalsIgnoreCase(company.toString())) && !("NA".equalsIgnoreCase(product.toString())))
			context.write(k, value);
	}

	
}
