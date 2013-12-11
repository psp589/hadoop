package numeros2;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class add1Double {
	public static class Map
	extends MapReduceBase
	implements Mapper<LongWritable,Text,DoubleWritable,DoubleWritable>{
		//private Text word= new Text();
		public void map(LongWritable key,
				Text value,
				OutputCollector<DoubleWritable,DoubleWritable> output,
				Reporter reporter)
						throws IOException{
			String line= value.toString();
			DoubleWritable clave= new DoubleWritable();
			DoubleWritable valor= new DoubleWritable();
			clave.set(Double.parseDouble(line));
			valor.set(Double.parseDouble(line)+1.0);
			output.collect(clave, valor);
		}
	}
	public static void main(String[] args)throws Exception {
		JobConf conf=new JobConf(add1Double.class);
		conf.setJobName("sumar1Double");
		
		conf.setOutputKeyClass(DoubleWritable.class);
		conf.setOutputValueClass(DoubleWritable.class);
		
		conf.setMapperClass(Map.class);
		
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		JobClient.runJob(conf);
	}
}
