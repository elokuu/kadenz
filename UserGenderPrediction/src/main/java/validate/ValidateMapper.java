package validate;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ValidateMapper extends Mapper<LongWritable,Text,NullWritable,Text> {
    private String splitter = ",";
    private Text v = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(splitter);
        v.set(fields[0]+splitter+fields[2]);
        context.write(NullWritable.get(),v);
    }
}
