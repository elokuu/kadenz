package validate;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class ValidateDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        if (args.length != 2){
            System.err.println("ValidateDriver <input> <output>");
            System.exit(-1);
        }
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration,"validate");
        job.setJarByClass(ValidateDriver.class);
        job.setMapperClass(ValidateMapper.class);
        job.setReducerClass(ValidateReducer.class);

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileSystem.get(configuration).delete(new Path(args[1]),true);
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        boolean result =  job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
