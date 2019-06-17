package movieClassify;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


public class MovieClassifyDriver{

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        if (args.length != 3){
            System.err.println("MovieClassifyDriver <traininput> <output> <k>");
            System.exit(-1);
        }
        Configuration configuration = new Configuration();
        configuration.setInt("K", Integer.parseInt(args[2]));


        Job job = Job.getInstance(configuration,"movie_knn");
        job.setJarByClass(MovieClassifyDriver.class);
        job.setMapperClass(MovieClassifyMapper.class);
        job.setReducerClass(MovieClassifyReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DistanceAndLabel.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileSystem.get(configuration).delete(new Path(args[1]),true);
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        boolean result =  job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }

}
