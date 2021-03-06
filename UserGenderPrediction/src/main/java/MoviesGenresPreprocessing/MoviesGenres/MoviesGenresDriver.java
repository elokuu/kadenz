package MoviesGenresPreprocessing.MoviesGenres;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MoviesGenresDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        //开启map端输出压缩
        configuration.setBoolean("mapreduce.map.output.compress",true);
        //设置map端输出压缩方式
        configuration.setClass("mapreduce.map.output.compress.codec",BZip2Codec.class,CompressionCodec.class);

        Job job = Job.getInstance(configuration);

        job.setJarByClass(MoviesGenresDriver.class);
        job.setMapperClass(MoviesGenresMapper.class);
        job.setReducerClass(MoviesGenresReducer.class);

        job.setMapOutputKeyClass(UserAndGender.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
