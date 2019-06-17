package MoviesGenresPreprocessing.ratings_users;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class RatingsAndUsersDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setJarByClass(RatingsAndUsersDriver.class);

        job.setMapperClass(RatingsAndUsersMapper.class);

        //注意 是OutputClass 而不是 MapOutputClass
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        //加载缓存数据

        job.addCacheFile(new URI("hdfs://hadoop100:9001/movie/users.dat#users"));
        //Map端和Join的逻辑不需要Reduce阶段，设置reduceTask的数量为0
        job.setNumReduceTasks(0);

        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
