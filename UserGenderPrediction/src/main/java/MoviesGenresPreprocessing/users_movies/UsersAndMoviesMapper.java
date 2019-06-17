package MoviesGenresPreprocessing.users_movies;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

public class UsersAndMoviesMapper extends Mapper<LongWritable,Text,Text,NullWritable> {
    HashMap<String,String> moviesMap = new HashMap<String, String>();
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream("movies"),"UTF-8"));
        String movieInfo;
        while (StringUtils.isNotEmpty(movieInfo = reader.readLine())){
            //切割
            String[] fields = movieInfo.split("::");
            moviesMap.put(fields[0],fields[2]);
        }
        //关闭资源
        IOUtils.closeStream(reader);
    }

    Text k = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取一行
        String line = value.toString();
        //切割
        String[] fields = line.split("::");
        //获取movieID
        String movieID = fields[5];
        //取出usersAndRatings
        String movieGenres = moviesMap.get(movieID);
        //拼接
        line = line+"::"+movieGenres;
        k.set(line);
        //写出
        context.write(k,NullWritable.get());
    }
}


