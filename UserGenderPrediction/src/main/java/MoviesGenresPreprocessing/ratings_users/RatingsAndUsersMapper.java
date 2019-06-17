package MoviesGenresPreprocessing.ratings_users;

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
import java.util.HashMap;

public class RatingsAndUsersMapper extends Mapper<LongWritable,Text,Text,NullWritable> {
    HashMap<String,String> usersMap = new HashMap<String, String>();
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream("users"),"UTF-8"));
        String userInfo;
        while (StringUtils.isNotEmpty(userInfo = reader.readLine())){
            //切割
            String[] fields = userInfo.split("::");
            usersMap.put(fields[0],userInfo);
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
        //获取userID
        String userID = fields[0];
        //取出userInfo
        String userInfo = usersMap.get(userID);
        //拼接
        line = userInfo+"::"+fields[1];
        k.set(line);
        //写出
        context.write(k,NullWritable.get());
    }
}

