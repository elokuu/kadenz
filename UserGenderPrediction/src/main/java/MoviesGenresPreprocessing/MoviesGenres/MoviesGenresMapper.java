package MoviesGenresPreprocessing.MoviesGenres;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MoviesGenresMapper extends Mapper<LongWritable,Text,UserAndGender,Text> {
    private UserAndGender userAndGender = new UserAndGender();
    private Text genres = new Text();
    private String splitter = "::";

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(splitter);
        userAndGender.setUserID(fields[0]);
        if(fields[1].equals("M")){
            userAndGender.setGender(0);
        }else {
            userAndGender.setGender(1);
        }
        userAndGender.setAge(Integer.parseInt(fields[2]));
        userAndGender.setOccupation(fields[3]);
        userAndGender.setZip_code(fields[4]);

        genres.set(fields[6]);
        context.write(userAndGender,genres);
    }
}
