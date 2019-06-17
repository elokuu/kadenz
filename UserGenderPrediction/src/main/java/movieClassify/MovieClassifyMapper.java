package movieClassify;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;

public class MovieClassifyMapper extends Mapper<LongWritable,Text,Text, DistanceAndLabel> {
    private DistanceAndLabel distanceAndLabel = new DistanceAndLabel();
    private String splitter = ",";
    ArrayList<String> testData = new ArrayList<String>();
    private String testPath="hdfs://hadoop100:9001/movie/testData";
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();
        FileSystem fileSystem = FileSystem.get(configuration);

        FSDataInputStream inputStream = fileSystem.open(new Path(testPath));
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));

        String line = "";
        while ((line=bufferedReader.readLine()) != null){
            testData.add(line);
        }
        inputStream.close();
        bufferedReader.close();
    }

    Text k = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        double distance = 0.0;
        String[] fields = value.toString().split(splitter);
        String[] singleTrainData = Arrays.copyOfRange(fields, 5 ,fields.length);

        String label = fields[1];
        for (String td : testData){
            String[] test = td.split(splitter);
            String[] singleTestData = Arrays.copyOfRange(test, 5, test.length);
            distance = Distance(singleTrainData,singleTestData);
            distanceAndLabel.set(distance,label);

            k.set(td);
            context.write(k,distanceAndLabel);
        }
    }

    private double Distance(String[] singleTrainData,String[] singleTestData){
        double sum = 0.0;
        for (int i=0; i<singleTrainData.length; i++){
            sum += Math.pow(Double.parseDouble(singleTrainData[i]),Double.parseDouble(singleTestData[i]));
        }
        return Math.sqrt(sum);
    }
}
