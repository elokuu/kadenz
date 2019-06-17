package findBestK;

import movieClassify.MovieClassifyDriver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import validate.ValidateDriver;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class AllJob {
    private static String[] validateArgs = {
            "/movie/knnout/part-r-00000",
            "/movie/validateout"
    };

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(configuration);
        double maxAccuracy = 0.0;
        int bestK = 0;
        int[] k = {3,5,9,15,30,55,70,80,100};
        for (int i=0; i<k.length; i++){
            double accuracy = 0.0;
            String[] classifyArgs = {
                    "/movie/trainData",
                    "/movie/knnout/",
                    String.valueOf(k[i])
            };
            MovieClassifyDriver.main(classifyArgs);
            ValidateDriver.main(validateArgs);
            FSDataInputStream fsDataInputStream = fileSystem.open(new Path("/movie/validateout/part-r-00000"));
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fsDataInputStream));

            String line = "";
            while ((line=bufferedReader.readLine()) != null){
                accuracy = Double.parseDouble(line);
            }
            bufferedReader.close();
            fsDataInputStream.close();

            if (accuracy > maxAccuracy){
                maxAccuracy = accuracy;
                bestK = k[i];
            }
            System.out.println("K="+k[i]+"\t"+"accuracy="+accuracy);
        }
        System.out.println("最优K值为: "+bestK+"\t"+"最优K值对应的准确率为: "+maxAccuracy);
    }
}
