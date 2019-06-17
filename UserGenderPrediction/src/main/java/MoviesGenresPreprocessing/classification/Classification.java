package MoviesGenresPreprocessing.classification;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

public class Classification {
    public static int getSize(FileSystem fileSystem, Path path) throws IOException {
        int count = 0;
        FSDataInputStream fsDataInputStream = fileSystem.open(path);
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fsDataInputStream));
        String line = "";
        while ((line=bufferedReader.readLine()) != null){
            count++;
        }
        bufferedReader.close();
        fsDataInputStream.close();
        return count;
    }

    public static Set<Integer> trainIndex(int count){
        Set<Integer> train_index = new HashSet<Integer>();
        int trainSplitNum = (int)(count*0.8);
        Random random = new Random();

        while (train_index.size() < trainSplitNum){
            train_index.add(random.nextInt(count));
        }
        return train_index;
    }

    public static Set<Integer> validateIndex(int count, Set<Integer> train_index){
        Set<Integer> validate_index = new HashSet<Integer>();
        int validateSplitNum = count-(int)(count*0.9);
        Random random = new Random();
        while (validate_index.size() < validateSplitNum){
            int a = random.nextInt(count);
            if(!train_index.contains(a)){
                validate_index.add(a);
            }
        }
        return validate_index;
    }

    public static void main(String[] args) throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS","hadoop100:9001");
        FileSystem fileSystem = FileSystem.get(configuration);
        Path moviedata = new Path("/movie/gender_genre_processing/part-m-00000");
        int datasize = getSize(fileSystem,moviedata);
        Set<Integer> train_index = trainIndex(datasize);
        Set<Integer> validate_index = validateIndex(datasize,train_index);

        Path train = new Path("hdfs://hadoop100:9001/movie/trainData");
        fileSystem.delete(train,true);
        FSDataOutputStream fsDataOutputStream1 = fileSystem.create(train);
        BufferedWriter bufferedWriter1 = new BufferedWriter(new OutputStreamWriter(fsDataOutputStream1));

        Path test = new Path("hdfs://hadoop100:9001/movie/testData");
        fileSystem.delete(test,true);
        FSDataOutputStream fsDataOutputStream2 = fileSystem.create(test);
        BufferedWriter bufferedWriter2 = new BufferedWriter(new OutputStreamWriter(fsDataOutputStream2));

        Path validate = new Path("hdfs://hadoop100:9001/movie/validateData");
        fileSystem.delete(validate,true);
        FSDataOutputStream fsDataOutputStream3 = fileSystem.create(validate);
        BufferedWriter bufferedWriter3 = new BufferedWriter(new OutputStreamWriter(fsDataOutputStream3));

        FSDataInputStream fsDataInputStream = fileSystem.open(moviedata);
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fsDataInputStream));

        String line = "";
        int sum = 0;
        while ((line = bufferedReader.readLine()) != null){
            sum += 1;
            if(train_index.contains(sum)){
                bufferedWriter1.write(line);
                bufferedWriter1.newLine();
            }else if (validate_index.contains(sum)){
                bufferedWriter3.write(line);
                bufferedWriter3.newLine();
            }else {
                bufferedWriter2.write(line);
                bufferedWriter2.newLine();
            }
        }

        bufferedWriter1.close();
        fsDataOutputStream1.close();
        bufferedWriter2.close();
        fsDataOutputStream2.close();
        bufferedWriter3.close();
        fsDataOutputStream3.close();

        bufferedReader.close();
        fsDataInputStream.close();
        fileSystem.close();
    }
}
