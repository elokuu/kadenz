package MoviesGenresPreprocessing.DataProcessing;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DataProcessingMapper extends Mapper<LongWritable,Text,Text,NullWritable> {
    private String splitter = ",";
    private Text k = new Text();
    enum DataProcessingCounter{
        NullData,
        AbnormalData
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(splitter);
        for (int i=5; i<fields.length; i++){
            if (fields[i].equals("") || fields[i].equals("null") || fields[i].equals("NULL") || fields[i].equals("NAN")){
                context.getCounter(DataProcessingCounter.NullData).increment(1);
                fields[i] = "0";
            }else {
                context.getCounter(DataProcessingCounter.NullData).increment(0);
            }
            if (Integer.parseInt(fields[i])<0){
                context.getCounter(DataProcessingCounter.AbnormalData).increment(1);
                fields[i] = "0";
            }else {
                context.getCounter(DataProcessingCounter.AbnormalData).increment(0);
            }
        }

        String result = fields[0];

        for (int i = 1; i<fields.length; i++){
            result = result + splitter + fields[i];
        }
        k.set(result);
        context.write(k,NullWritable.get());
    }
}
