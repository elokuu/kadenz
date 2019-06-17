package validate;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ValidateReducer extends Reducer<NullWritable,Text,DoubleWritable,NullWritable> {
    private String splitter = ",";
    DoubleWritable k = new DoubleWritable();
    @Override
    protected void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        int count = 0;
        for (Text value : values){
            count++;
            String predictLabel = value.toString().split(splitter)[0];
            String trueLabel = value.toString().split(splitter)[1];

            if (trueLabel.equals(predictLabel)){
                sum += 1;
            }
        }
        double accuracy = (double)sum/count;
        k.set(accuracy);
        context.write(k,NullWritable.get());
    }
}
