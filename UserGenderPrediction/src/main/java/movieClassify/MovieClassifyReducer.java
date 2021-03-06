package movieClassify;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class MovieClassifyReducer extends Reducer<Text, DistanceAndLabel,Text,NullWritable> {
    private int k = 0;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        k = context.getConfiguration().getInt("K",3);
    }

    Text text = new Text();
    @Override
    protected void reduce(Text key, Iterable<DistanceAndLabel> values, Context context) throws IOException, InterruptedException {
        String label = getMost(getTopK(sort(values)));
        text.set(label+","+key);
        context.write(text,NullWritable.get());
    }

    private String getMost(List<String> topK){
        HashMap<String,Integer> labelTimes = new HashMap<String, Integer>();

        for (String str : topK){
            String label = str.substring(str.lastIndexOf(",")+1,str.length());
            if (labelTimes.containsKey(label)){
                labelTimes.put(label,labelTimes.get(label)+1);
            }else {
                labelTimes.put(label, 1);
            }
        }
        int maxInt = Integer.MIN_VALUE;
        String mostLabel = "";
        for (Map.Entry<String, Integer> kv:labelTimes.entrySet()){
            if (kv.getValue() > maxInt){
                maxInt = kv.getValue();
                mostLabel = kv.getKey();
            }
        }
        return mostLabel;
    }

    private List<String> getTopK(List<String> sort){
        return sort.subList(0,k);
    }

    private List<String> sort(Iterable<DistanceAndLabel> values){
        ArrayList<String> result = new ArrayList<String>();
        for (DistanceAndLabel value : values){
            result.add(value.toString());
        }
        String[] tmp = new String[result.size()];
        result.toArray(tmp);
        Arrays.sort(tmp, new Comparator<String>() {

            @Override
            public int compare(String o1, String o2) {
                double o1D = Double.parseDouble(o1.substring(0, o1.indexOf(",")));
                double o2D = Double.parseDouble(o2.substring(0, o2.indexOf(",")));

                if (o1D > o2D){
                    return 1;
                }else if (o1D < o2D){
                    return -1;
                }else {
                    return 0;
                }
            }
        });
        return Arrays.asList(tmp);
    }
}
