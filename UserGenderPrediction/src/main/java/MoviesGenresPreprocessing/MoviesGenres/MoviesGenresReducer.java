package MoviesGenresPreprocessing.MoviesGenres;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class MoviesGenresReducer extends Reducer<UserAndGender,Text,Text,NullWritable> {
    Text k = new Text();
    @Override
    protected void reduce(UserAndGender key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        HashMap<String,Integer> genresCounts = new HashMap<String, Integer>();

        String[] genreslist = {"Action","Adventure","Animation","Children's","Comedy","Crime","Documentary","Drama",
                "Fantasy","Film-Noir","Horror","Musical","Mystery","Romance","Sci-Fi","Thriller","War","Western"};
        for (int i=0; i<genreslist.length; i++){
            if(!genresCounts.containsKey(genreslist[i])){
                genresCounts.put(genreslist[i],0);
            }
        }

        for (Text value : values){
            String[] genres = value.toString().split("\\|");
            for (int i=0; i<genres.length; i++){
                if (genresCounts.containsKey(genres[i])){
                    genresCounts.put(genres[i],genresCounts.get(genres[i])+1);
                }
            }
        }

        String result = "";
        for (Map.Entry<String,Integer> kv : genresCounts.entrySet()){
            if (result.length() == 0){
                result = kv.getValue().toString();
            }else {
                result = result +","+ kv.getValue();
            }
        }
        k.set(key.toString()+","+result);

        context.write(k,NullWritable.get());
    }
}
