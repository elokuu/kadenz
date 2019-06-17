package MoviesGenresPreprocessing.randomDataGeneration;

import org.apache.commons.lang3.RandomStringUtils;

import java.io.*;
import java.util.Random;

public class RandomDataGeneration {
    public static void generateUsersFile(){
        try {
            File file = new File("e:/movie/users.dat");

            if (!file.exists()) {
                file.createNewFile();
            }

            FileWriter fw = new FileWriter(file.getAbsoluteFile());
            BufferedWriter bw = new BufferedWriter(fw);
            String content = "";
            Random random = new Random();
            EnumGender[] enums = EnumGender.values();
            for(int i=1; i<=1000; i++){
                EnumGender enumGender = enums[random.nextInt(enums.length)];
                content = i + "::" + enumGender.toString() + "::" +
                        (random.nextInt(60)+1)+ "::" + (random.nextInt(21)+1) + "::"+
                        RandomStringUtils.randomNumeric(5);
                bw.write(content);
                bw.newLine();
            }

            bw.close();

            System.out.println("users.dat Done");

        } catch (IOException e) {
            e.printStackTrace();
        }

    }
    enum EnumGender{
        F, M
    }
    public static void generateRatingsFile(){
        try {
            File file = new File("e:/movie/ratings.dat");

            if (!file.exists()) {
                file.createNewFile();
            }

            FileWriter fw = new FileWriter(file.getAbsoluteFile());
            BufferedWriter bw = new BufferedWriter(fw);
            String content = "";
            Random random = new Random();
            for(int i=1; i<=10000; i++){
                content = (random.nextInt(1000)+1) + "::" + (random.nextInt(3952)+1) + "::"
                        + (random.nextInt(5)+1) + "::"+
                        RandomStringUtils.randomNumeric(9);
                bw.write(content);
                bw.newLine();
            }

            bw.close();

            System.out.println("ratings.dat Done");

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void generateMoviesFile(){
        String[] genres = {"Action","Adventure","Animation","Children's","Comedy","Crime","Documentary","Drama",
                "Fantasy","Film-Noir","Horror","Musical","Mystery","Romance","Sci-Fi","Thriller","War","Western"};
        try {
            File file = new File("e:/movie/movies.dat");

            if (!file.exists()) {
                file.createNewFile();
            }

            FileWriter fw = new FileWriter(file.getAbsoluteFile());
            BufferedWriter bw = new BufferedWriter(fw);
            String content = "";
            Random random = new Random();
            for(int i=1; i<=3952; i++){
                int[] genresIndex = randomCommon(0,17,random.nextInt(3)+1);
                content = i + "::" + "MovieName" + "::"
                        + genres[genresIndex[0]];
                for (int j=1;j<genresIndex.length;j++){
                    content = content + "|" + genres[genresIndex[j]];
                }

                bw.write(content);
                bw.newLine();
            }

            bw.close();

            System.out.println("movies.dat Done");

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static int[] randomCommon(int min, int max, int n){
        if (n > (max - min + 1) || max < min) {
            return null;
        }
        int[] result = new int[n];
        int count = 0;
        while(count < n) {
            int num = (int) (Math.random() * (max - min)) + min;
            boolean flag = true;
            for (int j = 0; j < n; j++) {
                if(num == result[j]){
                    flag = false;
                    break;
                }
            }
            if(flag){
                result[count] = num;
                count++;
            }
        }
        return result;
    }
    public static void main(String[] args){
        generateUsersFile();
        generateRatingsFile();
        generateMoviesFile();
    }
}
