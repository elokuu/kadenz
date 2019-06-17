package jarUtil;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

public class JarUtil {
    public static String jar(Class<?> cls){
        String outputJar = cls.getName()+".jar";
        String input = cls.getClassLoader().getResource("").getFile();
        input = input.substring(0, input.length() - 1);
        input = input.substring(0,input.lastIndexOf("/")+1);
        input = input + "bin/";
        jar(input,outputJar);
        return outputJar;
    }
    public static void jar(String inputFileName, String outputFileName){
        JarOutputStream outputStream = null;
        try {
            outputStream = new JarOutputStream(new FileOutputStream(outputFileName));
            File file = new File(inputFileName);
            jar(outputStream,file,"");
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            try {
                outputStream.close();
            }catch (IOException e1){
                e1.printStackTrace();
            }
        }
    }

    public static void jar(JarOutputStream outputStream,File file,String base) throws IOException {
        if (file.isDirectory()){
            File[] files = file.listFiles();
            base = base.length() == 0 ? "" : base + "/";
            for (int i=0; i<files.length; i++){
                jar(outputStream,files[i],base+files[i].getName());
            }
        }else {
            outputStream.putNextEntry(new JarEntry(base));
            FileInputStream fileInputStream = new FileInputStream(file);
            byte[] buf = new byte[1024];
            int n = fileInputStream.read(buf);
            while (n != -1){
                outputStream.write(buf,0,n);
                n = fileInputStream.read(buf);
            }
            fileInputStream.close();
        }
    }
}
