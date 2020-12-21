package adpushup;

import adpushup.reader.AdsTxtReader;

import java.io.*;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

/**
 * Properties class containing all the configured properties in a map.
 * @author Ayush
 */
public class ConfigProperties {
    private static HashMap<String,String> props = new HashMap<>();

    public static void initProperties(String infile) {
        final int lhs = 0;
        final int rhs = 1;
        HashMap<String, String> map = new HashMap<>();
        try{
            BufferedReader bfr = new BufferedReader(new FileReader(new File(infile)));
            String line;
            while ((line = bfr.readLine()) != null) {
                if (!line.isEmpty()) {
                    String[] pair = line.trim().split("=");
                    map.put(pair[lhs].trim(), pair[rhs].trim());
                }
            }
            bfr.close();
        }catch (Exception e){
            System.out.println("Error while init properties "+e.getMessage());
        }
        props=map;
    }

    public static Map<String,String> getProperties(){
        return props;
    }
}
