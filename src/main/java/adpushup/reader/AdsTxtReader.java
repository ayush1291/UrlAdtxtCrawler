package adpushup.reader;

import adpushup.ConfigProperties;
import adpushup.dbrelation.Adstxt;
import adpushup.dbrelation.Advertiser;
import adpushup.dbrelation.Website;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.nio.Buffer;
import java.util.*;
import java.util.concurrent.Future;

/**
 * Implementation class for AdsReader
 * @author Ayush
 */
public class AdsTxtReader implements AdsReader{
    private static String HTTP_PREFIX="http://www.";
    private static String HTTPS_PREFIX="https://www.";
    private static String URL_POSTFIX="/ads.txt";

    /**
     *
     * @param website
     * @return Set<List<String>>
     */
    public Set<Adstxt> readerFromUrl(Website website) throws Exception{
        Set<Adstxt> res = readerFromUrlAndReturn(HTTPS_PREFIX+website.getUrl()+URL_POSTFIX,website.getId());
        if(res.size()==0) return readerFromUrlAndReturn(HTTP_PREFIX+website.getUrl()+URL_POSTFIX,website.getId());
        return res;
    }


    private Set<Adstxt> readerFromUrlAndReturn(String url, Integer urlid) throws Exception{
        Set<Adstxt> set = new HashSet<>();
        BufferedReader in = null;
        try {
            in = new BufferedReader(
                    new InputStreamReader(getConnection(url).getInputStream()));
            String inputLine;
            while ((inputLine = in.readLine()) != null){
                inputLine=inputLine.substring(0,inputLine.indexOf("#")==-1?inputLine.length():inputLine.indexOf("#"));
                String[] splits=inputLine.split(",");
                if(splits.length==0 && inputLine.charAt(0)!='#') break;
                if(isValidRecord(splits)){
                    set.add(getObjectFromRecord(splits,urlid));
                }
            }
            in.close();
        }catch (IOException e) {
            throw new Exception(e);
        }
        return set;
    }

    /**
     * Validates the adstxt record
     * @param splits
     * @return Boolean
     */
    private static boolean isValidRecord(String[] splits){
        return (splits!=null && (splits.length>=3 && splits.length<=4) && (splits[2].trim().toLowerCase().equals("direct") || splits[2].trim().toLowerCase().equals("reseller")));
    }

    private static Adstxt getObjectFromRecord(String[] splits,Integer urlId){
        return new Adstxt(urlId, Advertiser.getAdvertiser(splits[0].trim()),splits[1].trim(),splits[2].trim(),splits.length==4?splits[3]:null);
    }


    /**
     * Get connection object based on url passed
     * @param url The url for which connection is made
     * @return URLConnection
     * @throws IOException
     */
    private URLConnection getConnection(String url) throws IOException {
        URLConnection connection = new URL(url).openConnection();
        connection.setRequestProperty("User-Agent", "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.95 Safari/537.11");
        connection.connect();
        return connection;
    }
}

