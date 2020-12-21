package adpushup;

import adpushup.dbrelation.Website;
import adpushup.jdbc.JdbcConnector;
import adpushup.logs.LogWriter;
import adpushup.reader.AdsReader;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * This program crawls all the listed domains and writes their ads.txt data into sql db as per configuration provided
 *
 * @author Ayush
 * @version 1.0
 */
public class Main {


    public static void main(String[] args) {
        if(args.length<2){
            System.out.println("Please pass args as [config_file] [domain_list_file_path] [-optional-no of threads]");
        }
        ConfigProperties.initProperties(args[0]);
        crawl(args);
    }


    /**
     * Crawls over all the websites and parses and writes ads.txt data into sql.
     * @param args
     */
    private static void crawl(String[] args){
        int threads=args.length==3?Integer.parseInt(args[2]):45;
        ExecutorService service= Executors.newFixedThreadPool(threads);
        AdsReader reader = getAdsReader();
        Set<String> urls = new HashSet<>();
        int batch=50;
        try(BufferedReader br = new BufferedReader(new FileReader(args[1]))) {
            String url = "platinumgod.co.uk";
            int i=0;
            Connection connection = JdbcConnector.getConnection();
            List<Website> tasks = new ArrayList<>();
            while (url != null) {
                if(urls.contains(url)){
                    url=br.readLine().trim();
                    continue;
                }
                Website website=Website.findByName(url,connection);
                if(website==null){
                    Website.insert(new Website(url),connection);
                    website=Website.findByName(url,connection);
                }
                tasks.add(website);
                if(tasks.size()==batch){
                    service.submit(new CrawlAndInsertMultipleTask(tasks,reader));
                    tasks=new ArrayList<>();
                }
                urls.add(url);
                url = br.readLine().trim();
            }
        }catch (Exception e){
            System.out.println("System aborted as "+e.getMessage());
        }
        service.shutdown();
        LogWriter.SUMMARY("Success "+CrawlAndInsertMultipleTask.integer.get()+"\n");
        LogWriter.SUMMARY("Failure "+CrawlAndInsertMultipleTask.failure.get());
    }

    /**
     * This method creates a class to get the reader class for reading and parsing all the ads.txt data
     * @return AdsReader
     */
    private static AdsReader getAdsReader(){
        try {
            return  (AdsReader) Class.forName(ConfigProperties.getProperties().get("ads.txt.reader.class")).newInstance();
        } catch (Exception e) {
            System.out.println("Could not instantiate reader class "+e.getMessage());
        }
        return null;
    }
}
