package adpushup;

import adpushup.dbrelation.Adstxt;
import adpushup.dbrelation.Website;
import adpushup.jdbc.JdbcConnector;
import adpushup.logs.LogWriter;
import adpushup.reader.AdsReader;

import java.sql.Connection;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A Runnable class which parses url and inserts ad records inside sql tables
 */
public class CrawlAndInsertMultipleTask implements Runnable{
    AdsReader reader;
    List<Website> websites= null;
    List<Future<Set<Adstxt>>> responses;
    public static volatile AtomicInteger integer= new AtomicInteger(0);
    public static volatile AtomicInteger failure= new AtomicInteger(0);

    public CrawlAndInsertMultipleTask(List<Website> website, AdsReader reader){
        this.websites=website;
        this.reader=reader;
    }

    public CrawlAndInsertMultipleTask(List<Website> website, AdsReader reader,List<Future<Set<Adstxt>>> responses){
        this.websites=website;
        this.reader=reader;
        this.responses=responses;
    }

    @Override
    public void run() {
        Connection conn = JdbcConnector.getConnection();
        Map<Integer,Set<Adstxt>> map = new HashMap<>();
        for(Website website:websites){
            Set<Adstxt> set = null;
            try {
                set = reader.readerFromUrl(website);
                if(set.size()==0) throw new Exception("No size");
            } catch (Exception e) {
                LogWriter.FAILURE(website.getUrl()+"    "+e.getMessage());
                continue;
            }finally {
                System.out.println("done "+integer.incrementAndGet());
            }
            set.addAll(Adstxt.findByUrl(website.getId(),conn));
            map.put(website.getId(),set);
            LogWriter.SUCCESS(website.getUrl()+"     "+set.size());
        }
        JdbcConnector.closeConnection(conn);
        Adstxt.bulkDeleteAndInsertMultipleTasks(map);
    }
}
