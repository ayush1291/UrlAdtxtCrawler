package adpushup;

import adpushup.dbrelation.Adstxt;
import adpushup.dbrelation.Website;
import adpushup.reader.AdsReader;

import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A Runnable class which parses url and inserts ad records inside sql tables
 */
public class CrawlAndInsertTask implements Runnable{
    AdsReader reader;
    Website website= null;
    public static AtomicInteger integer= new AtomicInteger(0);

    public CrawlAndInsertTask(Website website, AdsReader reader){
        this.website=website;
        this.reader=reader;
    }

    @Override
    public void run() {
        Set<Adstxt> readFromUrl= null;
        try {
            readFromUrl = reader.readerFromUrl(website);
        } catch (Exception e) {
        }
        readFromUrl.addAll(Adstxt.findByUrl(website.getId()));
        Adstxt.bulkDeleteAndInsert(readFromUrl,website.getId());
        System.out.println("Done "+integer.incrementAndGet());
    }
}
