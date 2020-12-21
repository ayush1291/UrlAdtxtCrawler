package adpushup.reader;

import adpushup.dbrelation.Adstxt;
import adpushup.dbrelation.Website;

import java.io.BufferedReader;
import java.nio.Buffer;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;

/**
 * AdsReader interface provides functionality to read from various types of sources supported. Starting with reading from a url.
 * @author Ayush
 */
public interface AdsReader {

    /**
     * Reads data from a given url. Validates all the ads data and parses it and returns a set containing all the unique records
     * @param url
     * @return Set<List<String>>
     */
    public Set<Adstxt> readerFromUrl(Website url) throws Exception;
}
