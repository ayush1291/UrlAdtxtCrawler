package adpushup.dbrelation;

import adpushup.jdbc.JdbcConnector;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

/**
 * A Db relation mapped to ads_txt table.
 */
public class Adstxt implements DBRelation{
    private static final String TABLE_NAME="ads_txt";
    private static final String SQL_FIND_BY_URL="select * from "+TABLE_NAME+" where url=:url";
    private static final String SQL_DELETE_BY_URL="delete from "+TABLE_NAME+" where url=:url";

    private Integer url;
    private Integer advName;
    private String advId;
    private String accountType;
    private String tagId;
    private Adstxt(){}

    private Adstxt(Integer url){
        this.url=url;
    }

    public Adstxt(Integer url, Integer advName, String advId, String accountType, String tagId) {
        this.url = url;
        this.advName = advName;
        this.advId = advId;
        this.accountType = accountType;
        this.tagId = tagId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Adstxt adstxt = (Adstxt) o;
        return url.equals(adstxt.url) && advName.equals(adstxt.advName) && advId.equals(adstxt.advId) && accountType.equals(adstxt.accountType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(url, advName, advId, accountType);
    }


    /**
     * fetches all the rows for the url from the table
     * @param url
     * @return List of returned rows mapped to AdsTxt class
     */
    public static List<Adstxt> findByUrl(Integer url){
        try(Connection con = JdbcConnector.getConnection()){
            List<Adstxt> res= AdsTxtHelper.mapColumns(JdbcConnector.executeQuery(SQL_FIND_BY_URL,con, AdsTxtHelper.getObjectToMap(new Adstxt(url))));
            if(res!=null && res.size()>0) return res;
        } catch (SQLException e) {
        }
        return new ArrayList<>();
    }

    /**
     * fetches all the rows for the url from the table
     * @param url
     * @param con
     * @return List of returned rows mapped to AdsTxt class
     */
    public static List<Adstxt> findByUrl(Integer url, Connection con){
        try{
            List<Adstxt> res= AdsTxtHelper.mapColumns(JdbcConnector.executeQuery(SQL_FIND_BY_URL,con, AdsTxtHelper.getObjectToMap(new Adstxt(url))));
            if(res!=null && res.size()>0) return res;
        } catch (SQLException e) {
        }
        return new ArrayList<>();
    }

    /**
     * Deletes and bulk inserts in a single transaction
     * @param adstxts
     * @param url
     */
    public static void bulkDeleteAndInsert(Set<Adstxt> adstxts,Integer url){
        if(adstxts.size()==0) return;
        try(Connection con=JdbcConnector.getConnection()) {
            con.setAutoCommit(false);
            Statement stm = con.createStatement();
            JdbcConnector.executeUpdateQuery(SQL_DELETE_BY_URL,con, AdsTxtHelper.getObjectToMap(new Adstxt(url)));
            for(Adstxt adstxt:adstxts){
                stm.addBatch(AdsTxtHelper.createQueryToInsert(adstxt));
            }
            stm.executeBatch();
            con.commit();
        } catch (SQLException e) {
        }
    }

    /**
     * Deletes and bulk inserts many tasks in a single transaction
     * @param adstxts
     * @param url
     */
    public static void bulkDeleteAndInsertMultipleTasks(Map<Integer,Set<Adstxt>> adstxts){
        if(adstxts.size()==0) return;
        try(Connection con=JdbcConnector.getConnection()) {
            con.setAutoCommit(false);
            Statement stm = con.createStatement();
            for(Map.Entry<Integer,Set<Adstxt>> entry:adstxts.entrySet()){
                JdbcConnector.executeUpdateQuery(SQL_DELETE_BY_URL,con, AdsTxtHelper.getObjectToMap(new Adstxt(entry.getKey())));
                for(Adstxt adstxt:entry.getValue()){
                    stm.addBatch(AdsTxtHelper.createQueryToInsert(adstxt));
                }
            }
            stm.executeBatch();
            con.commit();
        } catch (SQLException e) {
        }
    }

    /**
     * Creates an object from a list. The list contains the adstxt record
     * @param url
     * @param list
     * @return A Adstxt object
     */
    public static Adstxt createObjectFromList(Integer url, List<String> list){
        Adstxt adstxt=new Adstxt();
        adstxt.url=url;
        adstxt.advName=Advertiser.getAdvertiser(list.get(0));
        adstxt.advId=list.get(1);
        adstxt.accountType=list.get(2);
        adstxt.tagId=list.size()==4?list.get(3):null;
        return adstxt;
    }

    /**
     * Helper class for various db operations
     */
    private static class AdsTxtHelper {

        /**
         * Maps all the rows returned into a list of Adstxt class
         * @param resultSet
         * @return List
         * @throws SQLException
         */
        public static List<Adstxt> mapColumns(ResultSet resultSet) throws SQLException {
            List<Adstxt> res = new ArrayList<>();
            while(resultSet.next()){
                Adstxt adstxt= new Adstxt();
                adstxt.url=resultSet.getInt(1);
                adstxt.advName=resultSet.getInt(2);
                adstxt.advId=resultSet.getString(3);
                adstxt.accountType=resultSet.getString(4);
                adstxt.tagId=resultSet.getString(5);
                res.add(adstxt);
            }
            return res;
        }

        /**
         * Creates an insert query from Adstxt object
         * @param adstxt
         * @return string query
         */
        public static String createQueryToInsert(Adstxt adstxt){
            StringBuilder query=new StringBuilder();
            query.append("Insert into "+TABLE_NAME+" (url,adv_name,adv_id,account_type,tag_id) values "+appendVal(adstxt));
            return query.toString();
        }

        /**
         * Helper method for creating insert queries. Creates a string value to append.
         * @param adstxt
         * @return
         */
        public static String appendVal(Adstxt adstxt){
            StringBuilder value = new StringBuilder();
            value.append("(");
            value.append(adstxt.url+",");
            value.append(adstxt.advName+",");
            value.append("'"+adstxt.advId+"'"+",");
            value.append("'"+adstxt.accountType+"'"+",");
            value.append("'"+adstxt.tagId+"'");
            value.append(")");
            return value.toString();
        }

        /**
         * Creates a mapping of named column and java class attributes for Adstxt
         * @param adstxt
         * @return map
         */
        public static Map<String,Object> getObjectToMap(Adstxt adstxt){
            Map<String,Object> map = new HashMap<>();
            map.put("url",adstxt.url);
            map.put("abvName",adstxt.advName);
            map.put("advid",adstxt.advId);
            map.put("accountType",adstxt.accountType);
            map.put("tagId",adstxt.tagId);
            return map;
        }
    }
}
