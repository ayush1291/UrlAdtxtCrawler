package adpushup.dbrelation;

import adpushup.jdbc.JdbcConnector;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A Db relation mapped to website table.
 */
public class Website {

    private static final String TABLE_NAME="website";
    private static final String SQL_FIND_BY_NAME="select * from "+TABLE_NAME+" where url=:url";
    private static final String SQL_FIND_ALL="select * from "+TABLE_NAME;

    private Integer id;
    private String url;

    public Website(){}

    public Website(String url){
        this.url =url;
    }

    public Website(Integer id, String url) {
        this.id = id;
        this.url = url;
    }

    public Integer getId() {
        return id;
    }

    public String getUrl() {
        return url;
    }


    /**
     * find record from db by searching with name
     * @param name
     * @param con
     * @return Website
     */
    public static Website findByName(String name, Connection con){
        try{
         List<Website> res= WebsiteHelper.mapColumns(JdbcConnector.executeQuery(SQL_FIND_BY_NAME,con, WebsiteHelper.getObjectToMap(new Website(name))));
         if(res.size()>0) return res.get(0);
        } catch (SQLException e) {
        }
        return null;
    }

    /**
     * Inserts a record into website tabe.
     * @param website
     * @param con
     * @return true if success, else false
     */
    public static boolean insert(Website website, Connection con){
        try{
            int x=JdbcConnector.executeUpdateQuery(WebsiteHelper.createQueryToInsert(website),con);
            return x>0;
        } catch (Exception e) {
        }
        return false;
    }


    /**
     * Helper class for db opertions
     */
    private static class WebsiteHelper {

        /**
         * Maps all the rows returned into a list of Advertiser class
         * @param resultSet
         * @return List with size either 0, when no records, or else with size of number of rows returned
         * @throws SQLException
         */
        public static List<Website> mapColumns(ResultSet resultSet) throws SQLException {
            List<Website> res = new ArrayList<>();
            while(resultSet.next()){
                Website website= new Website();
                website.id= resultSet.getInt(1);
                website.url = resultSet.getString(2);
                res.add(website);
            }
            return res;
        }


        /**
         * Creates an insert query from given object
         * @param website
         * @return string query formed
         */
        public static String createQueryToInsert(Website website){
            StringBuilder query=new StringBuilder();
            query.append("Insert into "+TABLE_NAME+" (url) values ("+"'"+website.getUrl()+"'"+")");
            return query.toString();
        }


        /**
         * Creates a map for named query from the given object
         * @param website
         * @return Map of string and object
         */
        public static Map<String,Object> getObjectToMap(Website website){
            Map<String,Object> map = new HashMap<>();
            map.put("url",website.getUrl());
            map.put("id",website.getId());
            return map;
        }
    }
}
