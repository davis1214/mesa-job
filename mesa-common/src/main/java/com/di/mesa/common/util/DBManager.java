package com.di.mesa.common.util;

//import com.mchange.v2.c3p0.DataSources;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.mysql.jdbc.jdbc2.optional.MysqlConnectionPoolDataSource;

/**
 * Created by zhaoxican on 17/2/21.
 */
public class DBManager {

    private static MysqlConnectionPoolDataSource dataSource;
    private static final Log log = LogFactory.getLog(DBManager.class);

    static {
        try {
            log.info("initing DBManager...");
            Properties dbProperties = new Properties();
            dbProperties.load(DBManager.class.getResourceAsStream("/server.properties"));

            System.out.println(dbProperties.getProperty("jdbc.url"));
            System.out.println(dbProperties.getProperty("jdbc.user"));
            System.out.println(dbProperties.getProperty("jdbc.password"));

            
            
//            DataSource  unpool = DataSources.unpooledDataSource(
//                    dbProperties.getProperty("jdbc.url"), dbProperties.getProperty("jdbc.user"),
//                    dbProperties.getProperty("jdbc.password"));
//
//            HashMap<String, String> config = new HashMap<String, String>();
//            config.put("acquireRetryAttempts", "30");
//            config.put("acquireRetryDelay", "1000");
//            config.put("idleConnectionTestPeriod", "60");
//            config.put("maxPoolSize", "3");
//
//            dataSource = DataSources.pooledDataSource(unpool, config);
            
            dataSource = new MysqlConnectionPoolDataSource();
            dataSource.setUser(dbProperties.getProperty("jdbc.user"));
            dataSource.setPassword( dbProperties.getProperty("jdbc.password"));
            dataSource.setUrl(dbProperties.getProperty("jdbc.url"));

        } catch(Exception e) {
            log.error("init datasourcepool failed!", e);
        }     }

    private DBManager() {
    }

    /**
     * 获取链接，用完后记得关闭
     *
     * @see {@link DBManager#closeConn(Connection)}
     * @return
     */
    public static Connection getConn() {
        Connection conn = null;
        try {
            conn = dataSource.getConnection();
        } catch (Exception e) {
        	e.printStackTrace();
            log.error("获取数据库连接失败：" + e.getMessage());
        }
        return conn;
    }
    
    /**
     * 关闭连接
     *
     * @param conn
     *            需要关闭的连接
     */
    public static void closeConn(Connection conn) {
        try {
            if (conn != null && !conn.isClosed()) {
                conn.setAutoCommit(true);
                conn.close();
            }
        } catch (Exception e) {
            log.error("关闭数据库连接失败：" + e.getMessage());
        }
    }


    public static List<Map<String, String>> execQuery(
            String sql) throws SQLException {
        Connection conn = getConn();

        PreparedStatement ps = conn.prepareStatement(sql);
        ResultSet rs = null;
        try {
            rs = ps.executeQuery();

            if (rs == null)
                return null;

            ResultSetMetaData rss = rs.getMetaData();
            int columnCount = rss.getColumnCount();
            List<Map<String, String>> list = new ArrayList<Map<String, String>>();
            Map<String, String> row;
            while (rs.next()) {
                row = new HashMap<String, String>();
                for(int i = 1; i <= columnCount; i++){
                    row.put(rss.getColumnName(i), rs.getString(i));
                }
                list.add(row);
            }
            return list;
        } finally {
            if(rs != null)
                rs.close();
            if(ps != null)
                ps.close();

            closeConn(conn);
        }

    }

    public static void execUpdate(String sql) {
        Connection conn = getConn();
        Statement ps = null;
        try {

            ps = conn.createStatement();
            ps.executeUpdate(sql);
        }catch (Exception e){
            log.error(e.getMessage());
        }finally {
            if(ps != null)
                try {
                    ps.close();
                } catch (SQLException e) {
                }

            closeConn(conn);
        }
    }

    public static List<Map<String, String>> execQueryByPrepareStatement(
            String sql, String[] fields, Object... paras) throws SQLException {
        Connection conn = getConn();
        PreparedStatement ps = null;

        ps = conn.prepareStatement(sql);
        ps.setQueryTimeout(10);
        if (paras != null && paras.length > 0) for (int i = 0; i < paras.length; i++)
            ps.setObject(i + 1, paras[i]);

        ResultSet rs = null;
        try {
            rs = ps.executeQuery();
            if (rs == null) return null;
            List<Map<String, String>> list = new ArrayList<Map<String, String>>();
            Map<String, String> row;
            while (rs.next()) {
                row = new HashMap<String, String>();
                for (String f : fields)
                    row.put(f, String.valueOf(rs.getObject(f)));
                list.add(row);
            }
            return list;
        } finally {
            rs.close();
            ps.close();
            closeConn(conn);
        }

    }
}

