package org.bonitasoft.properties;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NameNotFoundException;
import javax.naming.NamingException;

import org.bonitasoft.log.event.BEventFactory;

/* -------------------------------------------------------------------- */
/*                                                                      */
/* getConnection */
/*                                                                      */
/* -------------------------------------------------------------------- */

public class BonitaEngineConnection {

    private final static Logger logger = Logger.getLogger(BonitaEngineConnection.class.getName());
    private static final String loggerLabel = "BonitaProperties_2.8.0:";

    /*
     * private String[] listDataSources = new String[] { "java:/comp/env/bonitaSequenceManagerDS", // tomcat
     * "java:jboss/datasources/bonitaSequenceManagerDS" }; // jboss
     */

    // the datasource "java:/comp/env/bonitaDS", can't be used : it's under the control of Bitronix which not allow request.
    protected final static List<String> listDataSources = Arrays.asList(
            "java:/comp/env/RawBonitaDS", // 7.
            "java:/comp/env/bonitaSequenceManagerDS", // tomcat
            "java:jboss/datasources/bonitaSequenceManagerDS"); // jboss
    protected final static List<String> listBusinessDataSources = Arrays.asList(
            "java:/comp/env/NotManagedBizDataDS",
            "java:jboss/datasources/NotManagedBizDataDS");

    /**
     * execute a request
     * 
     * @param sqlRequest
     * @param parameters
     * @return
     * @throws Exception
     */
    public static List<Map<String, Object>> executeSqlRequest(String sqlRequest, List<Object> parameters, int maximumResult) throws Exception {
        List<Map<String, Object>> listResult = new ArrayList<>();
        try (Connection con = BonitaEngineConnection.getConnection();) {
            PreparedStatement pstmt = con.prepareStatement(sqlRequest);
            ResultSet rs = null;
            for (int i = 0; i < parameters.size(); i++)
                pstmt.setObject(i + 1, parameters.get(i));
            rs = pstmt.executeQuery();

            ResultSetMetaData rsMeta = pstmt.getMetaData();
            while (rs.next()) {
                Map<String, Object> record = new HashMap<>();
                for (int columnIndex = 1; columnIndex <= rsMeta.getColumnCount(); columnIndex++)
                    record.put(rsMeta.getColumnLabel(columnIndex).toUpperCase(), rs.getObject(columnIndex));
                listResult.add(record);
                if (listResult.size() >= maximumResult)
                    return listResult;
            }
            return listResult;
        } catch (Exception e) {
            throw e;
        }

    }

    /**
     * getConnection
     * 
     * @return
     * @throws NamingException
     * @throws SQLException
     */

    public static Connection getConnection() throws SQLException {
        // logger.info(loggerLabel+".getDataSourceConnection() start");

        DatabaseConnection.ConnectionResult connectionResult = DatabaseConnection.getConnection(listDataSources);
        if (connectionResult.con != null)
            return connectionResult.con;
        // already logged logger.severe(loggerLabel + ".getDataSourceConnection: Can't found a datasource : " +BEventFactory.getSyntheticLog(connectionResult.listEvents));
        return null;
    }

    /**
     * get the BusinessConnection
     * 
     * @return
     * @throws SQLException
     */
    public static Connection getBusinessConnection() throws SQLException {
        // logger.info(loggerLabel+".getDataSourceConnection() start");

        DatabaseConnection.ConnectionResult connectionResult = DatabaseConnection.getConnection(listBusinessDataSources);
        if (connectionResult.con != null)
            return connectionResult.con;

        // Everything is already logged logger.severe(loggerLabel + ".getBusinessDataSourceConnection: Can't found a datasource : " + msg.toString());
        return null;
    }
}
