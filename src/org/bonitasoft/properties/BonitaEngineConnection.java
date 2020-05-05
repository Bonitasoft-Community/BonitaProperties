package org.bonitasoft.properties;

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
import javax.naming.NamingException;
import javax.sql.DataSource;

/* -------------------------------------------------------------------- */
/*                                                                      */
/* getConnection */
/*                                                                      */
/* -------------------------------------------------------------------- */

public class BonitaEngineConnection {

    private final static Logger logger = Logger.getLogger(BonitaEngineConnection.class.getName());
    private static final String loggerLabel = "BonitaProperties_2.3.0:";

    private static List<String> listDataSources = Arrays.asList("java:/comp/env/bonitaSequenceManagerDS",
            "java:jboss/datasources/bonitaSequenceManagerDS");

    /**
     * execute a request
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
                if (listResult.size()>=maximumResult)
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

        String msg = "";
        List<String> listDatasourceToCheck = new ArrayList<String>();
        for (String dataSourceString : listDataSources)
            listDatasourceToCheck.add(dataSourceString);

        for (String dataSourceString : listDatasourceToCheck) {
            // logger.info(loggerLabel + ".getDataSourceConnection() check[" + dataSourceString + "]");
            try {
                final Context ctx = new InitialContext();
                final DataSource dataSource = (DataSource) ctx.lookup(dataSourceString);
                // logger.info(loggerLabel + ".getDataSourceConnection() [" + dataSourceString + "] isOk");
                return dataSource.getConnection();

            } catch (NamingException e) {
                logger.info(loggerLabel + ".getDataSourceConnection() error[" + dataSourceString + "] : " + e.toString());
                msg += "DataSource[" + dataSourceString + "] : error " + e.toString() + ";";
            }
        }
        logger.severe(loggerLabel + ".getDataSourceConnection: Can't found a datasource : " + msg);
        return null;
    }

}
