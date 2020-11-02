package org.bonitasoft.properties;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NameNotFoundException;
import javax.naming.NamingException;

import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.log.event.BEvent.Level;
import org.bonitasoft.log.event.BEventFactory;

public class DatabaseConnection {

    private final static Logger logger = Logger.getLogger(DatabaseConnection.class.getName());
    private static final String LOGGER_LABEL = "BonitaProperties:";
    private static BEvent eventConnectDatabase = new BEvent(DatabaseConnection.class.getName(), 2, Level.ERROR,
            "Can't connect", "Can't connect to the database",
            "The connection can't be establish", "Check Exception");

    public static class ConnectionResult {
        public String datasource;
        public Connection con = null;
        public List<BEvent> listEvents = new ArrayList<>();
    }

    /**
     * @param dataSourceName
     * @return
     * @throws SQLException
     */
    public static ConnectionResult getConnection(List<String> listDataSourceName) throws SQLException {
        // logger.info(loggerLabel+".getDataSourceConnection() start");
        ConnectionResult connectionResult = new ConnectionResult();
        
        
        List<String> listCompletedDataSourceName = new ArrayList<>();
        listCompletedDataSourceName.addAll(listDataSourceName);
        // hum, the datasource should start with a java:xxx
        for (String dataSourceName : listDataSourceName) {
            if (dataSourceName.startsWith("java:/comp/env/")
                    || dataSourceName.startsWith("java:jboss/datasources/"))
                continue;
            listCompletedDataSourceName.add("java:/comp/env/" + dataSourceName);
        }
        // logger.info(loggerLabel + ".getDataSourceConnection() check[" + dataSourceString + "]");

        for (String dataSourceIterator : listCompletedDataSourceName) {
            try {

                final Context ctx = new InitialContext();
                final Object dataSource = ctx.lookup(dataSourceIterator);
                if (dataSource == null)
                    continue;
                // in Postgres, this object is not a javax.sql.DataSource, but a specific Object.
                // see https://jdbc.postgresql.org/development/privateapi/org/postgresql/xa/PGXADataSource.html
                // but each has a method "getConnection()
                Method m = dataSource.getClass().getMethod("getConnection");

                connectionResult.con = (Connection) m.invoke(dataSource);
                // logger.info(loggerLabel + ".getDataSourceConnection() [" + dataSourceString + "] isOk");
                connectionResult.listEvents.clear(); // clear error on previous tentative
                connectionResult.datasource = dataSourceIterator;
                return connectionResult;
            } catch (NameNotFoundException e) {
                // nothing to do, expected
            } catch (NamingException | NoSuchMethodException | SecurityException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
                connectionResult.listEvents.add(new BEvent(eventConnectDatabase, "Datasource[" + dataSourceIterator + "] Error:" + e.getMessage()));
            }
        }
        // here ? We did'nt connect then
        logger.severe(LOGGER_LABEL + BEventFactory.getSyntheticLog(connectionResult.listEvents));

        return connectionResult;
    }
}
