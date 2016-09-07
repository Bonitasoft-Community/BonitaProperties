package org.bonitasoft.ext.properties;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.sql.DataSource;

import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.log.event.BEvent.Level;
import org.bonitasoft.log.event.BEventFactory;
import org.bonitasoft.web.extension.page.PageResourceProvider;

@SuppressWarnings("serial")
public class BonitaProperties extends Properties {

    private final String myName;

    private static Logger logger = Logger.getLogger("org.bonitasoft.ext.properties");

    private static BEvent EventErrorAtLoad = new BEvent("org.bonitasoft.ext.properties", 1, Level.ERROR, "Error during loading properties", "Check Exception ",
            "The properties are empty", "Check Exception");

    private static BEvent EventCreationDatabase = new BEvent("org.bonitasoft.ext.properties", 2, Level.ERROR,
            "Error during creation the table in the database", "Check Exception ",
            "The properties will not work (no read, no save)", "Check Exception");

    private static BEvent EventErrorAtStore = new BEvent("org.bonitasoft.ext.properties", 3, Level.ERROR, "Error during saving the table in the database",
            "Check Exception ",
            "The properties are not saved", "Check Exception");
    /*
     * in case of an administration usage, all properties has to be load
     */
    private Map<String, Map<String, String>> allProperties;

    // java:/comp/env/bonitaSequenceManagerDS
    private final String sqlDataSourceName = "java:/comp/env/bonitaSequenceManagerDS";

    private final String sqlTableName = "bonitaproperties";
    private final String sqlResourceName = "resourcename";
    private final String sqlPropertiesKey = "propkey";
    private final String sqlPropertiesValue = "propvalue";

    /**
     * when you want to access the BonitaProperties from a Page, you have a PageResourceProvider.
     *
     * @param pageResourceProvider
     */
    public BonitaProperties(final PageResourceProvider pageResourceProvider)
    {
        myName = pageResourceProvider.getPageName();
    }

    public BonitaProperties(final String name)
    {
        myName = name;
    }

    /**
     * load all the properties informations
     */
    public List<BEvent> load()
    {
        Statement stmt = null;
        final List<BEvent> listEvents = new ArrayList<BEvent>();
        try
        {
            logger.info("Connect to [" + sqlDataSourceName + "]");
            final Context ctx = new InitialContext();
            final DataSource dataSource = (DataSource) ctx.lookup(sqlDataSourceName);
            Connection con = dataSource.getConnection();

            listEvents.addAll(checkCreateDatase(con));
            if (BEventFactory.isError(listEvents))
            {
                return listEvents;
            }
            String sqlRequest = "select * from " + sqlTableName;
            if (myName != null)
            {
                sqlRequest += " where " + sqlResourceName + "= '" + myName + "'";
            }
            stmt = con.createStatement();
            final ResultSet rs = stmt.executeQuery(sqlRequest);
            while (rs.next())
            {
                final String resourceName = rs.getString(sqlResourceName);
                final String key = rs.getString(sqlPropertiesKey);
                final String value = rs.getString(sqlPropertiesValue);
                // if there is a name ? Load the properties, else load the administration properties
                if (myName != null) {
                    setProperty(key, value);
                } else
                {
                    Map<String, String> onePropertie = allProperties.get(resourceName);
                    if (onePropertie == null) {
                        onePropertie = new HashMap<String, String>();
                    }
                    onePropertie.put(key, value);
                    allProperties.put(resourceName, onePropertie);
                }

            }
            stmt.close();
            con = null;
            if (myName != null) {
                logger.info("Load  Properties[" + myName + "] :  " + size() + " properties");
            } else {
                logger.info("Load  AllProperties :  " + allProperties.size() + " properties");
            }

        } catch (final Exception e)
        {
            final StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            final String exceptionDetails = sw.toString();
            logger.severe("Error during load properties [" + myName + "] : " + e.toString() + " : " + exceptionDetails);
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (final SQLException e1) {
                }
            }
            listEvents.add(new BEvent(EventErrorAtLoad, e, "properties name;[" + myName + "]"));
        }
        return listEvents;
    }

    /**
     * save all properties informations
     * ATTENTION : the current information are purge and replace with the new one. This is not a merge, so if the application want to do an update, a first
     * load() has to be done
     */
    public List<BEvent> store()
    {
        final List<BEvent> listEvents = new ArrayList<BEvent>();

        Statement stmt = null;
        try
        {
            final Context ctx = new InitialContext();
            final DataSource dataSource = (DataSource) ctx.lookup(sqlDataSourceName);
            Connection con = dataSource.getConnection();

            String sqlRequest = "delete from " + sqlTableName;
            if (myName != null) {
                sqlRequest += " where " + sqlResourceName + "= '" + myName + "'";
            }

            stmt = con.createStatement();

            logger.info("Purge all with [" + sqlRequest + "]");
            stmt.executeUpdate(sqlRequest);

            // now create all records
            if (myName != null)
            {
                for (final Object key : keySet())
                {
                    insertSql(stmt, myName, key.toString(), get(key).toString());
                }
            }
            else
            {
                for (final String resourceName : allProperties.keySet())
                {
                    for (final String key : allProperties.get(resourceName).keySet()) {
                        insertSql(stmt, resourceName, key.toString(), allProperties.get(resourceName).get(key).toString());
                    }
                }
            }
            con.commit();
            stmt.close();
            stmt = null;
            con = null;
        } catch (final Exception e)
        {
            final StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            final String exceptionDetails = sw.toString();
            logger.severe("Error during load properties [" + myName + "] : " + e.toString() + " : " + exceptionDetails);
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (final SQLException e1) {
                }
            }
            listEvents.add(new BEvent(EventErrorAtStore, e, "properties name;[" + myName + "]"));

        }
        return listEvents;
    }

    /**
     * do an insert in the SQL file
     *
     * @param stmt
     * @param resourceName
     * @param key
     * @param value
     * @throws SQLException
     */
    private void insertSql(final Statement stmt, final String resourceName, final String key, final String value) throws SQLException
    {
        final String sqlRequest = "insert into " + sqlTableName + " (" +
                sqlResourceName + "," + sqlPropertiesKey + "," + sqlPropertiesValue
                + ") VALUES('" + resourceName + "','" + key + "','" + value + "')";
        stmt.executeUpdate(sqlRequest);

    }

    /**
     * check if the table exist; if not then create it
     *
     * @param con
     * @return
     */
    private List<BEvent> checkCreateDatase(final Connection con)
    {
        final List<BEvent> listEvents = new ArrayList<BEvent>();

        try {
            final DatabaseMetaData dbm = con.getMetaData();

            final String databaseProductName = dbm.getDatabaseProductName();

            // check if "employee" table is there
            // nota: don't use the patern, it not give a correct result with H2
            final ResultSet tables = dbm.getTables(null, null, null, null);

            boolean exist = false;
            while (tables.next())
            {
                final String tableName = tables.getString("TABLE_NAME");
                if (tableName.equalsIgnoreCase(sqlTableName)) {
                    exist = true;
                }
            }
            if (exist) {
                // Table exists
                logger.info("CheckCreateTable [" + sqlTableName + "] : EXIST ");
            }
            else
            {
                logger.info("CheckCreateTable [" + sqlTableName + "] : NOT EXIST : create it");
                // create the table
                final String createTableString = "create table " + sqlTableName + " ("
                        + getSqlField(sqlResourceName, 200, databaseProductName) + ", "
                        + getSqlField(sqlPropertiesKey, 200, databaseProductName) + ", "
                        + getSqlField(sqlPropertiesValue, 10000, databaseProductName) + ")";

                logger.info("CheckCreateTable : not exist; Create table with [" + createTableString + "]");

                final Statement stmt = con.createStatement();
                stmt.executeUpdate(createTableString);
                con.commit();

                stmt.close();

            }
        } catch (final SQLException e) {
            final StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            final String exceptionDetails = sw.toString();
            logger.severe("Error during load properties [" + myName + "] : " + e.toString() + " : " + exceptionDetails);
            listEvents.add(new BEvent(EventCreationDatabase, e, "properties name;[" + myName + "]"));

        }
        return listEvents;
    }

    /**
     * calculate the field according different database
     *
     * @param colName
     * @param colSize
     * @param databaseProductName
     * @return
     */
    private String getSqlField(final String colName, final int colSize, final String databaseProductName) {

        if ("oracle".equalsIgnoreCase(databaseProductName)) {
            return colName + " VARCHAR2(" + colSize + ")";
        }
        if ("PostgreSQL".equalsIgnoreCase(databaseProductName)) {
            return colName + " varying(" + colSize + ")";
        } else if ("H2".equalsIgnoreCase(databaseProductName)) {
            return colName + "   varchar(" + colSize + ")";
        } else {
            return colName + "   varchar(" + colSize + ")";
        }
    }

}
