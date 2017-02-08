package org.bonitasoft.ext.properties;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
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

    private static Logger logger = Logger.getLogger("org.bonitasoft.ext.properties");

    private static BEvent EventErrorAtLoad = new BEvent("org.bonitasoft.ext.properties", 1, Level.ERROR, "Error during loading properties", "Check Exception ",
            "The properties are empty", "Check Exception");

    private static BEvent EventCreationDatabase = new BEvent("org.bonitasoft.ext.properties", 2, Level.ERROR,
            "Error during creation the table in the database", "Check Exception ",
            "The properties will not work (no read, no save)", "Check Exception");

    private static BEvent EventErrorAtStore = new BEvent("org.bonitasoft.ext.properties", 3, Level.ERROR, "Error during saving the table in the database",
            "Check Exception ",
            "The properties are not saved", "Check Exception");

    /**
     * name of this properties, in order to manage only a dedicated perimeter
     */
    private final String mName;

    /**
     * to run in a multi thread environment, the caller can work domainName per domainName. A domainName can be a Userid, or whatever : load and save are then
     * working only
     * on the domainName.
     */
    private String mDomainName = null;

    /*
     * in case of an administration usage, all properties has to be load
     */
    private Hashtable<String, Hashtable<String, String>> mAllProperties;

    private String sqlDataSourceName = "java:/comp/env/bonitaSequenceManagerDS";

    // java:/comp/env/bonitaSequenceManagerDS

    private final static String cstSqlTableName = "bonitaproperties";
    private final static String cstSqlResourceName = "resourcename";
    private final static String cstSqldomainName = "domainname";
    private final static String cstSqlPropertiesKey = "propkey";
    private final static String cstSqlPropertiesValue = "propvalue";

    private boolean checkDatabaseAtFirstAccess = true;

    /**
     * when you want to access the BonitaProperties from a Page, you have a PageResourceProvider.
     *
     * @param pageResourceProvider
     */
    public BonitaProperties(final PageResourceProvider pageResourceProvider)
    {
        mName = pageResourceProvider.getPageName();
    }

    public BonitaProperties(final String name)
    {
        mName = name;
    }

    public List<BEvent> load()
    {
        return loaddomainName(null);
    }

    /**
     * load all the properties informations
     */
    public List<BEvent> loaddomainName(final String domainName)
    {
        mDomainName = domainName;
        Connection con = null;

        PreparedStatement pstmt = null;
        final List<BEvent> listEvents = new ArrayList();
        try
        {
            logger.info("Connect to [" + sqlDataSourceName + "] loaddomainename[" + domainName + "]");
            final Context ctx = new InitialContext();
            final DataSource dataSource = (DataSource) ctx.lookup(sqlDataSourceName);
            con = dataSource.getConnection();
            if (checkDatabaseAtFirstAccess) {
            listEvents.addAll(checkCreateDatase(con));
            }
            checkDatabaseAtFirstAccess = false;
            if (BEventFactory.isError(listEvents)) {
                return listEvents;
            }
            String sqlRequest = "select * from bonitaproperties where 1=1 ";

            final List<Object> listSqlParameters = new ArrayList();
            if (mName != null)
            {
                sqlRequest = sqlRequest + " and resourcename= ?";
                listSqlParameters.add(mName);
            }
            if (domainName != null)
            {
                sqlRequest = sqlRequest + " and domainname= ?";
                listSqlParameters.add(mDomainName);
            }
            logger.info("sqlRequest[" + sqlRequest + "]");

            pstmt = con.prepareStatement(sqlRequest);
            for (int i = 0; i < listSqlParameters.size(); i++) {
                pstmt.setObject(i + 1, listSqlParameters.get(i));
            }
            int count = 0;
            final ResultSet rs = pstmt.executeQuery();
            while (rs.next())
            {
                count++;
                final String resourceName = rs.getString(cstSqlResourceName);
                final String key = rs.getString(cstSqlPropertiesKey);
                final String value = rs.getString(cstSqlPropertiesValue);
                // if there is a name ? Load the properties, else load the administration properties
                if (mName != null)
            {
                    setProperty(key, value);
            }
                else
            {
                    Hashtable<String, String> onePropertie = mAllProperties.get(resourceName);
                    if (onePropertie == null) {
                        onePropertie = new Hashtable<String, String>();
                    }
                    onePropertie.put(key, value);
                    mAllProperties.put(resourceName, onePropertie);
            }
            }
            logger.info(" load from mName[" + mName + "] mDomainName[" + mDomainName + "] found[" + count + "]records");

            pstmt.close();
            pstmt = null;
            con.close();
            con = null;
            if (mName != null) {
                logger.info("Load  Properties[" + mName + "] :  " + size() + " properties");
            } else {
                logger.info("Load  AllProperties :  " + mAllProperties.size() + " properties");
            }
        } catch (final Exception e)
        {
            final StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            final String exceptionDetails = sw.toString();
            logger.severe("Error during load properties [" + mName + "] : " + e.toString() + " : " + exceptionDetails);
            if (pstmt != null) {
                try
            {
                    pstmt.close();
            }
 catch (final SQLException localSQLException) {
                }
            }
            if (con != null) {
                try
            {
                    con.close();
            }
 catch (final SQLException localSQLException1) {
                }
            }
            listEvents.add(new BEvent(EventErrorAtLoad, e, "properties name;[" + mName + "]"));
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
        String sqlRequest = null;
        try
        {
            final Context ctx = new InitialContext();
            final DataSource dataSource = (DataSource) ctx.lookup(sqlDataSourceName);
            Connection con = dataSource.getConnection();

            sqlRequest = "delete from " + cstSqlTableName + " where 1=1 ";
            if (mName != null) {
                sqlRequest += " and " + cstSqlResourceName + "= '" + mName + "'";
            }
            if (mDomainName != null) {
                sqlRequest += " and " + cstSqldomainName + "= '" + mDomainName + "'";
            }
            stmt = con.createStatement();

            logger.info("Purge all with [" + sqlRequest + "]");
            stmt.executeUpdate(sqlRequest);

            stmt.close();
            stmt = null;

            // now create all records
            Exception exceptionDuringRecord = null;
            if (mName != null)
            {
                exceptionDuringRecord = insertSql(con, mName, mDomainName, this);

            }
            else
            {

                for (final String resourceName : mAllProperties.keySet())
                {
                    exceptionDuringRecord = insertSql(con, resourceName, mDomainName, mAllProperties.get(resourceName));
                    if (exceptionDuringRecord != null)
                    {
                        break;
                    }
                }
            }
            if (exceptionDuringRecord != null)
            {
                listEvents.add(new BEvent(EventErrorAtStore, exceptionDuringRecord, "properties name;[" + mName + "]"));
                con.rollback();
            } else {
                con.commit();
            }

            stmt = null;
            con = null;
        } catch (final Exception e)
        {
            final StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            final String exceptionDetails = sw.toString();
            logger.severe("Error during store properties [" + mName + "] sqlRequest[" + sqlRequest + "] : " + e.toString() + " : " + exceptionDetails);
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (final SQLException e1) {
                }
            }
            listEvents.add(new BEvent(EventErrorAtStore, e, "properties name;[" + mName + "]"));

        }
        return listEvents;
    }

    public void setDataSource(final String dataSource)
    {
        sqlDataSourceName = dataSource;
    }

    public String getDataSource()
    {
        return sqlDataSourceName;
    }

    public boolean getCheckDatabase()
    {
        return checkDatabaseAtFirstAccess;
    }

    public void setCheckDatabase(final boolean checkDatabaseAtFirstAccess)
    {
        this.checkDatabaseAtFirstAccess = checkDatabaseAtFirstAccess;
    }

    /**
     * check the database to verify that the database is accessible and the table too
     * 
     * @return
     */
    public List<BEvent> checkDatabase()
    {
        final List<BEvent> listEvents = new ArrayList();
        Connection con = null;
        try
        {
            final Context ctx = new InitialContext();

            final DataSource dataSource = (DataSource) ctx.lookup(sqlDataSourceName);
            con = dataSource.getConnection();

            listEvents.addAll(checkCreateDatase(con));
        } catch (final Exception e)
        {
            final StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            final String exceptionDetails = sw.toString();
            logger.severe("Error during checkCreateDatase [" + exceptionDetails + "]");
            if (con != null) {
                try
                {
                    con.close();
                } catch (final SQLException localSQLException) {
                }
            }
            listEvents.add(new BEvent(EventErrorAtStore, e, "properties name;[" + mName + "]"));
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
    private Exception insertSql(final Connection con, final String resourceName, final String domainName, final Hashtable record /*
                                                                                                                                  * , final String key, final
                                                                                                                                  * String
                                                                                                                                  * value
                                                                                                                                  */)

    {
        /*
         * final String sqlRequest = "insert into " + sqlTableName + " (" +
         * sqlResourceName + "," + sqldomainName + "," + sqlPropertiesKey + "," + sqlPropertiesValue
         * + ") VALUES('" + resourceName + "','" + domainName + "','" + key + "','" + value + "')";
         */
        final String sqlRequest = "insert into " + cstSqlTableName + " (" +
                cstSqlResourceName + "," + cstSqldomainName + "," + cstSqlPropertiesKey + "," + cstSqlPropertiesValue
                + ") VALUES( ?,?,?,?)";
        String whatToLog = "prepareStatement";
        PreparedStatement pstmt = null;
        try
        {
            pstmt = con.prepareStatement(sqlRequest);
            for (final Object key : record.keySet()) {
                whatToLog = "values [" + cstSqlResourceName + ":" + resourceName + "] "
                        + "," + cstSqldomainName + ":" + domainName
                        + "," + cstSqlPropertiesKey + ":" + key.toString()
                        + "," + cstSqlPropertiesValue + ":" + record.get(key) + "]";

                pstmt.setString(1, resourceName);
                pstmt.setString(2, domainName);
                pstmt.setString(3, key.toString());

                pstmt.setString(4, record.get(key) == null ? null : record.get(key).toString());
                pstmt.executeUpdate(sqlRequest);

            }
            pstmt.close();
        } catch (final SQLException e) {
            final StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            final String exceptionDetails = sw.toString();
            logger.info("insertSql[" + whatToLog + "] at " + exceptionDetails);
            if (pstmt != null) {
                try {
                    pstmt.close();
                } catch (final Exception e2) {
                }
            };
            return e;
        }
        return null;
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
                if (tableName.equalsIgnoreCase(cstSqlTableName)) {
                    exist = true;
                }
            }
            if (exist) {
                final Map<String, Integer> listColsExpected = new HashMap<String, Integer>();
                listColsExpected.put(cstSqlResourceName.toLowerCase(), 200);
                listColsExpected.put(cstSqldomainName.toLowerCase(), 500);
                listColsExpected.put(cstSqlPropertiesKey.toLowerCase(), 200);
                listColsExpected.put(cstSqlPropertiesValue.toLowerCase(), 10000);

                final Map<String, Integer> alterCols = new HashMap<String, Integer>();

                // Table exists : is the fields are correct ?
                final ResultSet rs = dbm.getColumns(null /* catalog */, null /* schema */, null /* cstSqlTableName */, null /* columnNamePattern */);

                while (rs.next())
                {
                    String tableNameCol = rs.getString("TABLE_NAME");
                    final String colName = rs.getString("COLUMN_NAME");
                    final int length = rs.getInt("COLUMN_SIZE");

                    tableNameCol = tableNameCol == null ? "" : tableNameCol.toLowerCase();

                    if (!tableNameCol.equalsIgnoreCase(cstSqlTableName)) {
                        continue;
                    }
                    // final int dataType = rs.getInt("DATA_TYPE");
                    final Integer expectedSize = listColsExpected.containsKey(colName.toLowerCase()) ? listColsExpected.get(colName.toLowerCase()) : null;
                    if (expectedSize == null)
                    {
                        logger.info("Colum  [" + colName.toLowerCase() + "] : does not exist in [ " + listColsExpected + "];");
                        continue; // this columns is new
                    }
                    if (length < expectedSize) {
                        logger.info("Colum  [" + colName.toLowerCase() + "] : length[" + length + "] expected[" + expectedSize + "]");
                        alterCols.put(colName.toLowerCase(), expectedSize);
                    }
                    listColsExpected.remove(colName.toLowerCase());
                    // logger.info("Remove Colum  [" + colName.toLowerCase() + "] : list is now [ " + listColsExpected + "];");
                }
                // OK, create all missing column
                for (final String col : listColsExpected.keySet())
                {
                    executeAlterSql(con, "alter table " + cstSqlTableName + " add  " + getSqlField(col, listColsExpected.get(col), databaseProductName));
                }
                // all change operation
                for (final String col : alterCols.keySet())
                {
                    executeAlterSql(con, "alter table " + cstSqlTableName + " alter column " + getSqlField(col, alterCols.get(col), databaseProductName));
                }
                logger.info("CheckCreateTable [" + cstSqlTableName + "] : Correct ");
            }
            else
            {
                logger.info("CheckCreateTable [" + cstSqlTableName + "] : NOT EXIST : create it");
                // create the table
                final String createTableString = "create table " + cstSqlTableName + " ("
                        + getSqlField(cstSqlResourceName, 200, databaseProductName) + ", "
                        + getSqlField(cstSqldomainName, 500, databaseProductName) + ", "
                        + getSqlField(cstSqlPropertiesKey, 200, databaseProductName) + ", "
                        + getSqlField(cstSqlPropertiesValue, 10000, databaseProductName) + ")";
                executeAlterSql(con, createTableString);

            }
        } catch (final SQLException e) {
            final StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            final String exceptionDetails = sw.toString();
            logger.severe("Error during load properties [" + mName + "] : " + e.toString() + " : " + exceptionDetails);
            listEvents.add(new BEvent(EventCreationDatabase, e, "properties name;[" + mName + "]"));

        }
        return listEvents;
    }

    private void executeAlterSql(final Connection con, final String sqlRequest) throws SQLException
    {
        logger.info("executeAlterSql : Execute [" + sqlRequest + "]");

        final Statement stmt = con.createStatement();
        stmt.executeUpdate(sqlRequest);
        con.commit();

        stmt.close();

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
