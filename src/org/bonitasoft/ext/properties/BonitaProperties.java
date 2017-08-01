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
import java.util.Collections;
import java.util.Comparator;
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

    private final String loggerLabel="BonitaProperties_1.4:";
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

    private boolean mLoadAllDomains = false;

    private Long mTenantId;

    /*
     * in case of an administration usage, all properties has to be load
     * Structure : key is NAME
     * Value is Key/value
     * or
     * Value is Domain / Key/Value
     * Example :
     * .. "MyGoldApplication" : {
     * ...... "State":"California",
     * ...... "year":"1849",
     * ...... "Francis" : {
     * .......... "age":"43",
     * .........."country":"France"
     * ......}
     * ...... "Elvis" : {
     * .........."age":"25",
     * .........."country":"USA"
     * ......}
     * }
     */
    private Hashtable<String, Hashtable<String, Object>> mAllProperties;

    private String sqlDataSourceName = "java:/comp/env/bonitaSequenceManagerDS";

    // java:/comp/env/bonitaSequenceManagerDS

    private final static String cstSqlTableName = "bonitaproperties";
    private final static String cstSqlResourceName = "resourcename";
    private final static String cstSqldomainName = "domainname";
    private final static String cstSqlTenantId = "tenantId";

    private final static String cstSqlPropertiesKey = "propkey";
    private final static String cstSqlPropertiesValue = "propvalue";
    private final static int cstSqlPropertiesValueLength = 10000;

    private boolean checkDatabaseAtFirstAccess = true;

    /**
     * when you want to access the BonitaProperties from a Page, you have a PageResourceProvider.
     *
     * @param pageResourceProvider
     */
    public BonitaProperties(final PageResourceProvider pageResourceProvider)
    {
        mName = pageResourceProvider.getPageName();
        mTenantId = 1L;
    }

    public BonitaProperties(final PageResourceProvider pageResourceProvider, final long tenantId)
    {
        mName = pageResourceProvider.getPageName();
        mTenantId = tenantId;
    }


    public BonitaProperties(final String name)
    {
        mName = name;
        mTenantId = 1L;
    }

    public BonitaProperties(final String name, final long tenantId)
    {
        mName = name;
        mTenantId = tenantId;
    }

    public static BonitaProperties getAdminInstance()
    {
        final BonitaProperties bonitaProperties = new BonitaProperties((String) null);
        bonitaProperties.mLoadAllDomains = true;
        bonitaProperties.mTenantId = 1L;
        return bonitaProperties;
    }

    public static BonitaProperties getAdminInstance(final long tenantId)
    {
        final BonitaProperties bonitaProperties = new BonitaProperties((String) null);
        bonitaProperties.mLoadAllDomains = true;
        bonitaProperties.mTenantId = tenantId;

        return bonitaProperties;
    }

    public List<BEvent> load()
    {
        return loaddomainName(null);
    }

    // the constant is public then when a list of key is visible, the call know is this is a split key
    public final static String cstMarkerSplitTooLargeKey = "_#~_";
    /**
     * load all the properties informations
     */
    public List<BEvent> loaddomainName(final String domainName)
    {
        // logger.info("BonitaProperties.loadDomainName [" + domainName + "] CheckDatabase[" + checkDatabaseAtFirstAccess + "]");
        mDomainName = domainName;
        Connection con = null;
        final boolean originCheckDatabaseAtFirstAccess = checkDatabaseAtFirstAccess;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        final List<BEvent> listEvents = new ArrayList<BEvent>();
        try
        {
            //logger.info("Connect to [" + sqlDataSourceName + "] loaddomainename[" + domainName + "]");
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
            String sqlRequest = "select * from bonitaproperties where (" + cstSqlTenantId + "= " + (mTenantId == null ? 1 : mTenantId);
            // be compatible with 1.3 or lower which continue to read / write in this table : the tenantId will be null then at each write
            // so if an application using 1.3 move to 1.4, it must continue to read the data
            if (mTenantId == null && mTenantId == 1) {
                sqlRequest += " or " + cstSqlTenantId + " is null";
            }
            sqlRequest += ")";

            final List<Object> listSqlParameters = new ArrayList<Object>();
            if (mName != null)
            {
                sqlRequest = sqlRequest + " and resourcename= ?";
                listSqlParameters.add(mName);
            }
            if (!mLoadAllDomains)
            {
                if (mDomainName == null)
                {
                    // protect the domain information
                    sqlRequest = sqlRequest + " and domainname is null";
                }
                else
                {
                    sqlRequest = sqlRequest + " and domainname= ?";
                    listSqlParameters.add(mDomainName);
                }
            }

            //logger.info("sqlRequest[" + sqlRequest + "]");

            pstmt = con.prepareStatement(sqlRequest);
            for (int i = 0; i < listSqlParameters.size(); i++) {
                pstmt.setObject(i + 1, listSqlParameters.get(i));
            }
            final List<ItemKeyValue> collectTooLargeKey = new ArrayList<ItemKeyValue>();
            int count = 0;
            rs = pstmt.executeQuery();
            while (rs.next())
            {
                count++;

                final ItemKeyValue itemKeyValue = ItemKeyValue.getInstance(rs);

                // if the key contains the SplitTooLargeKey  then we collect it in a different map

                if (itemKeyValue.rsKey.contains(cstMarkerSplitTooLargeKey)) {
                    collectTooLargeKey.add(  itemKeyValue);
                } else
                {
                    dispatchKey(itemKeyValue);
                }
            }

            // if the collectTooLargeKey is not empty, recompose the key
            final List<ItemKeyValue> recomposeKey = recomposeTooLargekey(collectTooLargeKey);
            for (final ItemKeyValue itemKeyValue : recomposeKey)
            {
                dispatchKey(itemKeyValue);
            }
            logger.info(loggerLabel+".Loadfrom  [" + mDomainName
                    + "] CheckDatabase[" + originCheckDatabaseAtFirstAccess
                    + "] name[" + mName
                    + "] " + (mName != null ? "*LoadProperties*" : "*LoadALLPROPERTIES*")
                    + " found[" + count + "]records sqlRequest[" + sqlRequest + "]");


            /*
             * if (mName != null) {
             * logger.info("Load  Properties[" + mName + "] :  " + size() + " properties");
             * } else {
             * logger.info("Load  AllProperties :  " + mAllProperties.size() + " properties");
             * }
             */
        } catch (final Exception e)
        {
            final StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            final String exceptionDetails = sw.toString();
            logger.severe(loggerLabel+".loadDomainName Error during load properties [" + mName + "] : " + e.toString() + " : " + exceptionDetails);

            listEvents.add(new BEvent(EventErrorAtLoad, e, "properties name;[" + mName + "]"));
        }
        finally
        {
            if (rs != null) {
                try
                {
                    rs.close();
                    rs = null;
                } catch (final SQLException localSQLException) {
                }
            }
            if (pstmt != null) {
                try
                {
                    pstmt.close();
                    pstmt = null;
                } catch (final SQLException localSQLException) {
                }
            }
            if (con != null) {
                try
                {
                    con.close();
                    con = null;
                } catch (final SQLException localSQLException1) {
                }
            }
        }
        return listEvents;
    }

    /**
     * let the JUNIT test access to this class
     */
    protected static class ItemKeyValue
    {

        public long rsTenantId;
        public String rsResourceName;
        public String rsDomainName;
        public String rsKey;
        public String rsValue;

        /** theses fields are used to recompose the key from a split key */
        public String baseKey;
        public int numKey;

        public static ItemKeyValue getInstance(final long tenantId, final String resourceName, final String domainName, final String key, final String value)
        {
            final ItemKeyValue itemKeyValue = new ItemKeyValue();
            itemKeyValue.rsTenantId = tenantId;
            itemKeyValue.rsResourceName = resourceName;
            itemKeyValue.rsDomainName = domainName;
            itemKeyValue.rsKey = key;
            itemKeyValue.rsValue = value;
            return itemKeyValue;
        }

        public static ItemKeyValue getInstance(final ItemKeyValue source)
        {
            final ItemKeyValue itemKeyValue = new ItemKeyValue();
            itemKeyValue.rsTenantId = source.rsTenantId;
            itemKeyValue.rsResourceName = source.rsResourceName;
            itemKeyValue.rsDomainName = source.rsDomainName;
            itemKeyValue.rsKey = source.rsKey;
            itemKeyValue.rsValue = source.rsValue;
            return itemKeyValue;
        }

        public static ItemKeyValue getInstance(final ResultSet rs) throws SQLException
        {
            final ItemKeyValue itemKeyValue = new ItemKeyValue();
            itemKeyValue.rsTenantId = rs.getLong(cstSqlTenantId);
            itemKeyValue.rsResourceName = rs.getString(cstSqlResourceName);
            itemKeyValue.rsDomainName = rs.getString(cstSqldomainName);
            itemKeyValue.rsKey = rs.getString(cstSqlPropertiesKey);
            itemKeyValue.rsValue = rs.getString(cstSqlPropertiesValue);
            return itemKeyValue;
        }
    }

    /**
     *
     * @param itemKeyValue
     */
    private void dispatchKey(final ItemKeyValue itemKeyValue)
    {
        if (mName != null)
        {

            setProperty(itemKeyValue.rsKey, itemKeyValue.rsValue);
        }
        else
        {
            if (mAllProperties == null) {
                mAllProperties = new Hashtable<String, Hashtable<String, Object>>();
            }
            Hashtable<String, Object> mapName = mAllProperties.get(itemKeyValue.rsResourceName);
            if (mapName == null) {
                mapName = new Hashtable<String, Object>();
                mAllProperties.put(itemKeyValue.rsResourceName, mapName);
            }
            if (itemKeyValue.rsDomainName != null)
            {
                Hashtable<String, String> mapDomain = (Hashtable<String, String>) mapName.get(itemKeyValue.rsDomainName);
                if (mapDomain == null) {
                    mapDomain = new Hashtable<String, String>();
                    mapName.put(itemKeyValue.rsDomainName, mapDomain);
                }
                mapDomain.put(itemKeyValue.rsKey, itemKeyValue.rsValue);
            } else {
                mapName.put(itemKeyValue.rsKey, itemKeyValue.rsValue);
            }
        }
    }

    /**
     * save all properties informations
     * ATTENTION : the current information are purge and replace with the new one. This is not a merge, so if the application want to do an update, a first
     * load() has to be done
     */
    public List<BEvent> store()
    {
        final List<BEvent> listEvents = new ArrayList<BEvent>();
        logger.info(loggerLabel+"store()");

        Statement stmt = null;
        String sqlRequest = null;
        Connection con = null;
        try
        {
            final Context ctx = new InitialContext();
            final DataSource dataSource = (DataSource) ctx.lookup(sqlDataSourceName);
            con = dataSource.getConnection();

            sqlRequest = "delete from " + cstSqlTableName + " where  " + cstSqlTenantId + "= " + (mTenantId == null ? 1 : mTenantId);
            if (mName != null) {
                sqlRequest += " and " + cstSqlResourceName + "= '" + mName + "'";
            }
            if (mDomainName == null) {
                // protect the domain
                sqlRequest += " and " + cstSqldomainName + " is null";
            }
            else
            {
                sqlRequest += " and " + cstSqldomainName + "= '" + mDomainName + "'";
            }
            stmt = con.createStatement();

            logger.info(loggerLabel+"Purge all with [" + sqlRequest + "]");
            stmt.executeUpdate(sqlRequest);


            // now create all records
            Exception exceptionDuringRecord = null;
            if (mName != null)
            {
                exceptionDuringRecord = insertSql(con, mName, mDomainName, this);
                if (exceptionDuringRecord!=null)
                    listEvents.add(new BEvent(EventErrorAtStore, exceptionDuringRecord, "properties name;[" + mName + "]"));


            }
            else
            {

                for (final String resourceName : mAllProperties.keySet())
                {
                    exceptionDuringRecord = insertSql(con, resourceName, mDomainName, mAllProperties.get(resourceName));
                    if (exceptionDuringRecord != null)
                    {
                    	listEvents.add(new BEvent(EventErrorAtStore, exceptionDuringRecord, "properties name;[" + mName + "]"));
                        break;
                    }
                }
            }
            // maybe the database are in autocommit mode ? In that situation, do not commit
            if (con.getAutoCommit()) {
                logger.info(loggerLabel+" Database are in Autocommit");
            } else
            {
                if (exceptionDuringRecord != null)
                {                	
                    logger.info(loggerLabel+" Roolback");
                    con.rollback();
                } else {
                    con.commit();
                }
            }

        } catch (final Exception e)
        {
            final StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            final String exceptionDetails = sw.toString();
            logger.severe("Error during store properties [" + mName + "] sqlRequest[" + sqlRequest + "] : " + e.toString() + " : " + exceptionDetails);

            listEvents.add(new BEvent(EventErrorAtStore, e, "properties name;[" + mName + "]"));

        }
        finally
        {
            if (stmt != null) {
                try
                {
                    stmt.close();
                    stmt = null;
                }
                catch (final SQLException e1)
                {
                }
            }

            if (con != null) {
                try
                {
                    con.close();
                    con = null;
                }
                catch (final SQLException e1)
                {
                }
            }
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


    /* ******************************************************************************** */
    /*                                                                                                                                                                  */
    /*  Administration */
    /*                                                                                                                                      */
    /*                                                                                  */
    /* ******************************************************************************** */
    public void traceInLog()
    {
    	logger.info(loggerLabel+"trace()");
    	if (mAllProperties!=null)
	    	for (String domaineName : mAllProperties.keySet())
	    	{
	    		Map<String,Object> subProperties = mAllProperties.get( domaineName );
	        	for (String key : mAllProperties.keySet())
	        	{
	        		Object value= subProperties.get(key);
	    	
	        		logger.info(loggerLabel+" allProperties["+domaineName+"] .key("+key+") value=["+(value==null? null : (value.toString().length()>10 ? value.toString().substring(0,10)+"...":value))+"]");
	        	}
	    	}
    	
    	 for (Object key : this.keySet() )
         {
    		 String value= this.getProperty(key.toString());
     		logger.info(loggerLabel+" key("+key+") value=["+(value==null? null : (value.toString().length()>10 ? value.toString().substring(0,10)+"...":value))+"]");
         }
    		
    }
    
    
    /**
     * return all the properties. This is for administration, when the load is done without any domain.
     *
     * @return first level is the Resource name, second is all values in this resource.
     *         In case of domain, the value is a Map
     */
    public Hashtable<String, Hashtable<String, Object>> getAllProperties()
    {
        return mAllProperties;
    }

    /* ******************************************************************************** */
    /*                                                                                                                                                                  */
    /* Database  */
    /*                                                                                                                                      */
    /*                                                                                  */
    /* ******************************************************************************** */

    /**
     * check the database to verify that the database is accessible and the table too
     *
     * @return
     */
    public List<BEvent> checkDatabase()
    {
        final List<BEvent> listEvents = new ArrayList<BEvent>();
        Connection con = null;
        try
        {
            final Context ctx = new InitialContext();

            final DataSource dataSource = (DataSource) ctx.lookup(sqlDataSourceName);
            con = dataSource.getConnection();

            listEvents.addAll(checkCreateDatase(con));
        }
        catch (final Exception e)
        {
            final StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            final String exceptionDetails = sw.toString();
            logger.severe("Error during checkCreateDatase [" + exceptionDetails + "]");

            listEvents.add(new BEvent(EventErrorAtStore, e, "properties name;[" + mName + "]"));
        }
        finally
        {
            if (con != null)
            {
                try
                {
                    con.close();
                    con = null;
                } catch (final SQLException localSQLException)
                {
                }
            }
        }
        return listEvents;
    }

    /**
     *
     * @param con
     * @param resourceName
     * @param domainName
     * @param record
     * @return
     */
    private Exception insertSql(Connection con, final String resourceName, final String domainName, final Hashtable record /*
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
        //------------- prepare the data
        List<ItemKeyValue> listItems = new ArrayList<ItemKeyValue>();
        for (final Object key : record.keySet()) {
            final ItemKeyValue itemKeyValue = ItemKeyValue.getInstance(mTenantId, resourceName, domainName, key.toString(),
                    record.get(key) == null ? null : record.get(key).toString());
            listItems.add(itemKeyValue);
        }
        // decompose
        listItems = decomposeTooLargekey(listItems);

        final String sqlRequest = "insert into " + cstSqlTableName + " ("
                + cstSqlTenantId + ","
                + cstSqlResourceName
                + "," + cstSqldomainName
                + "," + cstSqlPropertiesKey
                + "," + cstSqlPropertiesValue
                + ") VALUES( ?, ?,?,?,?)";
        String whatToLog = "prepareStatement";
        PreparedStatement pstmt = null;
        try
        {
            pstmt = con.prepareStatement(sqlRequest);
            for (final ItemKeyValue itemKeyValue : listItems) {
                whatToLog = "values [" + cstSqlResourceName + ":" + itemKeyValue.rsResourceName + "] "
                        + "," + cstSqldomainName + ":" + itemKeyValue.rsDomainName
                        + "," + cstSqlPropertiesKey + ":" + itemKeyValue.rsKey
                        + "," + cstSqlPropertiesValue + ":" + itemKeyValue.rsValue + "]";

                pstmt.setLong(1, mTenantId);
                pstmt.setString(2, itemKeyValue.rsResourceName);
                pstmt.setString(3, itemKeyValue.rsDomainName);
                pstmt.setString(4, itemKeyValue.rsKey);

                pstmt.setString(5, itemKeyValue.rsValue);
                pstmt.executeUpdate();

            }

        } catch (final SQLException e)
        {
            final StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            final String exceptionDetails = sw.toString();
            logger.info("insertSql[" + whatToLog + "] at " + exceptionDetails);

            return e;
        }
        finally
        {
            if (pstmt != null)
            {
                try
                {
                    pstmt.close();
                    pstmt = null;
                }
                catch (final Exception e2) {
                }
            }
            if (con != null)
            {
                try
                {
                    con.close();
                    con = null;
                }
                catch (final Exception e2) {
                }
            }
        }
        return null;
    }

    /* ******************************************************************************** */
    /*                                                                                                                                                                  */
    /* Database Administration */
    /*                                                                                                                                      */
    /*                                                                                  */
    /* ******************************************************************************** */

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
                listColsExpected.put(cstSqlTenantId.toLowerCase(), -1);
                listColsExpected.put(cstSqlResourceName.toLowerCase(), 200);
                listColsExpected.put(cstSqldomainName.toLowerCase(), 500);
                listColsExpected.put(cstSqlPropertiesKey.toLowerCase(), 200);
                listColsExpected.put(cstSqlPropertiesValue.toLowerCase(), cstSqlPropertiesValueLength);

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
                    if (cstSqlTenantId.equalsIgnoreCase(col)) {
                        executeAlterSql(con, "update  " + cstSqlTableName + " set " + cstSqlTenantId + "=1");
                    }

                }
                // all change operation
                for (final String col : alterCols.keySet())
                {
                    executeAlterSql(con, "alter table " + cstSqlTableName + " alter column " + getSqlField(col, alterCols.get(col), databaseProductName));
                }
                logger.info(loggerLabel+"CheckCreateTable [" + cstSqlTableName + "] : Correct ");
            }
            else
            {
                logger.info(loggerLabel+"CheckCreateTable [" + cstSqlTableName + "] : NOT EXIST : create it");
                // create the table
                final String createTableString = "create table " + cstSqlTableName + " ("
                        + getSqlField(cstSqlTenantId, -1, databaseProductName) + ", "
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
            logger.severe(loggerLabel+"CheckCreateTableError during checkCreateDatase properties [" + mName + "] : " + e.toString() + " : " + exceptionDetails);
            listEvents.add(new BEvent(EventCreationDatabase, e, "properties name;[" + mName + "]"));

        }
        return listEvents;
    }

    private void executeAlterSql(final Connection con, final String sqlRequest) throws SQLException
    {
        logger.info(loggerLabel+"executeAlterSql : Execute [" + sqlRequest + "]");

        final Statement stmt = con.createStatement();
        stmt.executeUpdate(sqlRequest);

        if (!con.getAutoCommit())
        {
            con.commit();
        }

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
        if (colSize == -1)
        {
            // long
            if ("oracle".equalsIgnoreCase(databaseProductName)) {
                return colName + " NUMBER ";
            } else if ("PostgreSQL".equalsIgnoreCase(databaseProductName)) {
                return colName + " BIGINT";
            } else if ("H2".equalsIgnoreCase(databaseProductName)) {
                return colName + "   BIGINT";
            }
            return colName + "   BIGINT";
        }

        // varchar
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

    /* ******************************************************************************** */
    /*                                                                                                                                                                  */
    /* Able to save big data (more than the database size) */
    /*                                                                                                                                      */
    /*                                                                                  */
    /* ******************************************************************************** */

    /**
     * @param collectTooLargeKey
     * @return
     */
    protected List<ItemKeyValue> recomposeTooLargekey(final List<ItemKeyValue> collectTooLargeKey)
    {
        // first, reorder the list
        for (final ItemKeyValue itemKeyValue : collectTooLargeKey)
        {
            final int pos = itemKeyValue.rsKey.indexOf(cstMarkerSplitTooLargeKey);
            if (pos == -1)
            {
                itemKeyValue.baseKey = itemKeyValue.rsKey;
                itemKeyValue.numKey = 0;
            }
            else
            {
            itemKeyValue.baseKey = itemKeyValue.rsKey.substring(0, pos);
            itemKeyValue.numKey = Integer.valueOf(itemKeyValue.rsKey.substring(pos + cstMarkerSplitTooLargeKey.length()));
            }
        }
        Collections.sort(collectTooLargeKey, new Comparator<ItemKeyValue>()
        {
            @Override
            public int compare(final ItemKeyValue s1, final ItemKeyValue s2)
            {
                if (s1.baseKey.equals(s2.baseKey)) {
                    return Integer.compare(s1.numKey, s2.numKey);
                }
                return s1.baseKey.compareTo(s2.baseKey);
            }
        });

        // Now, complete
        ItemKeyValue lastKeyValue = null;
        final List<ItemKeyValue> collapseTooLargeKey = new ArrayList<ItemKeyValue>();
        for (final ItemKeyValue itemKeyValue : collectTooLargeKey)
        {
            if (lastKeyValue == null || !lastKeyValue.baseKey.equals(itemKeyValue.baseKey))
            {
                lastKeyValue = itemKeyValue;
                lastKeyValue.rsKey = lastKeyValue.baseKey;
                collapseTooLargeKey.add(lastKeyValue);
            } else {
                lastKeyValue.rsValue += itemKeyValue.rsValue;
            }

        }
        return collapseTooLargeKey;
    }

    /**
     * decompose
     *
     * @param collectTooLargeKey
     * @return
     */
    protected List<ItemKeyValue> decomposeTooLargekey(final List<ItemKeyValue> collectTooLargeKey)
    {
        final List<ItemKeyValue> newCollectTooLargeKey = new ArrayList<ItemKeyValue>();
        for (final ItemKeyValue itemKeyValue : collectTooLargeKey)
        {
            if (itemKeyValue.rsValue == null || itemKeyValue.rsValue.length() < cstSqlPropertiesValueLength) {
                newCollectTooLargeKey.add(itemKeyValue);
            } else
            {
            	logger.info(loggerLabel+"decomposeTooLargeKey["+itemKeyValue.rsKey+"] : size=["+itemKeyValue.rsValue.length()+"]");
                // too large : create multiple value
                String value = itemKeyValue.rsValue;
                int count = 0;
                while (value.length() > 0)
                {
                    final ItemKeyValue partialKeyValue = ItemKeyValue.getInstance(itemKeyValue);
                    partialKeyValue.rsKey = itemKeyValue.rsKey + cstMarkerSplitTooLargeKey + count;
                    partialKeyValue.rsValue = value.length() > cstSqlPropertiesValueLength - 1 ? value.substring(0, cstSqlPropertiesValueLength - 1) : value;
                    newCollectTooLargeKey.add(partialKeyValue);

                    value = value.length() > cstSqlPropertiesValueLength - 1 ? value.substring(cstSqlPropertiesValueLength - 1) : "";
                    count++;
                }
            }
        }
        return newCollectTooLargeKey;
    }

}
