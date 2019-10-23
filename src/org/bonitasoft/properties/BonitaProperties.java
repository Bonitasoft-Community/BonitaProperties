package org.bonitasoft.properties;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.log.event.BEvent.Level;
import org.bonitasoft.log.event.BEventFactory;
import org.bonitasoft.web.extension.page.PageResourceProvider;

@SuppressWarnings("serial")
/**
 * 
 * BonitaProperties extends Properties except for Stream argument.
 * Using the stream argument (to save document for example), use only the method get / set
 *
 */
public class BonitaProperties extends Properties {

    private static Logger logger = Logger.getLogger(BonitaProperties.class.getName());

    private static BEvent EventErrorAtLoad = new BEvent("org.bonitasoft.ext.properties", 1, Level.ERROR,
            "Error during loading properties", "Check Exception ",
            "The properties are empty", "Check Exception");

    private static BEvent EventCreationDatabase = new BEvent("org.bonitasoft.ext.properties", 2, Level.ERROR,
            "Error during creation the table in the database", "Check Exception ",
            "The properties will not work (no read, no save)", "Check Exception");

    private static BEvent EventErrorAtStore = new BEvent("org.bonitasoft.ext.properties", 3, Level.ERROR,
            "Error during saving the table in the database",
            "Check Exception ",
            "The properties are not saved", "Check Exception");

    private final String loggerLabel = "BonitaProperties_2.1.0:";
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

    private java.util.logging.Level logLevel = java.util.logging.Level.FINE;
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
    /** use hastable to be thread safe*/
    private Hashtable<String, Hashtable<String, Object>> mAllProperties;

   
    private Hashtable<String, InputStream> mPropertiesStream = new Hashtable<String,InputStream>();
    /** we mark in this set all key which change (delete, update, insert). Then, at update, we can do the correct change, not all.
     * 
     */
    private HashSet<String> mMarkPropertiesStreamToUpdate = new HashSet<String>();
    
    private String[] listDataSources = new String[] { "java:/comp/env/bonitaSequenceManagerDS", // tomcat
            "java:jboss/datasources/bonitaSequenceManagerDS" }; // jboss 

    /**
     * save the datasource used
     */
    private String sqlDataSourceName;
    // java:/comp/env/bonitaSequenceManagerDS

    private final static String cstSqlTableName = "bonitaproperties";
    private final static String cstSqlResourceName = "resourcename";
    private final static String cstSqldomainName = "domainname";
    private final static String cstSqlTenantId = "tenantId";

    private final static String cstSqlPropertiesKey = "propkey";
    private final static String cstSqlPropertiesValue = "propvalue";
    private final static String cstSqlPropertiesStream = "propstream";
    
    /**
     * size is set under 4000 due to an oracle limitation of varchar to 4000
     */
    private final static int cstSqlPropertiesValueLengthDatabase = 3500;
    private final static int cstSqlPropertiesValueLength = 3500;

    private boolean checkDatabaseAtFirstAccess = true;

    /**
     * If the string is too big, it is decompose in multiple value in the database.
     * Note: 
     *    - its compatible ascendant. Then, a key may be read in decomposed, then saved after in Stream
     *    - to use the store( Collection ), the mechanism must be set to true
     */
    private boolean policyBigStringToStream=false;
    
  

    /**
     * when you want to access the BonitaProperties from a Page, you have a PageResourceProvider.
     *
     * @param pageResourceProvider
     */
    public BonitaProperties(final PageResourceProvider pageResourceProvider) {
        mName = pageResourceProvider.getPageName();
        mTenantId = 1L;
    }

    public BonitaProperties(final PageResourceProvider pageResourceProvider, final long tenantId) {
        mName = pageResourceProvider.getPageName();
        mTenantId = tenantId;
    }

    public BonitaProperties(final String name) {
        mName = name;
        mTenantId = 1L;
    }

    public BonitaProperties(final String name, final long tenantId) {
        mName = name;
        mTenantId = tenantId;
    }

    public static BonitaProperties getAdminInstance() {
        final BonitaProperties bonitaProperties = new BonitaProperties((String) null);
        bonitaProperties.mLoadAllDomains = true;
        bonitaProperties.mTenantId = 1L;
        return bonitaProperties;
    }

    public static BonitaProperties getAdminInstance(final long tenantId) {
        final BonitaProperties bonitaProperties = new BonitaProperties((String) null);
        bonitaProperties.mLoadAllDomains = true;
        bonitaProperties.mTenantId = tenantId;

        return bonitaProperties;
    }

    public List<BEvent> load() {
        return loaddomainName(null);
    }

    // the constant is public then when a list of key is visible, the call know is this is a split key
    public final static String cstMarkerSplitTooLargeKey = "_#~_";
    public final static String cstMarkerTooLargeToStream = "_##TooLargeToStream~_";

    /**
     * load all the properties informations.
     * Attention, the checkDatabaseAtFirstAccess is use to check the database structure. If you don't want that and get fast access, 
     */
    public List<BEvent> loaddomainName(final String domainName) {
        // logger.fine("BonitaProperties.loadDomainName [" + domainName + "] CheckDatabase[" + checkDatabaseAtFirstAccess + "]");
        mDomainName = domainName;
        Connection con = null;
        final boolean originCheckDatabaseAtFirstAccess = checkDatabaseAtFirstAccess;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        final List<BEvent> listEvents = new ArrayList<BEvent>();
        try {
            final DataSource dataSource = getDataSourceConnection();
            if (dataSource == null)
                throw new Exception("No datasource available");

            con = dataSource.getConnection();
            if (checkDatabaseAtFirstAccess) {
                listEvents.addAll(checkCreateDatase(con));
            }
            checkDatabaseAtFirstAccess = false;
            if (BEventFactory.isError(listEvents)) {
                return listEvents;
            }
            String sqlRequest = "select * from bonitaproperties where (" + cstSqlTenantId + "= "
                    + (mTenantId == null ? (" 1 or " + cstSqlTenantId + " is null") : mTenantId);
            // be compatible with 1.3 or lower which continue to read / write in this table : the tenantId will be null then at each write
            // so if an application using 1.3 move to 1.4, it must continue to read the data
            sqlRequest += ")";

            final List<Object> listSqlParameters = new ArrayList<Object>();
            if (mName != null) {
                sqlRequest = sqlRequest + " and resourcename= ?";
                listSqlParameters.add(mName);
            }
            if (!mLoadAllDomains) {
                if (mDomainName == null) {
                    // protect the domain information
                    sqlRequest = sqlRequest + " and domainname is null";
                } else {
                    sqlRequest = sqlRequest + " and domainname= ?";
                    listSqlParameters.add(mDomainName);
                }
            }

            logger.log(logLevel,"sqlRequest[" + sqlRequest + "]");
            mPropertiesStream = new Hashtable<String, InputStream>();
            mMarkPropertiesStreamToUpdate=new HashSet<String>();
            
            pstmt = con.prepareStatement(sqlRequest);
            for (int i = 0; i < listSqlParameters.size(); i++) {
                pstmt.setObject(i + 1, listSqlParameters.get(i));
            }
            final List<ItemKeyValue> collectTooLargeKey = new ArrayList<ItemKeyValue>();
            int count = 0;
            rs = pstmt.executeQuery();
            while (rs.next()) {
                count++;

                final ItemKeyValue itemKeyValue = ItemKeyValue.getInstance(rs);

                // if the key contains the SplitTooLargeKey  then we collect it in a different map

                if (itemKeyValue.rsKey.contains(cstMarkerSplitTooLargeKey)) {
                    collectTooLargeKey.add(itemKeyValue);
                } else {
                    dispatchKeyAtLoading(itemKeyValue);
                }
            }

            // if the collectTooLargeKey is not empty, recompose the key
            final List<ItemKeyValue> recomposeKey = recomposeTooLargekey(collectTooLargeKey);
            for (final ItemKeyValue itemKeyValue : recomposeKey) {
                dispatchKeyAtLoading(itemKeyValue);
            }
            logger.log(logLevel,loggerLabel + ".Loadfrom  [" + mDomainName
                    + "] CheckDatabase[" + originCheckDatabaseAtFirstAccess
                    + "] name[" + mName
                    + "] " + (mName != null ? "*LoadProperties*" : "*LoadALLPROPERTIES*")
                    + " found[" + count + "]records sqlRequest[" + sqlRequest + "]");

            /*
             * if (mName != null) {
             * logger.fine("Load  Properties[" + mName + "] :  " + size() + " properties");
             * } else {
             * logger.fine("Load  AllProperties :  " + mAllProperties.size() + " properties");
             * }
             */
        } catch (final Exception e) {
            final StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            final String exceptionDetails = sw.toString();
            logger.severe(loggerLabel + ".loadDomainName Error during load properties [" + mName + "] : " + e.toString()
                    + " : " + exceptionDetails);

            listEvents.add(new BEvent(EventErrorAtLoad, e, "properties name;[" + mName + "]"));
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                    rs = null;
                } catch (final SQLException localSQLException) {
                }
            }
            if (pstmt != null) {
                try {
                    pstmt.close();
                    pstmt = null;
                } catch (final SQLException localSQLException) {
                }
            }
            if (con != null) {
                try {
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
    protected static class ItemKeyValue {

        public long rsTenantId;
        public String rsResourceName;
        public String rsDomainName;
        public String rsKey;
        public String rsValue;
        // save document
        public InputStream rsStream;

        /** theses fields are used to recompose the key from a split key */
        public String baseKey;
        public int numKey;

        public static ItemKeyValue getInstance(final long tenantId, final String resourceName, final String domainName,
                final String key, final String value) {
            final ItemKeyValue itemKeyValue = new ItemKeyValue();
            itemKeyValue.rsTenantId = tenantId;
            itemKeyValue.rsResourceName = resourceName;
            itemKeyValue.rsDomainName = domainName;
            itemKeyValue.rsKey = key;
            itemKeyValue.rsValue = value;
            itemKeyValue.rsStream = null;
            return itemKeyValue;
        }
        public static ItemKeyValue getInstanceStream(final long tenantId, final String resourceName, final String domainName,
            final String key, final InputStream valueStream) {
        final ItemKeyValue itemKeyValue = new ItemKeyValue();
        itemKeyValue.rsTenantId = tenantId;
        itemKeyValue.rsResourceName = resourceName;
        itemKeyValue.rsDomainName = domainName;
        itemKeyValue.rsKey = key;
        itemKeyValue.rsStream = valueStream;
        return itemKeyValue;
    }

        public static ItemKeyValue getInstance(final ItemKeyValue source) {
            final ItemKeyValue itemKeyValue = new ItemKeyValue();
            itemKeyValue.rsTenantId     = source.rsTenantId;
            itemKeyValue.rsResourceName = source.rsResourceName;
            itemKeyValue.rsDomainName   = source.rsDomainName;
            itemKeyValue.rsKey          = source.rsKey;
            itemKeyValue.rsValue        = source.rsValue;
            itemKeyValue.rsStream       = source.rsStream;
            return itemKeyValue;
        }

        public static ItemKeyValue getInstance(final ResultSet rs) throws SQLException {
            final ItemKeyValue itemKeyValue = new ItemKeyValue();
            itemKeyValue.rsTenantId       = rs.getLong(cstSqlTenantId);
            itemKeyValue.rsResourceName   = rs.getString(cstSqlResourceName);
            itemKeyValue.rsDomainName     = rs.getString(cstSqldomainName);
            itemKeyValue.rsKey            = rs.getString(cstSqlPropertiesKey);
            itemKeyValue.rsValue          = rs.getString(cstSqlPropertiesValue);
            itemKeyValue.rsStream         = rs.getBinaryStream(cstSqlPropertiesStream);
            
            return itemKeyValue;
        }
    }

    /**
     * Dispath a Bonita Properties value in multiple properties, in order to save it
     * @param itemKeyValue
     */
    private void dispatchKeyAtLoading(final ItemKeyValue itemKeyValue) {
        if (mName != null) {
            if (cstMarkerTooLargeToStream.equals(itemKeyValue.rsValue))
            {               
                setProperty(itemKeyValue.rsKey, streamToString( itemKeyValue.rsStream ));
            }
         else if (itemKeyValue.rsStream!=null)
            mPropertiesStream.put(itemKeyValue.rsKey, itemKeyValue.rsStream);
          else if (itemKeyValue.rsValue !=null)
            setProperty(itemKeyValue.rsKey, itemKeyValue.rsValue);
        } else {
            if (mAllProperties == null) {
                mAllProperties = new Hashtable<String, Hashtable<String, Object>>();
            }
            Hashtable<String, Object> mapName = mAllProperties.get(itemKeyValue.rsResourceName);
            if (mapName == null) {
                mapName = new Hashtable<String, Object>();
                mAllProperties.put(itemKeyValue.rsResourceName, mapName);
            }
            if (itemKeyValue.rsDomainName != null) {
                @SuppressWarnings("unchecked")
                Hashtable<String, String> mapDomain = (Hashtable<String, String>) mapName.get(itemKeyValue.rsDomainName);
                if (mapDomain == null) {
                    mapDomain = new Hashtable<String, String>();
                    mapName.put(itemKeyValue.rsDomainName, mapDomain);
                }
                // don't save the stream in the MapDomain for the moment
                if (itemKeyValue.rsStream!=null)
                  mapDomain.put(itemKeyValue.rsKey, "Stream");
                else if (itemKeyValue.rsValue!=null)
                  mapDomain.put(itemKeyValue.rsKey, itemKeyValue.rsValue);
            } else {
              if (itemKeyValue.rsStream!=null)
                mapName.put(itemKeyValue.rsKey, "stream");
              else if (itemKeyValue.rsValue!=null)
              mapName.put(itemKeyValue.rsKey, itemKeyValue.rsValue);
            }
        }
    }

    
    /* ******************************************************************************** */
    /*                                                                                  */
    /* Special getter/setter                                                            */
    /*                                                                                  */
    /*                                                                                  */
    /* ******************************************************************************** */
    public synchronized Object setPropertyStream(String key, InputStream value) {
        mMarkPropertiesStreamToUpdate.add( key );
      if (value==null) {
        mPropertiesStream.remove(key);
        return null;
      }
      else
        return mPropertiesStream.put( key,  value);
    }
    
    public InputStream getPropertyStream(String key) {
        return mPropertiesStream.get( key);
    }
    public void removeStream(String key ) {
        mMarkPropertiesStreamToUpdate.add( key );
        mPropertiesStream.remove( key );
    }
    /**
     * Attention : the enumerate may be large, so this is a method directly on the private object, not a copy
     * @return
     */
    public Enumeration<String> propertyStream() {
        return mPropertiesStream.keys();
    }
    /**
     * save all properties informations
     * ATTENTION : the current information are purge and replace with the new one. This is not a merge, so if the application want to do an update, a first
     * load() has to be done
     */
    public List<BEvent> store() {
        logger.log(logLevel, loggerLabel + "store()");

        return storeCollectionKeys( null );
    }
    /**
     * only save the list of key. That's mean if you delete the key, you can give it in the collection, and properties does not have a new value then.
     * @param listLimitedKeys
     * @return
     */
    public List<BEvent> storeCollectionKeys( Collection<String> listLimitedKeys)
    {
        final List<BEvent> listEvents = new ArrayList<BEvent>();
        logger.log(logLevel,loggerLabel + "storeCollectionKeys()");

        Statement stmt = null;
        String sqlRequest = null;
        Connection con = null;
        try {
            final DataSource dataSource = getDataSourceConnection();
            if (dataSource == null)
                throw new Exception("No datasource available");
            
            
            // attention: if there are still in the propertiesStream some Blob read from the database, it's important to REPLACE them now by a different inputStream, else the read/write will failed
           
            con = dataSource.getConnection();

            sqlRequest = "delete from " + cstSqlTableName + " where  " + cstSqlTenantId + "= "
                    + (mTenantId == null ? 1 : mTenantId);
            if (mName != null) {
                sqlRequest += " and " + cstSqlResourceName + "= '" + mName + "'";
            }
            if (mDomainName == null) {
                // protect the domain
                sqlRequest += " and " + cstSqldomainName + " is null";
            } else {
                sqlRequest += " and " + cstSqldomainName + "= '" + mDomainName + "'";
            }
            String baseRequest = sqlRequest;
            
            // be smart : does not delete the stream, only on change.
            sqlRequest +=" and "+cstSqlPropertiesStream +" is null";
            
            if (listLimitedKeys!=null)
            {
                String filter="";
                for (String key : listLimitedKeys)
                {
                    if (filter.length()>0)
                        filter+=" or ";
                    filter+=cstSqlPropertiesKey+" = '"+key+"' or "+cstSqlPropertiesKey+" like '"+key+cstMarkerSplitTooLargeKey+"%'";
                    
                }
                sqlRequest += " and ("+ filter+")";
            }
            /*
            boolean checkToDelete=false;
            if (listLimitedKeys == null )
                checkToDelete=true;
            else
            {
                for(String key : listLimitedKeys)
                    if (this.get( key ) ==null)
                        logger.info(loggerLabel+"We delete the key "+key);
            }
              */
            
            // now, purge all Stream marked
            stmt = con.createStatement();

            logger.log(logLevel,loggerLabel + "Purge with [" + sqlRequest + "]");
            stmt.executeUpdate(sqlRequest);

            
            // now purge all stream marked
            sqlRequest = baseRequest+" and "+cstSqlPropertiesKey+" in (";
            String listKeysStream="";
            for (String key : mMarkPropertiesStreamToUpdate)
            {
                listKeysStream+=", '"+key+"' ";
            }
            if (listKeysStream.length() > 0)
            {
                listKeysStream = listKeysStream.substring(1);
                sqlRequest += listKeysStream+")";
                logger.log(logLevel,loggerLabel + "Purge Stream with [" + sqlRequest + "]");                
                stmt.executeUpdate(sqlRequest);
            }
                        
            // now create all records
            Exception exceptionDuringRecord = null;
            if (mName != null) {
              // save the properties AND the mAllProperties which contains Stream
              exceptionDuringRecord = insertSql(con, mName, mDomainName, null, null, this, listLimitedKeys );
              if (exceptionDuringRecord != null)
                listEvents.add(
                        new BEvent(EventErrorAtStore, exceptionDuringRecord, "properties name;[" + mName + "]"));
              

                exceptionDuringRecord = insertSql(con, mName, mDomainName, mPropertiesStream, mMarkPropertiesStreamToUpdate, null,listLimitedKeys);
                if (exceptionDuringRecord != null)
                    listEvents.add(
                            new BEvent(EventErrorAtStore, exceptionDuringRecord, "properties name;[" + mName + "]"));

            } else {
                for (final String resourceName : mAllProperties.keySet()) {
                    exceptionDuringRecord = insertSql(con, resourceName, mDomainName, null, null, mAllProperties.get(resourceName),listLimitedKeys);
                    if (exceptionDuringRecord != null) {
                        listEvents.add(new BEvent(EventErrorAtStore, exceptionDuringRecord,
                                "properties name;[" + mName + "]"));
                        break;
                    }
                }
            }
            // maybe the database are in autocommit mode ? In that situation, do not commit
            if (con.getAutoCommit()) {
                logger.log(logLevel,loggerLabel + " Database are in Autocommit");
            } else {
                if (exceptionDuringRecord != null) {
                    logger.log(logLevel,loggerLabel + " Rollback");
                    con.rollback();
                } else {
                    con.commit();
                }
            }

        } catch (final Exception e) {
            final StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            final String exceptionDetails = sw.toString();
            logger.severe("Error during store properties [" + mName + "] sqlRequest[" + sqlRequest + "] : "
                    + e.toString() + " : " + exceptionDetails);

            listEvents.add(new BEvent(EventErrorAtStore, e, "properties name;[" + mName + "]"));

        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                    stmt = null;
                } catch (final SQLException e1) {
                }
            }

            if (con != null) {
                try {
                    con.close();
                    con = null;
                } catch (final SQLException e1) {
                }
            }
        }
        return listEvents;
    }

    public void setDataSource(final String dataSource) {
        sqlDataSourceName = dataSource;
    }

    public String getDataSource() {
        return sqlDataSourceName;
    }

    public boolean getCheckDatabase() {
        return checkDatabaseAtFirstAccess;
    }

    public void setCheckDatabase(final boolean checkDatabaseAtFirstAccess) {
        this.checkDatabaseAtFirstAccess = checkDatabaseAtFirstAccess;
    }
    
    public boolean getPolicyBigStringToStream() {
        return policyBigStringToStream;
    }

    
    public void setPolicyBigStringToStream(boolean policyBigStringToStream) {
        this.policyBigStringToStream = policyBigStringToStream;
    }
    public void setLogLevel( java.util.logging.Level logLevel ) 
    {
        this.logLevel = logLevel;
    }
    /* ******************************************************************************** */
    /*                                                                                                                                                                  */
    /* Administration */
    /*                                                                                                                                      */
    /*                                                                                  */
    /* ******************************************************************************** */
    public void traceInLog() {
        logger.log(logLevel, loggerLabel + "trace()");
        if (mAllProperties != null)
            for (String domaineName : mAllProperties.keySet()) {
                Map<String, Object> subProperties = mAllProperties.get(domaineName);
                for (String key : mAllProperties.keySet()) {
                    Object value = subProperties.get(key);

                    logger.log(logLevel,loggerLabel + " allProperties[" + domaineName + "] .key(" + key + ") value=["
                            + (value == null ? null : (value.toString().length() > 10
                                    ? value.toString().substring(0, 10) + "..." : value))
                            + "]");
                }
            }

        for (Object key : this.keySet()) {
            String value = this.getProperty(key.toString());
            logger.log(logLevel,loggerLabel + " key(" + key + ") value=["
                    + (value == null ? null
                            : (value.toString().length() > 10 ? value.toString().substring(0, 10) + "..." : value))
                    + "]");
        }

    }

    /**
     * return all the properties. This is for administration, when the load is done without any domain.
     *
     * @return first level is the Resource name, second is all values in this resource.
     *         In case of domain, the value is a Map
     */
    public Hashtable<String, Hashtable<String, Object>> getAllProperties() {
        return mAllProperties;
    }

    /* ******************************************************************************** */
    /*                                                                                                                                                                  */
    /* Database */
    /*                                                                                                                                      */
    /*                                                                                  */
    /* ******************************************************************************** */

    /**
     * check the database to verify that the database is accessible and the table too
     *
     * @return
     */
    public List<BEvent> checkDatabase() {
        List<BEvent> listEvents = new ArrayList<BEvent>();
        Connection con = null;
        try {
            final DataSource dataSource = getDataSourceConnection();
            if (dataSource == null)
                throw new Exception("No datasource available");
            con = dataSource.getConnection();

            listEvents.addAll(checkCreateDatase(con));
        } catch (final Exception e) {
            final StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            final String exceptionDetails = sw.toString();
            logger.severe("Error during checkCreateDatase [" + exceptionDetails + "]");

            listEvents.add(new BEvent(EventErrorAtStore, e, "properties name;[" + mName + "]"));
        } finally {
            if (con != null) {
                try {
                    con.close();
                    con = null;
                } catch (final SQLException localSQLException) {
                }
            }
        }
        return listEvents;
    }

    /**
     * @param con give a connection, and this connection will not be closed
     * @param resourceName
     * @param domainName
     * @param record
     * @return
     */
    private Exception insertSql(Connection con, 
        final String resourceName, 
        final String domainName,
        final Hashtable<String,InputStream> property,
        final HashSet<String> markPropertiesStreamToUpdate,
        @SuppressWarnings("rawtypes") final Hashtable record,
        Collection<String> listLimitedKeys)

    {
        /*
         * final String sqlRequest = "insert into " + sqlTableName + " (" +
         * sqlResourceName + "," + sqldomainName + "," + sqlPropertiesKey + "," + sqlPropertiesValue
         * + ") VALUES('" + resourceName + "','" + domainName + "','" + key + "','" + value + "')";
         */
        //------------- prepare the data
        List<ItemKeyValue> listItems = new ArrayList<ItemKeyValue>();
        if (property!=null)
        {
            
          for (final Object key : property.keySet()) {
            if (property.get( key ) ==null)
              continue;
            // only if this is part of the streamToUpdate
            if (markPropertiesStreamToUpdate.contains( key ))
            {
              final ItemKeyValue itemKeyValue = ItemKeyValue.getInstanceStream(mTenantId, resourceName, domainName,
                    key.toString(),
                    property.get(key));
              listItems.add(itemKeyValue);
            }
          }
        }
        if (record!=null)
        {
          for (final Object key : record.keySet()) {
            if (record.get( key ) ==null)
              continue;
              final ItemKeyValue itemKeyValue = ItemKeyValue.getInstance(mTenantId, resourceName, domainName,
                    key.toString(),
                    record.get(key).toString());
              listItems.add(itemKeyValue);
            }
        }
        
        
        // decompose
        listItems = decomposeTooLargekey(listItems);

        final String sqlRequest = "insert into " + cstSqlTableName + " ("
                + cstSqlTenantId + ","
                + cstSqlResourceName
                + "," + cstSqldomainName
                + "," + cstSqlPropertiesKey
                + "," + cstSqlPropertiesValue
                + "," + cstSqlPropertiesStream
                + ") VALUES( ?, ?,?,?,?,?)";
        String whatToLog = "prepareStatement";
        PreparedStatement pstmt = null;
        try {
            pstmt = con.prepareStatement(sqlRequest);
            for (final ItemKeyValue itemKeyValue : listItems) {
                if (listLimitedKeys!=null)
                {
                    boolean keyInList=false;
                    for (String key : listLimitedKeys)
                    {
                        if (itemKeyValue.rsKey.equals(key) || itemKeyValue.rsKey.startsWith(key+cstMarkerSplitTooLargeKey))
                            keyInList=true;   
                    }
                    // do not save the itemKeyValue if the jey is not in the list
                    if (! keyInList)
                        continue;
                }
                
                whatToLog = "values [" + cstSqlResourceName + ":" + itemKeyValue.rsResourceName + "] "
                        + "," + cstSqldomainName + ":" + itemKeyValue.rsDomainName
                        + "," + cstSqlPropertiesKey + ":" + itemKeyValue.rsKey
                        + "," + cstSqlPropertiesValue + ":" + itemKeyValue.rsValue
                        + "," + cstSqlPropertiesStream + ":" + (itemKeyValue.rsStream ==null ? "null" : "xx") 
                        + "]";

                pstmt.setLong(1, mTenantId == null ? 1L : mTenantId);
                pstmt.setString(2, itemKeyValue.rsResourceName);
                pstmt.setString(3, itemKeyValue.rsDomainName);
                pstmt.setString(4, itemKeyValue.rsKey);

                pstmt.setString(5, itemKeyValue.rsValue);
                pstmt.setBinaryStream(6, itemKeyValue.rsStream);
                try
                {
                    pstmt.executeUpdate();
                    logger.log(logLevel,loggerLabel+"Insert "+whatToLog);
                }
                catch(Exception e)
                {
                    logger.log(logLevel,loggerLabel+"UNIQUE INDEX VIOLATION : Insert "+whatToLog);
                    if (e.getMessage().startsWith("Unique index"))
                        continue;
                    throw e;
                }
            }

        } catch (final SQLException e) {
            final StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            final String exceptionDetails = sw.toString();
            logger.log(logLevel,"insertSql[" + whatToLog + "] at " + exceptionDetails);

            return e;
        } finally {
            if (pstmt != null) {
                try {
                    pstmt.close();
                    pstmt = null;
                } catch (final Exception e2) {
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
    private List<BEvent> checkCreateDatase(final Connection con) {

        final List<BEvent> listEvents = new ArrayList<BEvent>();

        try {
            final DatabaseMetaData dbm = con.getMetaData();

            final String databaseProductName = dbm.getDatabaseProductName();

            // check if "employee" table is there
            // nota: don't use the patern, it not give a correct result with H2
            final ResultSet tables = dbm.getTables(null, null, null, null);

            boolean exist = false;
            while (tables.next()) {
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
                listColsExpected.put(cstSqlPropertiesValue.toLowerCase(), cstSqlPropertiesValueLengthDatabase);
                listColsExpected.put(cstSqlPropertiesStream.toLowerCase(), -2);

                final Map<String, Integer> alterCols = new HashMap<String, Integer>();

                // Table exists : is the fields are correct ?
                final ResultSet rs = dbm.getColumns(null /* catalog */, null /* schema */, null /* cstSqlTableName */,
                        null /* columnNamePattern */);

                while (rs.next()) {
                    String tableNameCol = rs.getString("TABLE_NAME");
                    final String colName = rs.getString("COLUMN_NAME");
                    final int length = rs.getInt("COLUMN_SIZE");

                    tableNameCol = tableNameCol == null ? "" : tableNameCol.toLowerCase();

                    if (!tableNameCol.equalsIgnoreCase(cstSqlTableName)) {
                        continue;
                    }
                    // final int dataType = rs.getInt("DATA_TYPE");
                    final Integer expectedSize = listColsExpected.containsKey(colName.toLowerCase())
                            ? listColsExpected.get(colName.toLowerCase()) : null;
                    if (expectedSize == null) {
                        logger.log(logLevel,"Colum  [" + colName.toLowerCase() + "] : does not exist in [ " + listColsExpected
                                + "];");
                        continue; // this columns is new
                    }
                    if (length < expectedSize) {
                        logger.log(logLevel,"Colum  [" + colName.toLowerCase() + "] : length[" + length + "] expected["
                                + expectedSize + "]");
                        alterCols.put(colName.toLowerCase(), expectedSize);
                    }
                    listColsExpected.remove(colName.toLowerCase());
                    logger.log(logLevel,"Remove Colum  [" + colName.toLowerCase() + "] : list is now [ " + listColsExpected + "];");
                }
                // OK, create all missing column
                for (final String col : listColsExpected.keySet()) {
                    executeAlterSql(con, "alter table " + cstSqlTableName + " add  "
                            + getSqlField(col, listColsExpected.get(col), databaseProductName));
                    if (cstSqlTenantId.equalsIgnoreCase(col)) {
                        executeAlterSql(con, "update  " + cstSqlTableName + " set " + cstSqlTenantId + "=1");
                    }

                }
                // all change operation
                for (final String col : alterCols.keySet()) {
                    executeAlterSql(con, "alter table " + cstSqlTableName + " alter column "
                            + getSqlField(col, alterCols.get(col), databaseProductName));
                }
                logger.log(logLevel,loggerLabel + "CheckCreateTable [" + cstSqlTableName + "] : Correct ");
                // add the constraint
                /*
                String constraints = "alter table "+ cstSqlTableName + " add constraint uniq_propkey unique ("+
                        cstSqlTenantId+","
                        + cstSqlResourceName+","
                        + cstSqldomainName+","
                        + cstSqlPropertiesKey+")";
                 executeAlterSql(con, constraints);
                 */
            } else {
                // create the table
                final String createTableString = "create table " + cstSqlTableName + " ("
                        + getSqlField(cstSqlTenantId, -1, databaseProductName) + ", "
                        + getSqlField(cstSqlResourceName, 200, databaseProductName) + ", "
                        + getSqlField(cstSqldomainName, 500, databaseProductName) + ", "
                        + getSqlField(cstSqlPropertiesKey, 200, databaseProductName) + ", "
                        + getSqlField(cstSqlPropertiesValue, cstSqlPropertiesValueLengthDatabase, databaseProductName) +","
                        + getSqlField(cstSqlPropertiesStream, -2, databaseProductName) 
                        + ")";
                logger.log(logLevel,loggerLabel + "CheckCreateTable [" + cstSqlTableName + "] : NOT EXIST : create it with script["+createTableString+"]");
                executeAlterSql(con, createTableString);
                
                /* String constraints = "alter table "+ cstSqlTableName + " add constraint uniq_propkey unique ("+
                        cstSqlTenantId+","
                        + cstSqldomainName+","
                        + cstSqlResourceName+","
                        + cstSqlPropertiesKey+")";
                executeAlterSql(con, constraints);
                */

            }
        } catch (final SQLException e) {
            final StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            final String exceptionDetails = sw.toString();
            logger.severe(loggerLabel + "CheckCreateTableError during checkCreateDatase properties [" + mName + "] : "
                    + e.toString() + " : " + exceptionDetails);
            listEvents.add(new BEvent(EventCreationDatabase, e, "properties name;[" + mName + "]"));

        }
        return listEvents;
    }

    private void executeAlterSql(final Connection con, final String sqlRequest) throws SQLException {
        logger.log(logLevel,loggerLabel + "executeAlterSql : Execute [" + sqlRequest + "]");

        final Statement stmt = con.createStatement();
        stmt.executeUpdate(sqlRequest);

        if (!con.getAutoCommit()) {
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
        if (colSize == -1) {
            // long
            if ("oracle".equalsIgnoreCase(databaseProductName)) {
                return colName + " NUMBER ";
            } else if ("PostgreSQL".equalsIgnoreCase(databaseProductName)) {
                return colName + " BIGINT";
            } else if ("H2".equalsIgnoreCase(databaseProductName)) {
                return colName + " BIGINT";
            }
            return colName + " BIGINT";
        }
        if (colSize == -2) {
          // long
          if ("oracle".equalsIgnoreCase(databaseProductName)) {
              return colName + " BLOB ";
          } else if ("PostgreSQL".equalsIgnoreCase(databaseProductName)) {
              return colName + " blob bytea";
          } else if ("H2".equalsIgnoreCase(databaseProductName)) {
              return colName + " BLOB";
          }
          return colName + " BLOB";
      }
        // varchar
        if ("oracle".equalsIgnoreCase(databaseProductName)) {
            return colName + " VARCHAR2(" + colSize + ")";
        }
        if ("PostgreSQL".equalsIgnoreCase(databaseProductName)) {
            return colName + " varchar(" + colSize + ")"; // old varying
        } else if ("H2".equalsIgnoreCase(databaseProductName)) {
            return colName + " varchar(" + colSize + ")";
        } else {
            return colName + " varchar(" + colSize + ")";
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
    protected List<ItemKeyValue> recomposeTooLargekey(final List<ItemKeyValue> collectTooLargeKey) {
        // first, reorder the list
        for (final ItemKeyValue itemKeyValue : collectTooLargeKey) {
            final int pos = itemKeyValue.rsKey.indexOf(cstMarkerSplitTooLargeKey);
            if (pos == -1) {
                itemKeyValue.baseKey = itemKeyValue.rsKey;
                itemKeyValue.numKey = 0;
            } else {
                itemKeyValue.baseKey = itemKeyValue.rsKey.substring(0, pos);
                itemKeyValue.numKey = Integer
                        .valueOf(itemKeyValue.rsKey.substring(pos + cstMarkerSplitTooLargeKey.length()));
            }
        }
        Collections.sort(collectTooLargeKey, new Comparator<ItemKeyValue>() {

            @Override
            public int compare(final ItemKeyValue s1, final ItemKeyValue s2) {
                if (s1.baseKey.equals(s2.baseKey)) {
                    return Integer.compare(s1.numKey, s2.numKey);
                }
                return s1.baseKey.compareTo(s2.baseKey);
            }
        });

        // Now, complete
        String analyse="";
        ItemKeyValue lastKeyValue = null;
        final List<ItemKeyValue> collapseTooLargeKey = new ArrayList<ItemKeyValue>();
        for (final ItemKeyValue itemKeyValue : collectTooLargeKey) {
            // a new key ?
            if (lastKeyValue == null || !lastKeyValue.baseKey.equals(itemKeyValue.baseKey)) {
                if (analyse.length()>0)
                    logger.log(logLevel,"BonitaPropertiesMerge="+analyse);
                analyse= itemKeyValue.baseKey+"=["+itemKeyValue.rsValue+"],";
                lastKeyValue = itemKeyValue;
                lastKeyValue.rsKey = lastKeyValue.baseKey;
                collapseTooLargeKey.add(lastKeyValue);
            } else {
                lastKeyValue.rsValue += itemKeyValue.rsValue;
                analyse += "["+itemKeyValue.rsValue+"],";
            }

        }
        return collapseTooLargeKey;
    }

    /**
     * decompose
     *
     * @param collectTooLargeKey
     */
    protected List<ItemKeyValue> decomposeTooLargekey(final List<ItemKeyValue> collectTooLargeKey) {
        final List<ItemKeyValue> newCollectTooLargeKey = new ArrayList<ItemKeyValue>();
        for (final ItemKeyValue itemKeyValue : collectTooLargeKey) {
            if (itemKeyValue.rsValue == null || itemKeyValue.rsValue.length() < cstSqlPropertiesValueLength) {
                newCollectTooLargeKey.add(itemKeyValue);
            } else {
                logger.log(logLevel,loggerLabel + "decomposeTooLargeKey[" + itemKeyValue.rsKey + "] : size=["
                        + itemKeyValue.rsValue.length() + "]");
                // too large : create multiple value
                String value = itemKeyValue.rsValue;
                
                if (policyBigStringToStream) {
                    final ItemKeyValue streamKeyValue = ItemKeyValue.getInstance(itemKeyValue);
                    streamKeyValue.rsValue = cstMarkerTooLargeToStream;
                    streamKeyValue.rsStream = stringToStream(value);
                    newCollectTooLargeKey.add(streamKeyValue);
                }
                else
                {
                    // decompose in multiple key
                    String analyse="Orig["+value+"], result=";
                int count = 0;
                while (value.length() > 0) {
                    final ItemKeyValue partialKeyValue = ItemKeyValue.getInstance(itemKeyValue);
                    partialKeyValue.rsKey = itemKeyValue.rsKey + cstMarkerSplitTooLargeKey + count;
                    partialKeyValue.rsValue = value.length() > cstSqlPropertiesValueLength - 1
                            ? value.substring(0, cstSqlPropertiesValueLength - 1) : value;
                    newCollectTooLargeKey.add(partialKeyValue);

                    value = value.length() > cstSqlPropertiesValueLength - 1
                            ? value.substring(cstSqlPropertiesValueLength - 1) : "";
                    count++;
                    analyse+=partialKeyValue.rsKey+"=["+partialKeyValue.rsValue+"],";
                }
                logger.log(logLevel,"BonitaPropertiesSplit="+analyse);
                }
            }
        }
        return newCollectTooLargeKey;
    }

    private DataSource getDataSourceConnection() {
        // logger.fine(loggerLabel+".getDataSourceConnection() start");

        String msg = "";
        List<String> listDatasourceToCheck = new ArrayList<String>();
        if (sqlDataSourceName != null && sqlDataSourceName.trim().length() > 0)
            listDatasourceToCheck.add(sqlDataSourceName);
        for (String dataSourceString : listDataSources)
            listDatasourceToCheck.add(dataSourceString);

        for (String dataSourceString : listDatasourceToCheck) {
            // logger.fine(loggerLabel+".getDataSourceConnection() check["+dataSourceString+"]");
            try {
                final Context ctx = new InitialContext();
                final DataSource dataSource = (DataSource) ctx.lookup(dataSourceString);
                sqlDataSourceName = dataSourceString;
                // logger.fine(loggerLabel+".getDataSourceConnection() ["+dataSourceString+"] isOk");
                return dataSource;
            } catch (NamingException e) {
                // logger.fine(loggerLabel+".getDataSourceConnection() error["+dataSourceString+"] : "+e.toString());
                msg += "DataSource[" + dataSourceString + "] : error " + e.toString() + ";";
            }
        }
        logger.severe(loggerLabel + ".getDataSourceConnection: Can't found a datasource : " + msg);
        return null;
    }

    
    private String streamToString( InputStream stream) 
    {
        try
        {
        ByteArrayOutputStream result = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024];
        int length;
        while ((length = stream.read(buffer)) != -1) {
            result.write(buffer, 0, length);
        }
        return result.toString(StandardCharsets.UTF_8.name());
        }catch(Exception e) {
        return null;
        }
    }
    
    private InputStream stringToStream( String value) 
    {
        try
        {
                InputStream result = new ByteArrayInputStream(value.getBytes(StandardCharsets.UTF_8));
                return result;
       }catch(Exception e) {
        return null;
       }
    }
}
