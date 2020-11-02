package org.bonitasoft.properties;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.bonitasoft.log.event.BEvent;
/* ******************************************************************************** */
/*                                                                                                                                                                  */
/* Database Administration */
/*                                                                                                                                      */
/*                                                                                  */
/* ******************************************************************************** */

public class DatabaseOperation {
    private static Logger logger = Logger.getLogger(DatabaseOperation.class.getName());
    private final String loggerLabel = "BonitaProperties_2.6.1:";
    BonitaProperties bonitaProperties;
    
    private static final String CST_DRIVER_H2 = "H2";
    private static final String CST_DRIVER_ORACLE = "oracle";
    private static final String CST_DRIVER_POSTGRESQL = "PostgreSQL";
    private static final String CST_DRIVER_MYSQL = "MySQL";
    private static final String CST_DRIVER_SQLSERVER = "Microsoft SQL Server";
    
    protected DatabaseOperation( BonitaProperties bonitaProperties) {
        this.bonitaProperties = bonitaProperties;
    }
    protected List<BEvent> checkDatabase() {

        List<BEvent> listEvents = new ArrayList<>();
        Connection con=null;
        try {
            con = BonitaEngineConnection.getConnection();
            listEvents.addAll(checkCreateDatase(con));
        } catch (final Exception e) {
            final StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            final String exceptionDetails = sw.toString();
            logger.severe("Error during checkCreateDatase [" + exceptionDetails + "]");

            listEvents.add(new BEvent(BonitaProperties.eventErrorAtStore, e, "properties name;"));
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
     * check if the table exist; if not then create it
     *
     * @param con
     * @return
     */
    protected List<BEvent> checkCreateDatase(final Connection con) {

        final List<BEvent> listEvents = new ArrayList<>();
        StringBuilder logAnalysis = new StringBuilder();
        logAnalysis.append("CheckDatabase;");
        java.util.logging.Level logLevelAnalysis = java.util.logging.Level.FINE;
        if (bonitaProperties.logCheckDatabaseAtFirstAccess)
            logLevelAnalysis = java.util.logging.Level.INFO;

        try {
            final DatabaseMetaData dbm = con.getMetaData();

            final String databaseProductName = dbm.getDatabaseProductName();

            // check if "employee" table is there
            // nota: don't use the patern, it not give a correct result with H2
            final ResultSet tables = dbm.getTables(null, null, null, null);

            boolean exist = false;
            while (tables.next()) {
                final String tableName = tables.getString("TABLE_NAME");
                if (tableName.equalsIgnoreCase(BonitaProperties.cstSqlTableName)) {
                    exist = true;
                    break;
                }
            }
            logAnalysis.append( "Table [" + BonitaProperties.cstSqlTableName + "] exist? " + exist + ";");
            if (exist) {
                final Map<String, DataColumn> listColsExpected = new HashMap<>();
                listColsExpected.put( BonitaProperties.cstSqlTenantId.toLowerCase(), new DataColumn( BonitaProperties.cstSqlTenantId, COLTYPE.LONG)) ;
                listColsExpected.put( BonitaProperties.cstSqlResourceName.toLowerCase(), new DataColumn( BonitaProperties.cstSqlResourceName, COLTYPE.STRING, 200));
                listColsExpected.put( BonitaProperties.cstSqldomainName.toLowerCase(), new DataColumn( BonitaProperties.cstSqldomainName, COLTYPE.STRING, 500));
                listColsExpected.put( BonitaProperties.cstSqlPropertiesKey.toLowerCase(), new DataColumn( BonitaProperties.cstSqlPropertiesKey, COLTYPE.STRING, 200));
                listColsExpected.put( BonitaProperties.cstSqlPropertiesValue.toLowerCase(), new DataColumn( BonitaProperties.cstSqlPropertiesValue, COLTYPE.STRING, BonitaProperties.cstSqlPropertiesValueLengthDatabase));
                listColsExpected.put( BonitaProperties.cstSqlPropertiesStream.toLowerCase(), new DataColumn( BonitaProperties.cstSqlPropertiesStream, COLTYPE.BLOB));

                final Map<String, DataColumn> alterCols = new HashMap<>();

                // Table exists : is the fields are correct ?
                final ResultSet rs = dbm.getColumns(null /* catalog */, null /* schema */, null /* cstSqlTableName */,
                        null /* columnNamePattern */);

                while (rs.next()) {
                    String tableNameCol = rs.getString("TABLE_NAME");
                    final String colName = rs.getString("COLUMN_NAME");
                    final int length = rs.getInt("COLUMN_SIZE");

                    tableNameCol = tableNameCol == null ? "" : tableNameCol.toLowerCase();

                    if (!tableNameCol.equalsIgnoreCase(BonitaProperties.cstSqlTableName)) {
                        continue;
                    }
                    // final int dataType = rs.getInt("DATA_TYPE");
                    DataColumn dataColumn = listColsExpected.get(colName.toLowerCase());
                          
                    final Integer expectedSize = dataColumn==null ? null : dataColumn.length;
                    if (expectedSize == null) {
                        logAnalysis.append( "Colum[" + colName.toLowerCase() + "] not exist in [ " + listColsExpected + "];");
                        continue; // this columns is new
                    }
                    if (expectedSize>0 && length < expectedSize) {
                        logAnalysis.append("Column[" + colName.toLowerCase() + "] length[" + length + "] expected[" + expectedSize + "];");
                        alterCols.put(colName.toLowerCase(), dataColumn);
                    }
                    listColsExpected.remove(colName.toLowerCase());
                    // logAnalysis+="Remove Colum[" + colName.toLowerCase() + "] : list is now [ " + listColsExpected + "];";
                }
                // OK, create all missing column
                for (final DataColumn col : listColsExpected.values()) {
                    String sqlRequest = "alter table " + BonitaProperties.cstSqlTableName + " add  " + getSqlField(col, databaseProductName);
                    logAnalysis.append( sqlRequest + ";");

                    executeAlterSql(con, sqlRequest);
                    if (BonitaProperties.cstSqlTenantId.equalsIgnoreCase(col.colName)) {
                        sqlRequest = "update  " + BonitaProperties.cstSqlTableName + " set " + BonitaProperties.cstSqlTenantId + "=1";
                        logAnalysis.append( sqlRequest + ";");
                        executeAlterSql(con, sqlRequest);
                    }
                }
                // all change operation
                for (final DataColumn col : alterCols.values()) {
                    String sqlRequest = "alter table " + BonitaProperties.cstSqlTableName + " alter column "
                            + getSqlField(col, databaseProductName);
                    logAnalysis.append( sqlRequest + ";");

                    executeAlterSql(con, sqlRequest);
                }
                logAnalysis.append( "CheckCreateTable [" + BonitaProperties.cstSqlTableName + "] : Correct ");
                // add the constraint
                /*
                 * String constraints = "alter table "+ cstSqlTableName + " add constraint uniq_propkey unique ("+
                 * cstSqlTenantId+","
                 * + cstSqlResourceName+","
                 * + cstSqldomainName+","
                 * + cstSqlPropertiesKey+")";
                 * executeAlterSql(con, constraints);
                 */
            } else {
                // create the table
                final String createTableString = "create table " + BonitaProperties.cstSqlTableName + " ("
                        + getSqlField( new DataColumn(BonitaProperties.cstSqlTenantId, COLTYPE.LONG), databaseProductName) + ", "
                        + getSqlField( new DataColumn(BonitaProperties.cstSqlResourceName, COLTYPE.STRING,200), databaseProductName) + ", "
                        + getSqlField( new DataColumn(BonitaProperties.cstSqldomainName, COLTYPE.STRING, 500), databaseProductName) + ", "
                        + getSqlField( new DataColumn(BonitaProperties.cstSqlPropertiesKey, COLTYPE.STRING,200), databaseProductName) + ", "
                        + getSqlField( new DataColumn(BonitaProperties.cstSqlPropertiesValue, COLTYPE.STRING, BonitaProperties.cstSqlPropertiesValueLengthDatabase), databaseProductName) + ","
                        + getSqlField( new DataColumn(BonitaProperties.cstSqlPropertiesStream, COLTYPE.BLOB), databaseProductName)
                        + ")";
                logAnalysis.append( "CheckCreateTable [" + BonitaProperties.cstSqlTableName + "] : NOT EXIST : create it with script[" + createTableString + "]");
                executeAlterSql(con, createTableString);

                /*
                 * String constraints = "alter table "+ cstSqlTableName + " add constraint uniq_propkey unique ("+
                 * cstSqlTenantId+","
                 * + cstSqldomainName+","
                 * + cstSqlResourceName+","
                 * + cstSqlPropertiesKey+")";
                 * executeAlterSql(con, constraints);
                 */

            }
        } catch (final SQLException e) {
            final StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            final String exceptionDetails = sw.toString();
            logLevelAnalysis = java.util.logging.Level.SEVERE;

            logAnalysis.append( " ERROR during checkCreateDatase properties [" + bonitaProperties.mName + "] : "
                    + e.toString() + " : " + exceptionDetails);
            listEvents.add(new BEvent(BonitaProperties.eventCreationDatabase, e, "properties name;[" + bonitaProperties.mName + "]"));

        }
        String createIndex = "CREATE INDEX KEYS_INDEX ON " + BonitaProperties.cstSqlTableName + "(" + BonitaProperties.cstSqlTenantId + "," + BonitaProperties.cstSqlResourceName + "," + BonitaProperties.cstSqldomainName + "," + BonitaProperties.cstSqlPropertiesKey + ")";
        try {
            executeAlterSql(con, createIndex);
        } catch (SQLException e) {
            // Do not report this failure at this moment
            // final StringWriter sw = new StringWriter();
            // e.printStackTrace(new PrintWriter(sw));
            // final String exceptionDetails = sw.toString();
            // logLevelAnalysis = java.util.logging.Level.FINE;


            // logAnalysis += " ERROR during checkCreateDatase properties [" + mName + "] : "+ e.toString() + " : " + exceptionDetails;
        }

        logger.log(logLevelAnalysis, logAnalysis.toString());
        return listEvents;
    }

    private void executeAlterSql(final Connection con, final String sqlRequest) throws SQLException {
        logger.log(bonitaProperties.logLevel, loggerLabel + "executeAlterSql : Execute [" + sqlRequest + "]");

        try ( Statement stmt = con.createStatement() ) {
            stmt.executeUpdate(sqlRequest);

            if (!con.getAutoCommit()) {
                con.commit();
            }
        } catch (Exception e) {
            throw e;
        }

        

    }

    public class TypeTranslation {

        public COLTYPE colType;
        public Map<String, String> translationTable = new HashMap<>();

        public TypeTranslation(COLTYPE colType, String oracle, String postGres, String h2, String mySql, String sqlServer, String def) {
            this.colType = colType;
            translationTable.put(CST_DRIVER_ORACLE, oracle);
            translationTable.put(CST_DRIVER_POSTGRESQL, postGres);
            translationTable.put(CST_DRIVER_H2, h2);
            translationTable.put(CST_DRIVER_MYSQL, mySql);
            translationTable.put(CST_DRIVER_SQLSERVER, sqlServer);
            translationTable.put("def", def);
        }

        public String getValue(String databaseName) {
            if (translationTable.get(databaseName) != null)
                return translationTable.get(databaseName);
            return translationTable.get("def");
        }

    }
    public enum COLTYPE {
        LONG, BLOB, STRING, BOOLEAN, DECIMAL, TEXT
    }

    public static class DataColumn {

        public String colName;
        public COLTYPE colType;
        public int length;

        public DataColumn(String colName, COLTYPE colType, int length) {
            this.colName = colName.toLowerCase();
            this.colType = colType;
            this.length = length;
        }

        /*
         * for all field without a size (LONG, BLOG)
         */
        public DataColumn(String colName, COLTYPE colType) {
            this.colName = colName.toLowerCase();
            this.colType = colType;
            this.length = 0;
        }

    }

    /* String oracle, String postGres, String h2, String mySql, String sqlServer, String def */
    private List<TypeTranslation> allTransations = Arrays.asList(
            new TypeTranslation(COLTYPE.LONG, "NUMBER", "BIGINT", "BIGINT", "BIGINT", "NUMERIC(19, 0)", "NUMBER"),
            new TypeTranslation(COLTYPE.BOOLEAN, "NUMBER(1)", "BOOLEAN", "BOOLEAN", "BOOLEAN", "BIT", "BOOLEAN"),
            new TypeTranslation(COLTYPE.DECIMAL, "NUMERIC(19,5)", "NUMERIC(19,5)", "NUMERIC(19,5)", "NUMERIC(19,5)", "NUMERIC(19,5)", "NUMERIC(19,5)"),
            new TypeTranslation(COLTYPE.BLOB, "BLOB", "BYTEA", "MEDIUMBLOB", "MEDIUMBLOB", "VARBINARY(MAX)", "BLOB"),
            new TypeTranslation(COLTYPE.TEXT, "CLOB", "TEXT", "CLOB", "MEDIUMTEXT", "NVARCHAR(MAX)", "TEXT"),
            new TypeTranslation(COLTYPE.STRING, "VARCHAR2(%d CHAR)", "VARCHAR(%d)", "VARCHAR(%d)", "VARCHAR(%d)", "NVARCHAR(%d)", "VARCHAR(%d)"));

    /**
     * calculate the field according different database
     *
     * @param colName
     * @param colSize
     * @param databaseProductName
     * @return
     */
    private String getSqlField(final DataColumn col, final String databaseProductName) {
        for (TypeTranslation typeTranslation : allTransations) {
            if (typeTranslation.colType == col.colType) {
                String value = String.format(typeTranslation.getValue(databaseProductName), col.length);
                return col.colName + " " + value;
            }
        }
        return "";
    }
/*     public String getSqlField(final String colName, final int colSize, final String databaseProductName) {
        if (colSize == -1) {
            // long
            if (CST_DRIVER_ORACLE.equalsIgnoreCase(databaseProductName)) {
                return colName + " NUMBER ";
            } else if (CST_DRIVER_POSTGRESQL.equalsIgnoreCase(databaseProductName)) {
                return colName + " BIGINT";
            } else if (CST_DRIVER_H2.equalsIgnoreCase(databaseProductName)) {
                return colName + " BIGINT";
            }
            return colName + " BIGINT";
        }
        if (colSize == -2) {
            // long
            if (CST_DRIVER_ORACLE.equalsIgnoreCase(databaseProductName)) {
                return colName + " BLOB ";
            } else if (CST_DRIVER_POSTGRESQL.equalsIgnoreCase(databaseProductName)) {
                return colName + " bytea";
            } else if (CST_DRIVER_H2.equalsIgnoreCase(databaseProductName)) {
                return colName + " BLOB";
            }
            return colName + " BLOB";
        }
        // varchar
        if (CST_DRIVER_ORACLE.equalsIgnoreCase(databaseProductName)) {
            return colName + " VARCHAR2(" + colSize + ")";
        }
        if (CST_DRIVER_POSTGRESQL.equalsIgnoreCase(databaseProductName)) {
            return colName + " varchar(" + colSize + ")"; // old varying
        } else if (CST_DRIVER_H2.equalsIgnoreCase(databaseProductName)) {
            return colName + " varchar(" + colSize + ")";
        } else {
            return colName + " varchar(" + colSize + ")";
        }
    }
    */
}
