package fr.edf.dco.contacts.bhc.base;


import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;
import java.util.UUID;

import org.apache.hadoop.hbase.client.Put;

import com.google.common.collect.HashMultimap;

import fr.edf.com.dacc.HDFSLogger;
import fr.edf.dco.common.base.CustomException;
import fr.edf.dco.common.connector.base.ConnectorException;
import fr.edf.dco.common.connector.hadoop.HbaseConnector;
import fr.edf.dco.common.connector.hadoop.HdfsConnector;
import fr.edf.dco.common.connector.jdbc.SqlServerConnector;
import fr.edf.dco.common.connector.misc.KafkaConnector;
import fr.edf.dco.contacts.bhc.entities.cartography.Cartography;

/**
 * Singleton Design Pattern Implementation that holds all configurations and
 * properties, along with instances of sources connections
 * 
 * @author fahd-externe.essid@edf.fr
 */
public class ApplicationContext {

  // ---------------------------------------------------------------------------
  // CONSTRUCTOR
  // ---------------------------------------------------------------------------

  /**
   * Private singleton constructor
   */
  private ApplicationContext() {
    loadProperties();

    if (getProperty(Constants.PROPERTIES_APPLICATION_LOGS).equals(Constants.LOG_CONSOLE)) {
      logger = new ConsoleLogger(this.getClass().getName(), Constants.CONTACT_PROCESS_GOLABAL);
    } else {
      logger = new HDFSLogger(this.getClass().getName(), Constants.CONTACT_PROCESS_GOLABAL);
    }
  }

  // ---------------------------------------------------------------------------
  // IMPLEMENTATION
  // ---------------------------------------------------------------------------

  /**
   * returns the ApplicationContext singleton unique instance
   */
  public static ApplicationContext getInstance() {
    if (singleton == null) {
      synchronized (ApplicationContext.class) {
        singleton = new ApplicationContext();
      }
    }

    return singleton;
  }

  /**
   * Close application context by closing all connections
   */
  public void closeContext() {
    // close HDFS Appenders
    closeAppenders();

    // closing HBASE connection
    if (hbase != null) {
      try {
        hbase.disconnect();
      } catch (ConnectorException e) {
        logger.error(Constants.ERROR_HBASE, "could not close hbase connection " + e.getMessage());
      }
    }

    // closing HDFS connection
    if (hdfs != null) {
      try {
        hdfs.disconnect();
      } catch (ConnectorException e) {
        logger.error(Constants.ERROR_HDFS, "could not close hdfs connection " + e.getMessage());
      }
    }

    // closing SQLServer connection
    if (sql != null) {
      try {
        sql.disconnect();
      } catch (ConnectorException e) {
        logger.error(Constants.ERROR_SQLSERVER, "could not close SQLServer connection " + e.getMessage());
      }
    }

    // closing KAFKA connection
    if (kafka != null) {
      kafka.disconnect();
    }
  }

  /**
   * Load context properties from properties file
   */
  private void loadProperties() {
    properties = new Properties();
    FileInputStream inputStream;

    try {
      inputStream = new FileInputStream(Constants.PROPERTIES_APPLICATION_FILE_NAME);
      properties.load(inputStream);
      inputStream.close();
    } catch (IOException e) {
      logger.error(Constants.ERROR_OTHER, "Unable to locate properties file : " + Constants.PROPERTIES_APPLICATION_FILE_NAME);
    }
  }

  // ---------------------------------------------------------------------------
  // ACCESSORS
  // ---------------------------------------------------------------------------

  /**
   * Access properties file attributes by its name
   */
  public String getProperty(String property) {
    if (properties.containsKey(property)) {
      return properties.getProperty(property);
    } else {
      logger.error(Constants.ERROR_OTHER, "Property : " + property + " ,is not defined in properties file : " + Constants.PROPERTIES_APPLICATION_FILE_NAME);
      return null;
    }
  }

  /**
   * Return unique instance of HbaseConnector
   */
  public HbaseConnector getHbase() {
    if (hbase == null) {
      hbase = new HbaseConnector(getProperty(Constants.PROPERTIES_KERBEROS_USER), getProperty(Constants.PROPERTIES_KERBEROS_KEYTAB), true);
      try {
        hbase.connect();
      } catch (ConnectorException e) {
        logger.error(Constants.ERROR_HBASE, "Unable to connect to hbase : " + e.getMessage());
      }
    }

    return hbase;
  }

  /**
   * Return unique instance of HDFSConnector
   */
  public HdfsConnector getHdfs() {
    if (hdfs == null) {
      hdfs = new HdfsConnector(getProperty(Constants.PROPERTIES_KERBEROS_USER), getProperty(Constants.PROPERTIES_KERBEROS_KEYTAB), true);

      try {
        hdfs.connect();
      } catch (ConnectorException e) {
        logger.error(Constants.ERROR_HDFS, "Unable to connect to hdfs : " + e.getMessage());
      }
    }

    return hdfs;
  }

  /**
   * Return unique instance of KafkaConnector
   */
  public KafkaConnector getKafka(String topic) {
    if (kafka == null) {
      kafka = new KafkaConnector(getProperty(Constants.PROPERTIES_KAFKA_ZOOKEEPER_QUORUM), getProperty(Constants.PROPERTIES_KAFKA_BROKERS_LIST), UUID.randomUUID().toString());
      kafka.setTopic(topic);
      kafka.connect();
    }

    return kafka;
  }

  /**
   * Return HashMultimap of cartography entries
   */
  public HashMultimap<String, Cartography> getCartography() throws CustomException {
    if (cartgraphy == null) {
      cartgraphy = HashMultimap.create();

      // cartography entries are fetched from PMDBI SQLServer database
      SqlServerConnector sql = new SqlServerConnector(getProperty(Constants.PROPERTIES_SQL_SERVER_HOST), getProperty(Constants.PROPERTIES_SQL_SERVER_INSTANCE), getProperty(Constants.PROPERTIES_SQL_SERVER_USER), getProperty(Constants.PROPERTIES_SQL_SERVER_PWD), getProperty(Constants.PROPERTIES_SQL_SERVER_CARTOGRAPHY_DB));

      try {
        sql.connect();

        // executing stored procedure and filling HashMultimap
        ResultSet result = sql.procedure(getProperty(Constants.PROPERTIES_SQL_SERVER_CARTOGRAPHY_PROCEDURE));

        while (result.next()) {
          String id = result.getString(1);
          String template = result.getString(2);
          String strategy = result.getString(3);

          Cartography entry = new Cartography(id, strategy, template);
          cartgraphy.put(id, entry);
        }

        result.close();
      } catch (SQLException | ConnectorException e) {
        logger.error(Constants.ERROR_SQLSERVER, "Error while executing SQLServer stored procedure: " + e.getMessage());
        throw new CustomException("Cartography fucked up");
      }
    }

    return cartgraphy;
  }

  /**
   * Return all HBASE puts map by row keys
   */
  public HashMap<String, Put> getPuts() {
    if (contactsPuts == null) {
      contactsPuts = new HashMap<String, Put>();
    }

    return contactsPuts;
  }

  /**
   * Return HDFS file appender for a given HDFS path
   */
  public BufferedWriter getHdfsFileAppender(String path) {
    if (!getAppenders().containsKey(path)) {
      HdfsConnector hdfs = getHdfs();
      BufferedWriter br = null;

      try {
        br = hdfs.getFileAppender(path);
      } catch (IOException e) {
        logger.error(Constants.ERROR_HDFS, "Unable to open fine appender for HDFS path : " + path + " " + e.getMessage());
      }

      if (br != null) {
        getAppenders().put(path, br);
      }
    }

    return getAppenders().get(path);
  }

  /**
   * Return a map for HDFS file appenders by path
   */
  public HashMap<String, BufferedWriter> getAppenders() {
    if (hdfsFileAppenders == null) {
      hdfsFileAppenders = new HashMap<String, BufferedWriter>();
    }

    return hdfsFileAppenders;
  }

  /**
   * close HDFS file appenders
   */
  private void closeAppenders() {
    if (hdfsFileAppenders != null && new Boolean(getProperty(Constants.PROPERTIES_APPLICATION_ARCHIVAGE))) {
      Iterator<String> files = hdfsFileAppenders.keySet().iterator();

      while (files.hasNext()) {
        try {
          hdfsFileAppenders.get(files.next()).close();
        } catch (IOException e) {
          logger.error(Constants.ERROR_HDFS, "unable to close HDFS file appender" + e.getMessage());
        }
      }
    }
  }

  /**
   * Remove duplicated lines from files appenders
   */
  public void removeDuplicatesFromFile() {
    Iterator<String> files = getAppenders().keySet().iterator();
    HdfsConnector hdfs = getHdfs();

    while (files.hasNext()) {
      String file = files.next();

      try {
        hdfs.removeDuplicateLines(file);
      } catch (IOException e) {
        logger.error(Constants.ERROR_HDFS, "unable to remove duplicates from HDFS file : " + file + " " + e.getMessage());
      }
    }
  }

  /**
   * flush Contacts in HBASE
   */
  public void flushContacts(boolean delta) {

    if (contactsPuts != null && contactsPuts.size() > 0) {
      try {
        HbaseConnector hbase = getHbase();
        hbase.setTable(getProperty(Constants.PROPERTIES_HBASE_CONTACTS_TABLE));
        hbase.multiPut(new ArrayList<Put>(contactsPuts.values()));

        if (delta) {
          for (String key : contactsPuts.keySet()) {
            contactsPuts.put(key, Utils.resultToNoVersionPut(hbase.getRow(key), Utils.getContactPut(key)));
          }

          hbase.setTable(getProperty(Constants.PROPERTIES_HBASE_CONTACTS_TEMP_TABLE));
          hbase.multiPut(new ArrayList<Put>(contactsPuts.values()));
        }

        contactsPuts = new HashMap<String, Put>();
      } catch (IOException e) {
        e.printStackTrace();
        logger.error(Constants.ERROR_HBASE, "Could not write puts to hbase " + e.getMessage());
      }
    } else {
      logger.warn(Constants.WARNING_DATA, "No put operation executed; puts map was empty");
    }
  }

  /**
   * Flushing only Temporary table
   * 
   * @param delta
   */
  public void flushTempContacts() {

    if (contactsPuts != null && contactsPuts.size() > 0) {
      try {
        HbaseConnector hbase = getHbase();
        hbase.setTable(getProperty(Constants.PROPERTIES_HBASE_CONTACTS_TABLE));

        for (String key : contactsPuts.keySet()) {
          contactsPuts.put(key, Utils.resultToNoVersionPut(hbase.getRow(key), Utils.getContactPut(key)));
        }

        hbase.setTable(getProperty(Constants.PROPERTIES_HBASE_CONTACTS_TEMP_TABLE));
        hbase.multiPut(new ArrayList<Put>(contactsPuts.values()));

        contactsPuts = new HashMap<String, Put>();
      } catch (IOException e) {
        e.printStackTrace();
        logger.error(Constants.ERROR_HBASE, "Could not write puts to hbase " + e.getMessage());
      }
    } else {
      logger.warn(Constants.WARNING_DATA, "No put operation executed; puts map was empty");
    }
  }

  /**
   * Return the appropriate and unique HDFSLogger instance per class and process
   */
  @SuppressWarnings("rawtypes")
  public HDFSLogger getLogger(Class clazz, String process) {
    if (!getLoggers().containsKey(clazz + process)) {
      if (getProperty(Constants.PROPERTIES_APPLICATION_LOGS).equals(Constants.LOG_CONSOLE)) {
        getLoggers().put(clazz + process, new ConsoleLogger(clazz.getName(), process));
      } else {
        getLoggers().put(clazz + process, new HDFSLogger(clazz.getName(), process));
      }
    }

    return getLoggers().get(clazz + process);
  }

  /**
   * Return a map of all instances of HDFSLogger
   */
  private HashMap<String, HDFSLogger> getLoggers() {
    if (loggers == null) {
      loggers = new HashMap<String, HDFSLogger>();
    }

    return loggers;
  }

  /**
   * Return unique instance of SQLServerConnector
   */
  public SqlServerConnector getSqlServer() {
    if (sql == null) {
      sql = new SqlServerConnector(getProperty(Constants.PROPERTIES_SQL_SERVER_HOST), getProperty(Constants.PROPERTIES_SQL_SERVER_INSTANCE), getProperty(Constants.PROPERTIES_SQL_SERVER_USER), getProperty(Constants.PROPERTIES_SQL_SERVER_PWD), getProperty(Constants.PROPERTIES_SQL_SERVER_REPORTING_DB));

      try {
        sql.connect();
      } catch (ConnectorException e) {
        logger.error(Constants.ERROR_SQLSERVER, "Error while connecting to SQLServer : " + e.getMessage());
      }
    }

    return sql;
  }

  /**
   * Return wether the archiving processess is enabled
   * 
   * @return
   */
  public boolean getArchived() {
    return new Integer(getProperty(Constants.PROPERTIES_APPLICATION_ARCHIVAGE)).intValue() > 0;
  }

  public HashMap<String, Put> getContactPuts() {
    return contactsPuts;
  }

  public void resetContactPuts() {
    contactsPuts = new HashMap<String, Put>();
  }

  /**
   * Hbase to SQLServer acquittal action
   * @param table sqlServer target table 
   * @param action start or end
   */
  public void acquitHbaseToSql(String table, String action, Timestamp ts) {
    String query = "INSERT INTO " + getProperty(Constants.PROPERTIES_SQL_SERVER_ACQUITTAL_TABLE) + " (TABLE_CIBLE, ACTION, [DATE/HEURE]) " + "VALUES('" + table + "', '" + action + "', '" + ts +"')";

    //TODO : delete this case once SQLServer Prod is ready !!!
    // this function core will be replaced by :
    //    try {
    //      getSqlServer().executeUpdate(query);
    //    } catch (SQLException e) {
    //      logger.error(Constants.ERROR_SQLSERVER, "Could not write to SQLServer acquittal table : " + e.getMessage());
    //    }
    
    SqlServerConnector sqlServer = null;

    if (getProperty(Constants.PROPERTIES_APPLICATION_ENV_NAME).equals("Prod")) {
      sqlServer = new SqlServerConnector("10.122.3.254", "PMDBI", "cs_bhc", "_6w[q9KL6,7D", "Contacts_Sortants_Reporting");

      try {
        sqlServer.connect();
      } catch (ConnectorException e) {
        e.printStackTrace();
      }
    } else {
      sqlServer = getSqlServer();
    }

    try {
      sqlServer.executeUpdate(query);
    } catch (SQLException e) {
      logger.error(Constants.ERROR_SQLSERVER, "Could not write to SQLServer acquittal table : " + e.getMessage());
    }
  }

  // ---------------------------------------------------------------------------
  // DATA MEMBERS
  // ---------------------------------------------------------------------------

  private static volatile ApplicationContext singleton = null;
  private static Properties properties;
  private static HDFSLogger logger;
  private static HbaseConnector hbase = null;
  private static HdfsConnector hdfs = null;
  private static KafkaConnector kafka = null;
  private static SqlServerConnector sql = null;
  private static HashMultimap<String, Cartography> cartgraphy = null;
  private static HashMap<String, Put> contactsPuts = null;
  private static HashMap<String, BufferedWriter> hdfsFileAppenders = null;
  private static HashMap<String, HDFSLogger> loggers = null;
}
