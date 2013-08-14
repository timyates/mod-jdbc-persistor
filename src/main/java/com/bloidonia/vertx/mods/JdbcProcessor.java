/*
 * Copyright 2012-2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.bloidonia.vertx.mods ;

import com.mchange.v2.c3p0.* ;

import java.sql.Connection ;
import java.sql.Driver ;
import java.sql.DriverManager ;
import java.sql.PreparedStatement ;
import java.sql.ResultSet ;
import java.sql.SQLException ;
import java.sql.Statement ;

import java.util.ArrayList ;
import java.util.concurrent.ConcurrentHashMap ;
import java.util.Iterator ;
import java.util.List ;
import java.util.Map ;
import java.util.UUID ;

import org.apache.commons.dbutils.QueryRunner ;
import org.apache.commons.dbutils.ResultSetHandler ;
import org.apache.commons.dbutils.handlers.MapListHandler ;
import org.apache.commons.dbutils.handlers.LimitedMapListHandler ;

import org.vertx.java.busmods.BusModBase ;
import org.vertx.java.core.Handler ;
import org.vertx.java.core.eventbus.Message ;
import org.vertx.java.core.json.JsonArray ;
import org.vertx.java.core.json.JsonObject ;

public class JdbcProcessor extends BusModBase implements Handler<Message<JsonObject>> {
  private String address ;
  private String uid ;

  private String driver ;
  private String url ;
  private String username ;
  private String password ;
  private String pmdKnownBroken ;
  private StatementFiller statementFiller ;
  private int    minpool ;
  private int    maxpool ;
  private int    acquire ;
  private int    batchTimeout ;
  private int    transTimeout ;

  private volatile static ConcurrentHashMap<String,ComboPooledDataSource> poolMap = new ConcurrentHashMap<String,ComboPooledDataSource>( 8, 0.9f, 1 ) ;

  private static boolean setupPool( String  address,
                                    String  driver,
                                    String  url,
                                    String  username,
                                    String  password,
                                    int     minPool,
                                    int     maxPool,
                                    int     acquire,
                                    String  automaticTestTable,
                                    int     idleConnectionTestPeriod,
                                    String  preferredTestQuery,
                                    boolean testConnectionOnCheckin,
                                    boolean testConnectionOnCheckout ) throws Exception {
    if( poolMap.get( address ) == null ) {
      synchronized( poolMap ) {
        if( poolMap.get( address ) == null ) {
          DriverManager.registerDriver( (Driver)Class.forName( driver ).newInstance() ) ;
          ComboPooledDataSource pool = new ComboPooledDataSource() ;
          pool.setDriverClass( driver ) ;
          pool.setJdbcUrl( url ) ;
          pool.setUser( username ) ;
          pool.setPassword( password ) ;
          pool.setMinPoolSize( minPool ) ;
          pool.setMaxPoolSize( maxPool ) ;
          pool.setAcquireIncrement( acquire ) ;
          pool.setAutomaticTestTable( automaticTestTable ) ;
          pool.setIdleConnectionTestPeriod( idleConnectionTestPeriod ) ;
          pool.setPreferredTestQuery( preferredTestQuery ) ;
          pool.setTestConnectionOnCheckin( testConnectionOnCheckin ) ;
          pool.setTestConnectionOnCheckout( testConnectionOnCheckout ) ;
          if( poolMap.putIfAbsent( address, pool ) != null ) {
            pool.close() ;
          }
        }
      }
    }
    return false ;
  }

  private static void closePool( String address, String url ) throws SQLException {
    if( poolMap.get( address ) != null ) {
      synchronized( poolMap ) {
        ComboPooledDataSource pool = poolMap.get( address ) ;
        if( pool != null ) {
          pool = poolMap.remove( address ) ;
          if( pool != null ) {
            pool.close() ;
            DriverManager.deregisterDriver( DriverManager.getDriver( url ) ) ;
          }
        }
      }
    }
  }

  public void start() {
    super.start() ;

    address      = getOptionalStringConfig( "address", "com.bloidonia.jdbcpersistor" ) ;
    uid          = String.format( "%s-%s", address, UUID.randomUUID().toString() ) ;

    driver       = getOptionalStringConfig( "driver",   "org.hsqldb.jdbcDriver" ) ;
    url          = getOptionalStringConfig( "url",      "jdbc:hsqldb:mem:test?shutdown=true"  ) ;
    username     = getOptionalStringConfig( "username", ""                      ) ;
    password     = getOptionalStringConfig( "password", ""                      ) ;

    String automaticTestTable        = getOptionalStringConfig( "c3p0.automaticTestTable", null ) ;
    int idleConnectionTestPeriod     = getOptionalIntConfig( "c3p0.idleConnectionTestPeriod", 0 ) ;
    String preferredTestQuery        = getOptionalStringConfig( "c3p0.preferredTestQuery", null ) ;
    boolean testConnectionOnCheckin  = getOptionalBooleanConfig( "c3p0.testConnectionOnCheckin", false ) ;
    boolean testConnectionOnCheckout = getOptionalBooleanConfig( "c3p0.testConnectionOnCheckout", false ) ;

    minpool      = getOptionalIntConfig( "minpool",    5  ) ;
    maxpool      = getOptionalIntConfig( "maxpool",    20 ) ;
    acquire      = getOptionalIntConfig( "acquire",    5 ) ;

    pmdKnownBroken = getOptionalStringConfig( "pmdKnownBroken", "no" ) ;
    switch( pmdKnownBroken ) {
      case "yes" :
        statementFiller = new BrokenPMDStatementFiller() ;
        break ;
      case "maybe" :
        statementFiller = new MaybeBrokenPMDStatementFiller() ;
        break ;
      default :
        statementFiller = new NonBrokenPMDStatementFiller() ;
    }

    batchTimeout = getOptionalIntConfig( "batchtimeout",       5000  ) ;
    transTimeout = getOptionalIntConfig( "transactiontimeout", 10000 ) ;

    try {
      if( setupPool( address, driver, url, username, password, minpool, maxpool, acquire,
                     automaticTestTable, idleConnectionTestPeriod, preferredTestQuery,
                     testConnectionOnCheckin, testConnectionOnCheckout ) ) {
        logger.debug( "Pool created" ) ;
      }
      else {
        logger.debug( "Pool already exists" ) ; 
      }
      eb.registerHandler( address, this ) ;
      eb.send( String.format( "%s.ready", address ), new JsonObject() {{
        putString( "status", "ok" ) ;
      }} ) ;
    }
    catch( Exception ex ) {
      logger.fatal( "Error when starting JdbcBusMod", ex ) ;
    }
  }

  public void stop() {
    eb.send( address + ".unregister", new JsonObject() {{
      putString( "processor", uid ) ;
    }} ) ;
    eb.unregisterHandler( uid, this ) ;
    try {
      closePool( address, url ) ;
    }
    catch( SQLException ex ) {
      logger.error( String.format( "Error closing pool: %s", ex.getMessage() ), ex ) ;
    }
  }

  public void handle( final Message<JsonObject> message ) {
    String action = message.body().getString( "action" ) ;
    if ( logger.isDebugEnabled() ) {
      logger.debug( "** HANDLE ** " + this.toString() + " (main handler) RECEIVED CALL " + action ) ;
    }
    if( action == null ) {
      sendError( message, "action must be specified" ) ;
    }
    switch( action ) {
      case "select" :
        doSelect( message ) ;
        break ;
      case "execute" :
        doExecute( message ) ;
        break ;
      case "update" :
        doUpdate( message, false ) ;
        break ;
      case "insert" :
        doUpdate( message, true ) ;
        break ;
      case "transaction" :
        doTransaction( message ) ;
        break ;
      case "pool-status" :
        final ComboPooledDataSource pool = poolMap.get( address ) ;
        if( pool != null ) {
          try {
            sendOK( message, new JsonObject() {{
              putNumber( "connections", pool.getNumConnections() ) ;
              putNumber( "idle", pool.getNumIdleConnections() ) ;
              putNumber( "busy", pool.getNumBusyConnections() ) ;
              putNumber( "orphans", pool.getNumUnclosedOrphanedConnections() ) ;
            }} ) ;
          }
          catch( SQLException ex ) {
            sendError( message, "Cannot get pool info", ex ) ;
          }
        }
        else {
          sendError( message, "No pool found!" ) ;
        }
        break ;
      default:
        sendError( message, "Invalid action : " + action ) ;
    }
  }

  /****************************************************************************
   **
   **  Select handling
   **
   ****************************************************************************/

  private void doSelect( Message<JsonObject> message ) {
    Connection connection = null ;
    try {
      connection = poolMap.get( address ).getConnection() ;
      doSelect( message, connection, null ) ;
    }
    catch( Exception ex ) {
      sendError( message, "Caught error with SELECT", ex ) ;
    }
  }

  private void doSelect( Message<JsonObject> message,
                         Connection connection,
                         TransactionalHandler transaction ) throws SQLException {
    new BatchHandler( connection, message, transaction ) {
      public JsonObject process() throws SQLException {
        JsonObject reply = new JsonObject() ;
        ArrayList<Map<String,Object>> result = new ArrayList<Map<String,Object>>() ;
        // processing
        while( ( resultSet != null || valueIterator.hasNext() ) &&
               ( batchSize == -1 || result.size() < batchSize ) ) {
          LimitedMapListHandler handler = new LimitedMapListHandler( batchSize == -1 ? -1 : batchSize - result.size() ) ;
          if( resultSet == null ) {
            List<Object> params = valueIterator.next() ;
            statementFiller.fill( statement, params ) ;
            resultSet = statement.executeQuery() ;
          }
          store( result, handler ) ;
        }
        reply.putArray( "result", JsonUtils.listOfMapsToJsonArray( result ) ) ;
        return reply ;
      }
    }.handle( message ) ;
  }

  /****************************************************************************
   **
   **  Execute handling
   **
   ****************************************************************************/

  private void doExecute( Message<JsonObject> message ) {
    Connection connection = null ;
    try {
      connection = poolMap.get( address ).getConnection() ;
      doExecute( message, connection, null ) ;
    }
    catch( Exception ex ) {
      sendError( message, "Caught error with EXECUTE", ex ) ;
    }
    finally {
      SilentCloser.close( connection ) ;
    }
  }

  private void doExecute( Message<JsonObject> message,
                          Connection connection,
                          TransactionalHandler transaction ) throws SQLException {
    Statement statement = null ;
    try {
      statement = connection.createStatement() ;
      statement.execute( message.body().getString( "stmt" ) ) ;
      if( transaction == null ) {
        sendOK( message ) ;
      }
      else {
        JsonObject reply = new JsonObject() ;
        reply.putString( "status", "ok" ) ;
        message.reply( reply, transaction ) ;
      }
    }
    finally {
      SilentCloser.close( statement ) ;
    }
  }

  /****************************************************************************
   **
   **  Update/Insert handling
   **
   ****************************************************************************/

  private void doUpdate( Message<JsonObject> message, boolean insert ) {
    Connection connection = null ;
    try {
      connection = poolMap.get( address ).getConnection() ;
      doUpdate( message, connection, insert, null ) ;
    }
    catch( Exception ex ) {
      sendError( message, "Caught error with UPDATE.", ex ) ;
    }
  }

  private void doUpdate( Message<JsonObject> message,
                         Connection connection,
                         final boolean insert,
                         TransactionalHandler transaction ) throws SQLException {
    new BatchHandler( connection, message, transaction ) {
      void initialiseStatement( Message<JsonObject> initial ) throws SQLException {
        if( insert ) {
          this.statement = connection.prepareStatement( initial.body().getString( "stmt" ), Statement.RETURN_GENERATED_KEYS ) ;
        }
        else {
          this.statement = connection.prepareStatement( initial.body().getString( "stmt" ) ) ;
        }
      }
      public JsonObject process() throws SQLException {
        JsonObject reply = new JsonObject() ;
        ArrayList<Map<String,Object>> result = new ArrayList<Map<String,Object>>() ;
        // processing
        int nRows = 0 ;
        if( insert ) {
          while( ( resultSet != null || valueIterator.hasNext() ) &&
                 ( batchSize == -1 || result.size() < batchSize ) ) {
            LimitedMapListHandler handler = new LimitedMapListHandler( batchSize == -1 ? -1 : batchSize - result.size() ) ;
            if( resultSet == null ) {
              List<Object> params = valueIterator.next() ;
              statementFiller.fill( statement, params ) ;
              nRows += statement.executeUpdate() ;
              resultSet = statement.getGeneratedKeys() ;
            }
            store( result, handler ) ;
          }
          reply.putArray( "result", JsonUtils.listOfMapsToJsonArray( result ) ) ;
        }
        else {
          while( valueIterator.hasNext() ) {
            List<Object> params = valueIterator.next() ;
            statementFiller.fill( statement, params ) ;
            nRows += statement.executeUpdate() ;
          }
        }
        reply.putNumber( "updated", nRows ) ;
        return reply ;
      }
    }.handle( message ) ;
  }

  /****************************************************************************
   **
   **  Transaction handling
   **
   ****************************************************************************/

  private void doTransaction( Message<JsonObject> message ) {
    Connection connection = null ;
    try {
      connection = poolMap.get( address ).getConnection() ;
      connection.setAutoCommit( false ) ;
      doTransaction( message, connection ) ;
    }
    catch( Exception ex ) {
      sendError( message, "Caught exception in TRANSACTION.  Rolling back...", ex ) ;
      try { connection.rollback() ; }
      catch( SQLException exx ) {
        logger.error( "Failed to rollback", exx ) ;
      }
      SilentCloser.close( connection ) ;
    }
  }

  private void doTransaction( Message<JsonObject> message, Connection connection ) {
    JsonObject reply = new JsonObject() ;
    reply.putString( "status", "ok" ) ;

    int timeout = message.body().getNumber( "timeout", transTimeout ).intValue() ;
    final long timerId = vertx.setTimer( timeout, new TransactionTimeoutHandler( connection ) ) ;

    message.reply( reply, new TransactionalHandler( connection, timerId, timeout ) ) ;
  }
  
  private class TransactionTimeoutHandler implements Handler<Long> {
    Connection connection ;

    TransactionTimeoutHandler( Connection connection ) {
      this.connection = connection ;
    }

    public void handle( Long timerID ) {
      logger.warn( "Closing and rolling back transaction on timeout") ;
      try {
        connection.rollback() ;
        SilentCloser.close( connection ) ;
      } catch ( SQLException ex ) {
        logger.error( "Failed to rollback on transaction timeout", ex ) ;
      }
    }
  }

  private class TransactionalHandler implements Handler<Message<JsonObject>> {
    Connection connection ;
    long timerId ;
    int timeout ;

    TransactionalHandler( Connection connection, long timerId, int timeout ) {
      this.connection = connection ;
      this.timerId = timerId ;
      this.timeout = timeout ;
    }

    public void handle( Message<JsonObject> message ) {
      vertx.cancelTimer( timerId ) ;
      String action = message.body().getString( "action" ) ;
      if ( logger.isDebugEnabled() ) {
        logger.debug( "** HANDLE ** " + this.toString() + " (TRANSACTION handler) RECEIVED CALL " + action ) ;
      }
      if( action == null ) {
        sendError( message, "action must be specified" ) ;
      }
      try {
        switch( action ) {
          case "select" :
            doSelect( message, connection, this ) ;
            break ;
          case "execute" :
            doExecute( message, connection, this ) ;
            break ;
          case "update" :
            doUpdate( message, connection, false, this ) ;
            break ;
          case "insert" :
            doUpdate( message, connection, true, this ) ;
            break ;
          case "commit" :
            doCommit( message ) ;
            return ;
          case "rollback" :
            doRollback( message ) ;
            return ;
          default:
            sendError( message, "Invalid action : " + action + ". Rolling back." ) ;
            doRollback( null ) ;
            return ;
        }
        timerId = vertx.setTimer( timeout, new TransactionTimeoutHandler( connection ) ) ;
      }
      catch( Exception ex ) {
        sendError( message, "Error performing " + action + ".  Rolling back.", ex ) ;
        doRollback( null ) ;
      }
    }

    private void doCommit( Message<JsonObject> message ) {
      try {
        connection.commit() ;
        if( message != null ) sendOK( message ) ;
      }
      catch( SQLException ex ) { logger.error( "Failed to commit", ex ) ; }
      finally { SilentCloser.close( connection ) ; }
    }

    private void doRollback( Message<JsonObject> message ) {
      try {
        connection.rollback() ;
        if( message != null ) sendOK( message ) ;
      }
      catch( SQLException ex ) { logger.error( "Failed to rollback", ex ) ; }
      finally { SilentCloser.close( connection ) ; }
    }
  }

  /****************************************************************************
   **
   **  Batch handling
   **
   ****************************************************************************/

  private class BatchTimeoutHandler implements Handler<Long> {
    Connection connection ;
    Statement statement ;
    ResultSet rslt ;

    BatchTimeoutHandler( Statement statement, ResultSet rslt ) {
      this( null, statement, rslt ) ;
    }

    BatchTimeoutHandler( Connection conn, Statement statement, ResultSet rslt ) {
      this.connection = connection ;
      this.statement = statement ;
      this.rslt = rslt ;
    }

    public void handle( Long timerId ) {
      logger.warn( "Closing batch result set and statement on timeout" ) ;
      SilentCloser.close( connection, statement, rslt ) ;
    }
  }

  private abstract class BatchHandler implements Handler<Message<JsonObject>> {
    Connection connection ;
    String statementString ;
    List<List<Object>> values ;
    Iterator<List<Object>> valueIterator ;
    TransactionalHandler transaction ;
    long timerId ;
    int timeout ;
    int batchSize ;
    PreparedStatement statement ;
    ResultSet resultSet ;

    BatchHandler( Connection connection,
                  Message<JsonObject> initial,
                  TransactionalHandler transaction ) throws SQLException {
      this.connection = connection ;
      this.transaction = transaction ;
      this.timerId = -1 ;
      this.batchSize = initial.body().getNumber( "batchsize", -1 ).intValue() ;
      if( this.batchSize <= 0 ) this.batchSize = -1 ;
      this.timeout = initial.body().getNumber( "batchtimeout", batchTimeout ).intValue() ;

      // create a List<List<Object>> from the values
      this.values = JsonUtils.arrayNormaliser( initial.body().getArray( "values" ) ) ;
      if( this.values != null ) {
        this.valueIterator = values.iterator() ;
      }
      else {
        this.valueIterator = null ;
      }
      initialiseStatement( initial ) ;
    }

    void initialiseStatement( Message<JsonObject> initial ) throws SQLException {
      this.statement = connection.prepareStatement( initial.body().getString( "stmt" ) ) ;
    }

    abstract JsonObject process() throws SQLException ;

    void store( ArrayList<Map<String,Object>> result,
                LimitedMapListHandler handler ) throws SQLException {
      result.addAll( handler.handle( resultSet ) ) ;
      if( handler.isExpired() ) {
        SilentCloser.close( resultSet ) ;
        resultSet = null ;
      }
    }

    public void handle( final Message<JsonObject> message ) {
      if ( logger.isDebugEnabled() ) {
        logger.debug( "** HANDLE ** " + this.toString() + " (BATCH handler) RECEIVED CALL" ) ;
      }
      if( timerId != -1 ) {
        vertx.cancelTimer( timerId ) ;
      }
      JsonObject reply ;
      try {
        reply = process() ;
        if( resultSet != null || valueIterator.hasNext() ) {
          reply.putString( "status", "more-exist" ) ;
          logger.debug( "BATCH REPLY : " + reply ) ;
          message.reply( reply, this ) ;
          if( transaction == null ) {
            timerId = vertx.setTimer( timeout, new BatchTimeoutHandler( connection, statement, resultSet ) ) ;
          }
          else {
            timerId = vertx.setTimer( timeout, new BatchTimeoutHandler( statement, resultSet ) ) ;
          }
        }
        else if( transaction == null ) {
          SilentCloser.close( connection, statement ) ;
          logger.debug( "BATCH REPLY : " + reply ) ;
          sendOK( message, reply ) ;
        }
        else {
          SilentCloser.close( statement ) ;
          reply.putString( "status", "ok" ) ;
          logger.debug( "BATCH REPLY : " + reply ) ;
          message.reply( reply, transaction ) ;
        }
      }
      catch( SQLException ex ) {
        if( transaction == null ) {
          SilentCloser.close( connection, statement, resultSet ) ;
          sendError( message, "Error performing batch select", ex ) ;
        }
        else {
          try { connection.rollback() ; } catch( SQLException exx ) {}
          SilentCloser.close( connection, statement, resultSet ) ;
          reply = new JsonObject() ;
          reply.putString( "status", "error" ) ;
          reply.putString( "message", "Error performing transactional batch select" ) ;
          logger.error( "Error performing transactional batch select", ex ) ;
          message.reply( reply, transaction ) ;
        }
      }
    }
  }

  private class NonBrokenPMDStatementFiller implements StatementFiller {
    public void fill( PreparedStatement statement, List<Object> params ) throws SQLException {
      new QueryRunner( false ).fillStatement( statement, params.toArray( new Object[] {} ) ) ;
    }
  }

  private class MaybeBrokenPMDStatementFiller implements StatementFiller {
    public void fill( PreparedStatement statement, List<Object> params ) throws SQLException {
      try {
        new QueryRunner( false ).fillStatement( statement, params.toArray( new Object[] {} ) ) ;
      }
      catch( SQLException ex ) {
        logger.error( String.format( "Caught %s trying to fill statement. Assuming broken ParameterMetaData, and switching this instance to pmdKnownBroken='yes'", ex.getMessage() ), ex ) ;
        statementFiller = new BrokenPMDStatementFiller() ;
        new QueryRunner( true ).fillStatement( statement, params.toArray( new Object[] {} ) ) ;
      }
    }
  }

  private class BrokenPMDStatementFiller implements StatementFiller {
    public void fill( PreparedStatement statement, List<Object> params ) throws SQLException {
      new QueryRunner( true ).fillStatement( statement, params.toArray( new Object[] {} ) ) ;
    }
  }

  private interface StatementFiller {
    void fill( PreparedStatement statement, List<Object> params ) throws SQLException ;
  }
}