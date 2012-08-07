/*
 * Copyright 2011-2012 the original author or authors.
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
import java.sql.PreparedStatement ;
import java.sql.ResultSet ;
import java.sql.SQLException ;
import java.sql.Statement ;

import java.util.ArrayList ;
import java.util.Iterator ;
import java.util.List ;
import java.util.Map ;

import org.apache.commons.dbutils.QueryRunner ;
import org.apache.commons.dbutils.ResultSetHandler ;
import org.apache.commons.dbutils.handlers.MapListHandler ;
import org.apache.commons.dbutils.handlers.LimitedMapListHandler ;

import org.vertx.java.busmods.BusModBase ;
import org.vertx.java.core.Handler ;
import org.vertx.java.core.eventbus.Message ;
import org.vertx.java.core.impl.VertxInternal ;
import org.vertx.java.core.json.JsonArray ;
import org.vertx.java.core.json.JsonObject ;

public class JdbcBusMod extends BusModBase implements Handler<Message<JsonObject>> {
  private String address ;
  
  private String driver ;
  private String url ;
  private String username ;
  private String password ;
  private int    minpool ;
  private int    maxpool ;
  private int    acquire ;

  private ComboPooledDataSource pool = null ;

  public void start() {
    super.start() ;
    address   = getOptionalStringConfig( "address", "vertx.jdbcpersistor" ) ;

    driver    = getOptionalStringConfig( "driver",   "org.hsqldb.jdbcDriver" ) ;
    url       = getOptionalStringConfig( "url",      "jdbc:hsqldb:mem:test"  ) ;
    username  = getOptionalStringConfig( "username", ""                      ) ;
    password  = getOptionalStringConfig( "password", ""                      ) ;

    minpool   = getOptionalIntConfig( "minpool",    5  ) ;
    maxpool   = getOptionalIntConfig( "maxpool",    20 ) ;
    acquire   = getOptionalIntConfig( "acquire",    5 ) ;

    try {
      pool = new ComboPooledDataSource() ;
      pool.setDriverClass( driver ) ;
      pool.setJdbcUrl( url ) ;
      pool.setUser( username ) ;
      pool.setPassword( password ) ;
      pool.setMinPoolSize( minpool ) ;
      pool.setMaxPoolSize( maxpool ) ;
      pool.setAcquireIncrement( acquire ) ;
    }
    catch( Exception e ) {
      logger.error( "Failed to create jdbc pool", e ) ;
    }
    eb.registerHandler( address, this ) ;
  }

  public void stop() {
    eb.unregisterHandler( address, this ) ;
    if( pool != null ) {
      pool.close() ;
    }
  }

  public void handle( final Message<JsonObject> message ) {
    String action = message.body.getString( "action" ) ;
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
      default:
        sendError( message, "Invalid action : " + action ) ;
    }
  }

  /****************************************************************************
   **
   **  Select handling
   **
   ****************************************************************************/

  private void doSelect( final Message<JsonObject> message ) {
    Connection connection = null ;
    try {
      connection = pool.getConnection() ;
      doSelect( message, connection ) ;
    }
    catch( SQLException ex ) {
      sendError( message, "Caught error with SELECT", ex ) ;
    }
    finally {
      SilentCloser.close( connection ) ;
    }
  }

  private List<Map<String,Object>> processValues( Connection connection, QueryRunner runner, String stmt, JsonArray values ) throws SQLException {
    Iterator<Object> iter = values.iterator() ;
    Object first = iter.next() ;
    List<Map<String,Object>> result = null ;
    if( first instanceof JsonArray ) { // List of lists...
      result = processValues( connection, runner, stmt, (JsonArray)first ) ;
      while( iter.hasNext() ) {
        result.addAll( processValues( connection, runner, stmt, (JsonArray)iter.next() ) ) ;
      }
    }
    else {
      List<Object> params = new ArrayList<Object>() ;
      params.add( first ) ;
      while( iter.hasNext() ) {
        params.add( iter.next() ) ;
      }
      result = runner.query( connection, stmt, new MapListHandler(), params.toArray( new Object[] {} ) ) ;
    }
    return result ;
  }

  private void doSelect( final Message<JsonObject> message, Connection connection ) throws SQLException {
    doSelect( message, connection, null ) ;
  }

  private void doSelect( final Message<JsonObject> message, Connection connection, TransactionalHandler transaction ) throws SQLException {
    JsonArray values = message.body.getArray( "values" ) ;
    String statementString = message.body.getString( "stmt" ) ;
    int batchSize = message.body.getNumber( "batchsize", -1 ).intValue() ;
    if( batchSize <= 0 ) batchSize = -1 ;
    LimitedMapListHandler handler = new LimitedMapListHandler( batchSize ) ;
    List<Map<String,Object>> result ;
    QueryRunner runner = new QueryRunner() ;
    if( values != null ) {
      result = processValues( connection, runner, statementString, values ) ;
    }
    else {
      result = runner.query( connection, statementString, handler ) ;
    }

    JsonObject reply = new JsonObject() ;
    JsonArray rows = new JsonArray() ;
    for( Map<String,Object> row : result ) {
      rows.addObject( new JsonObject( row ) ) ;
    }
    reply.putArray( "result", rows ) ;
    sendOK( message, reply ) ;
  }

  /****************************************************************************
   **
   **  Select Batch handling
   **
   ****************************************************************************/

  private class BatchTimeoutHandler implements Handler<Long> {
    Connection connection ;
    Statement statement ;
    ResultSet rslt ;
    boolean inTransaction ;

    BatchTimeoutHandler( Connection conn, Statement statement, ResultSet rslt, boolean inTransaction ) {
      this.connection = connection ;
      this.statement = statement ;
      this.rslt = rslt ;
      this.inTransaction = inTransaction ;
    }

    public void handle( Long timerId ) {
      logger.warn( "Closing batch result set and statement on timeout" ) ;
      if( inTransaction ) {
        SilentCloser.close( statement, rslt ) ;
      }
      else {
        SilentCloser.close( connection, statement, rslt ) ;
      }
    }
  }

  private class BatchHandler implements Handler<Message<JsonObject>> {
    Connection connection ;
    Statement statement ;
    ResultSet rslt ;
    boolean inTransaction ;
    long timerId ;
    int timeout ;

    BatchHandler( Connection conn, Statement statement, ResultSet rslt, boolean inTransaction, long timerId, int timeout ) {
      this.connection = connection ;
      this.statement = statement ;
      this.rslt = rslt ;
      this.inTransaction = inTransaction ;
      this.timerId = timerId ;
      this.timeout = timeout ;
    }

    public void handle( final Message<JsonObject> message ) {
    }
  }

  /****************************************************************************
   **
   **  Execute handling
   **
   ****************************************************************************/

  private void doExecute( final Message<JsonObject> message ) {
    Connection connection = null ;
    try {
      connection = pool.getConnection() ;
      doExecute( message, connection ) ;
    }
    catch( SQLException ex ) {
      sendError( message, "Caught error with EXECUTE", ex ) ;
    }
    finally {
      SilentCloser.close( connection ) ;
    }
  }

  private void doExecute( final Message<JsonObject> message, Connection conn ) throws SQLException {
    doExecute( message, conn, null ) ;
  }

  private void doExecute( final Message<JsonObject> message, Connection conn, TransactionalHandler transaction ) throws SQLException {
    Connection connection = null ;
    Statement statement = null ;
    try {
      connection = pool.getConnection() ;
      statement = connection.createStatement() ;
      statement.execute( message.body.getString( "stmt" ) ) ;
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

  private List<Map<String,Object>> processInsertValues( PreparedStatement stmt, QueryRunner qr, MapListHandler handler, JsonArray values ) throws SQLException {
    Iterator<Object> iter = values.iterator() ;
    Object first = iter.next() ;
    List<Map<String,Object>> result = null ;
    if( first instanceof JsonArray ) { // List of lists...
      result = processInsertValues( stmt, qr, handler, (JsonArray)first ) ;
      while( iter.hasNext() ) {
        result.addAll( processInsertValues( stmt, qr, handler, (JsonArray)iter.next() ) ) ;
      }
    }
    else {
      result = new ArrayList<Map<String,Object>>() ;
      List<Object> params = new ArrayList<Object>() ;
      params.add( first ) ;
      while( iter.hasNext() ) {
        params.add( iter.next() ) ;
      }
      qr.fillStatement( stmt, params.toArray( new Object[] {} ) ) ;
      stmt.executeUpdate() ;
      ResultSet rslt = stmt.getGeneratedKeys() ;
      result.addAll( handler.handle( rslt ) ) ;
      SilentCloser.close( rslt ) ;
    }
    return result ;
  }

  private int processUpdateValues( PreparedStatement stmt, QueryRunner qr, JsonArray values ) throws SQLException {
    Iterator<Object> iter = values.iterator() ;
    Object first = iter.next() ;
    int result = 0 ;
    if( first instanceof JsonArray ) { // List of lists...
      result += processUpdateValues( stmt, qr, (JsonArray)first ) ;
      while( iter.hasNext() ) {
        result += processUpdateValues( stmt, qr, (JsonArray)iter.next() ) ;
      }
    }
    else {
      List<Object> params = new ArrayList<Object>() ;
      params.add( first ) ;
      while( iter.hasNext() ) {
        params.add( iter.next() ) ;
      }
      qr.fillStatement( stmt, params.toArray( new Object[] {} ) ) ;
      result += stmt.executeUpdate() ;
    }
    return result ;
  }

  private void doUpdate( final Message<JsonObject> message, boolean insert ) {
    Connection connection = null ;
    try {
      connection = pool.getConnection() ;
      connection.setAutoCommit( false ) ;
      doUpdate( message, connection, insert ) ;
      connection.commit() ;
    }
    catch( SQLException ex ) {
      sendError( message, "Caught error with UPDATE.  Rolling back...", ex ) ;
      try { connection.rollback() ; }
      catch( SQLException exx ) {
        logger.error( "Failed to rollback", exx ) ;
      }
    }
    finally {
      SilentCloser.close( connection ) ;
    }
  }

  private void doUpdate( final Message<JsonObject> message, Connection connection, boolean insert ) throws SQLException {
    doUpdate( message, connection, insert, null ) ;
  }

  private void doUpdate( final Message<JsonObject> message, Connection connection, boolean insert, TransactionalHandler transaction ) throws SQLException {
    JsonArray values = message.body.getArray( "values" ) ;
    String statementString = message.body.getString( "stmt" ) ;
    PreparedStatement stmt = null ;
    try {
      if( insert ) {
        MapListHandler handler = new MapListHandler() ;
        List<Map<String,Object>> result ;
        stmt = connection.prepareStatement( statementString, Statement.RETURN_GENERATED_KEYS ) ;
        if( values != null ) {
          result = processInsertValues( stmt, new QueryRunner(), handler, values ) ;
        }
        else {
          stmt.executeUpdate();
          ResultSet rslt = stmt.getGeneratedKeys();
          result = handler.handle( rslt ) ;
          SilentCloser.close( rslt ) ;
        }

        JsonObject reply = new JsonObject() ;
        JsonArray rows = new JsonArray() ;
        for( Map<String,Object> row : result ) {
          rows.addObject( new JsonObject( row ) ) ;
        }
        reply.putArray( "result", rows ) ;
        logger.warn( "INSERT RETURNING " + reply ) ;
        if( transaction == null ) {
          sendOK( message, reply ) ;
        }
        else {
          reply.putString( "status", "ok" ) ;
          message.reply( reply, transaction ) ;
        }
      }
      else {
        stmt = connection.prepareStatement( statementString ) ;
        int nRows = 0 ;
        if( values != null ) {
          nRows = processUpdateValues( stmt, new QueryRunner(), values ) ;
        }
        else {
          nRows += stmt.executeUpdate();
        }
        JsonObject reply = new JsonObject() ;
        reply.putNumber( "updated", nRows ) ;
        if( transaction == null ) {
          sendOK( message, reply ) ;
        }
        else {
          reply.putString( "status", "ok" ) ;
          message.reply( reply, transaction ) ;
        }
      }
    }
    finally {
      SilentCloser.close( stmt ) ;
    }
  }

  /****************************************************************************
   **
   **  Transaction handling
   **
   ****************************************************************************/

  private void doTransaction( final Message<JsonObject> message ) {
    Connection connection = null ;
    try {
      connection = pool.getConnection() ;
      connection.setAutoCommit( false ) ;
      doTransaction( message, connection ) ;
    }
    catch( SQLException ex ) {
      sendError( message, "Caught exception in TRANSACTION.  Rolling back...", ex ) ;
      try { connection.rollback() ; }
      catch( SQLException exx ) {
        logger.error( "Failed to rollback", exx ) ;
      }
      SilentCloser.close( connection ) ;
    }
  }

  private void doTransaction( final Message<JsonObject> message, final Connection connection ) {
    JsonObject reply = new JsonObject() ;
    reply.putString( "status", "ok" ) ;

    int timeout = message.body.getNumber( "timeout", 10000 ).intValue() ;
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

    public void handle( final Message<JsonObject> message ) {
      vertx.cancelTimer( timerId ) ;
      String action = message.body.getString( "action" ) ;
      logger.warn( "TransactionHandler processing " + action ) ;
      if( action == null ) {
        sendError( message, "action must be specified" ) ;
      }
      try {
        switch( action ) {
          case "select" :
            doSelect( message, connection, this ) ;
            timerId = vertx.setTimer( timeout, new TransactionTimeoutHandler( connection ) ) ;
            break ;
          case "execute" :
            doExecute( message, connection, this ) ;
            timerId = vertx.setTimer( timeout, new TransactionTimeoutHandler( connection ) ) ;
            break ;
          case "update" :
            doUpdate( message, connection, false, this ) ;
            timerId = vertx.setTimer( timeout, new TransactionTimeoutHandler( connection ) ) ;
            break ;
          case "insert" :
            doUpdate( message, connection, true, this ) ;
            timerId = vertx.setTimer( timeout, new TransactionTimeoutHandler( connection ) ) ;
            break ;
          case "commit" :
            doCommit( message ) ;
            break ;
          case "rollback" :
            doRollback( message ) ;
            break ;
          default:
            sendError( message, "Invalid action : " + action + ". Rolling back." ) ;
            doRollback( null ) ;
        }
      }
      catch( SQLException ex ) {
        sendError( message, "Error performing " + action + ".  Rolling back.", ex ) ;
        doRollback( null ) ;
      }

    }

    private void doCommit( final Message<JsonObject> message ) {
      try {
        connection.commit() ;
        if( message != null ) sendOK( message ) ;
      }
      catch( SQLException ex ) { logger.error( "Failed to commit", ex ) ; }
      finally { SilentCloser.close( connection ) ; }
    }

    private void doRollback( final Message<JsonObject> message ) {
      try {
        connection.rollback() ;
        if( message != null ) sendOK( message ) ;
      }
      catch( SQLException ex ) { logger.error( "Failed to rollback", ex ) ; }
      finally { SilentCloser.close( connection ) ; }
    }
  }
}