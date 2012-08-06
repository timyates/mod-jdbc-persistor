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
import org.apache.commons.dbutils.DbUtils ;
import org.apache.commons.dbutils.handlers.MapListHandler ;

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

  private void doSelect( final Message<JsonObject> message, Connection conn ) {
    sendError( message, "SELECT is not yet implemented." ) ;
  }

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

  private void doExecute( final Message<JsonObject> message, Connection conn ) {
    Connection connection = null ;
    Statement statement = null ;
    try {
      connection = pool.getConnection() ;
      statement = connection.createStatement() ;
      statement.execute( message.body.getString( "stmt" ) ) ;
      sendOK( message ) ;
    }
    catch( SQLException ex ) {
      sendError( message, "Error with EXECUTE", ex ) ;
    }
    finally {
      SilentCloser.close( connection, statement ) ;
    }
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
    JsonArray values = message.body.getArray( "values" ) ;
    String statementString = message.body.getString( "stmt" ) ;
    PreparedStatement stmt = null ;
    ResultSet rslt = null ;
    try {
      if( insert ) {
        ResultSetHandler<List<Map<String,Object>>> handler = new MapListHandler() ;
        List<Map<String,Object>> result ;
        QueryRunner qr = new QueryRunner() ;
        stmt = connection.prepareStatement( statementString, Statement.RETURN_GENERATED_KEYS ) ;
        if( values != null ) {
          result = new ArrayList<Map<String,Object>>() ;
          for( Object params : values ) {
            qr.fillStatement( stmt, params ) ;
            stmt.executeUpdate() ;
            rslt = stmt.getGeneratedKeys() ;
            result.addAll( handler.handle( rslt ) ) ;
          }
        }
        else {
          stmt.executeUpdate();
          rslt = stmt.getGeneratedKeys();
          result = handler.handle( rslt ) ;
        }

        JsonObject reply = new JsonObject() ;
        JsonArray rows = new JsonArray() ;
        for( Map<String,Object> row : result ) {
          rows.addObject( new JsonObject( row ) ) ;
        }
        reply.putArray( "result", rows ) ;
        sendOK( message, reply ) ;
      }
      else {
        connection.createStatement() ;
        sendOK( message ) ;
      }
    }
    finally {
      SilentCloser.close( stmt, rslt ) ;
    }
  }

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
    }
    finally {
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
      if( action == null ) {
        sendError( message, "action must be specified" ) ;
      }
      switch( action ) {
        case "select" :
          doSelect( message, connection ) ;
          timerId = vertx.setTimer( timeout, new TransactionTimeoutHandler( connection ) ) ;
          break ;
        case "execute" :
          doExecute( message, connection ) ;
          timerId = vertx.setTimer( timeout, new TransactionTimeoutHandler( connection ) ) ;
          break ;
        case "update" :
          try {
            doUpdate( message, connection, false ) ;
            timerId = vertx.setTimer( timeout, new TransactionTimeoutHandler( connection ) ) ;
          }
          catch( SQLException ex ) {
            sendError( message, "Error performing insert.  Rolling back.", ex ) ;
            doRollback( null ) ;
          }
          break ;
        case "insert" :
          try {
            doUpdate( message, connection, true ) ;
            timerId = vertx.setTimer( timeout, new TransactionTimeoutHandler( connection ) ) ;
          }
          catch( SQLException ex ) {
            sendError( message, "Error performing insert.  Rolling back.", ex ) ;
            doRollback( null ) ;
          }
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