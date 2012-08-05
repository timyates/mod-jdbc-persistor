package org.vertx.mods ;

import java.sql.Connection ;
import java.sql.Statement ;
import java.sql.ResultSet ;
import java.sql.SQLException ;

import java.util.ArrayList ;
import java.util.Iterator ;
import java.util.List ;
import java.util.Map ;

import com.mchange.v2.c3p0.* ;

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
      case "update" :
        doUpdate( message ) ;
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
      close( connection ) ;
    }
  }

  private void doSelect( final Message<JsonObject> message, Connection conn ) {
    sendError( message, "SELECT is not yet implemented." ) ;
  }

  private void doUpdate( final Message<JsonObject> message ) {
    Connection connection = null ;
    try {
      connection = pool.getConnection() ;
      connection.setAutoCommit( false ) ;
      doUpdate( message, connection ) ;
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
      close( connection ) ;
    }
  }

  private void doUpdate( final Message<JsonObject> message, Connection connection ) {
    sendError( message, "UPDATE is not yet implemented." ) ;
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
      close( connection ) ;
    }
  }

  private void doTransaction( final Message<JsonObject> message, final Connection connection ) {
    JsonObject reply = new JsonObject() ;
    reply.putString( "status", "ok" ) ;
    int timeout = message.body.getNumber( "timeout", 10000 ).intValue() ;
    // set a timer to rollback and close
    final long timerId = vertx.setTimer( timeout, new TransactionTimeoutHandler( connection ) ) ;

    // reply with the handler to continue with
    message.reply( reply, new TransactionalHandler( connection, timerId, timeout ) ) ;
  }
  
  private class BatchHandler implements Handler<Message<JsonObject>> {
    Connection connection ;
    ResultSet rslt ;

    BatchHandler( Connection conn, ResultSet rslt ) {
      this.connection = connection ;
      this.rslt = rslt ;
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
        connection.close() ;   
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
        case "update" :
          doUpdate( message, connection ) ;
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
          doRollback( message ) ;
      }
    }

    private void doCommit( final Message<JsonObject> message ) {
      try { connection.commit() ; }
      catch( SQLException ex ) { logger.error( "Failed to commit", ex ) ; }
      finally { close( connection ) ; }
    }

    private void doRollback( final Message<JsonObject> message ) {
      try { connection.rollback() ; }
      catch( SQLException ex ) { logger.error( "Failed to rollback", ex ) ; }
      finally { close( connection ) ; }
    }
  }

  private void close( Connection conn )                 { close( conn, null, null ) ; }
  private void close( Statement stmt  )                 { close( null, stmt, null ) ; }
  private void close( ResultSet rslt  )                 { close( null, null, rslt ) ; }
  private void close( Connection conn, Statement stmt ) { close( conn, stmt, null ) ; }
  private void close( Statement stmt, ResultSet rslt )  { close( null, stmt, rslt ) ; }
  private void close( Connection conn, Statement stmt, ResultSet rslt ) {
    try { if( rslt != null ) rslt.close() ; }
    catch( SQLException ex ) { logger.error( "Problems closing result set", ex ) ; }
    try { if( stmt != null ) stmt.close() ; }
    catch( SQLException ex ) { logger.error( "Problems closing statement set", ex ) ; }
    try { if( conn != null ) conn.close() ; }
    catch( SQLException ex ) { logger.error( "Problems closing connection set", ex ) ; }
  }
}