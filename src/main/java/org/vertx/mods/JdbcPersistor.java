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

package org.vertx.mods;

import java.sql.Connection ;
import java.sql.SQLException ;

import java.util.ArrayList ;
import java.util.Iterator ;
import java.util.List ;
import java.util.Map ;

import com.mchange.v2.c3p0.* ;

import org.apache.commons.dbutils.PatchedQueryRunner ;
import org.apache.commons.dbutils.DbUtils ;
import org.apache.commons.dbutils.handlers.MapListHandler ;

import org.vertx.java.busmods.BusModBase ;
import org.vertx.java.core.Handler ;
import org.vertx.java.core.eventbus.Message ;
import org.vertx.java.core.impl.VertxInternal ;
import org.vertx.java.core.json.JsonArray ;
import org.vertx.java.core.json.JsonObject ;

public class JdbcPersistor extends BusModBase implements Handler<Message<JsonObject>> {
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
      case "insert" :
        doInsert( message ) ;
        break ;
      case "execute" :
        doExecute( message ) ;
        break ;
      case "call" :
        doCall( message ) ;
        break ;
      default:
        sendError( message, "Invalid action : " + action ) ;
    }
  }

  private List<Map<String,Object>> processValues( PatchedQueryRunner runner, String stmt, JsonArray values ) throws SQLException {
    Iterator<Object> iter = values.iterator() ;
    Object first = iter.next() ;
    List<Map<String,Object>> result = null ;
    if( first instanceof JsonArray ) { // List of lists...
      result = processValues( runner, stmt, (JsonArray)first ) ;
      while( iter.hasNext() ) {
        result.addAll( processValues( runner, stmt, (JsonArray)iter.next() ) ) ;
      }
    }
    else {
      List<Object> params = new ArrayList<Object>() ;
      params.add( first ) ;
      while( iter.hasNext() ) {
        params.add( iter.next() ) ;
      }
      result = runner.query( stmt, new MapListHandler(), params.toArray( new Object[] {} ) ) ;
    }
    return result ;
  }

  private void doSelect( final Message<JsonObject> message ) {
    try {
      PatchedQueryRunner runner = new PatchedQueryRunner( pool ) ;
      JsonArray values = message.body.getArray( "values" ) ;
      String statement = message.body.getString( "stmt" ) ;
      List<Map<String,Object>> result ;
      if( values == null ) {
        result = runner.query( statement, new MapListHandler() ) ;
      }
      else {
        result = processValues( runner, statement, values ) ;
      }
      JsonObject reply = new JsonObject() ;
      JsonArray rows = new JsonArray() ;
      for( Map<String,Object> row : result ) {
        rows.addObject( new JsonObject( row ) ) ;
      }
      reply.putArray( "result", rows ) ;
      sendOK( message, reply ) ;
    }
    catch( SQLException ex ) {
      sendError( message, "Caught error with SELECT", ex ) ;
    }
  }

  private void doUpdate( final Message<JsonObject> message ) {
    sendError( message, "UPDATE is not yet implemented." ) ;
  }

  private List<Map<String,Object>> processInsertValues( Connection connection, PatchedQueryRunner runner, String stmt, JsonArray values ) throws SQLException {
    Iterator<Object> iter = values.iterator() ;
    Object first = iter.next() ;
    List<Map<String,Object>> result = null ;
    if( first instanceof JsonArray ) { // List of lists...
      result = processInsertValues( connection, runner, stmt, (JsonArray)first ) ;
      while( iter.hasNext() ) {
        result.addAll( processInsertValues( connection, runner, stmt, (JsonArray)iter.next() ) ) ;
      }
    }
    else {
      List<Object> params = new ArrayList<Object>() ;
      params.add( first ) ;
      while( iter.hasNext() ) {
        params.add( iter.next() ) ;
      }
      result = runner.insert( connection, stmt, new MapListHandler(), params.toArray( new Object[] {} ) ) ;
    }
    return result ;
  }

  private void doInsert( final Message<JsonObject> message ) {
    PatchedQueryRunner runner = new PatchedQueryRunner( pool ) ;
    Connection connection = null ;
    try {
      connection = pool.getConnection() ;
      connection.setAutoCommit( false ) ;
      JsonArray values = message.body.getArray( "values" ) ;
      String statement = message.body.getString( "stmt" ) ;
      List<Map<String,Object>> result ;
      if( values == null ) {
        result = runner.insert( connection, statement, new MapListHandler() ) ;
      }
      else {
        result = processInsertValues( connection, runner, statement, values ) ;
      }
      connection.commit() ;
      JsonObject reply = new JsonObject() ;
      JsonArray rows = new JsonArray() ;
      for( Map<String,Object> row : result ) {
        rows.addObject( new JsonObject( row ) ) ;
      }
      reply.putArray( "result", rows ) ;
      sendOK( message, reply ) ;
    }
    catch( SQLException ex ) {
      try {
        connection.rollback() ;
      }
      catch( SQLException ex2 ) {
        logger.error( "Caught exption rolling back commit", ex2 ) ;
      }
      sendError( message, "Error with INSERT", ex ) ;
    }
    finally {
      DbUtils.closeQuietly( connection ) ;
    }
  }

  private void doExecute( final Message<JsonObject> message ) {
    Connection connection = null ;
    try {
      connection = pool.getConnection() ;
      connection.createStatement().execute( message.body.getString( "stmt" ) ) ;
      sendOK( message ) ;
    }
    catch( SQLException ex ) {
      sendError( message, "Error with EXECUTE", ex ) ;
    }
    finally {
      DbUtils.closeQuietly( connection ) ;
    }
  }

  private void doCall( final Message<JsonObject> message ) {
    sendError( message, "CALL is not yet implemented." ) ;
  }
}