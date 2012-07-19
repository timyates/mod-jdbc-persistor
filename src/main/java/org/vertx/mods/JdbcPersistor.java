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

import org.apache.commons.dbutils.QueryRunner ;
import org.apache.commons.dbutils.DbUtils ;
import org.apache.commons.dbutils.handlers.MapListHandler ;

import org.vertx.java.busmods.BusModBase ;
import org.vertx.java.core.AsyncResult ;
import org.vertx.java.core.AsyncResultHandler ;
import org.vertx.java.core.Handler ;
import org.vertx.java.core.eventbus.Message ;
import org.vertx.java.core.impl.BlockingAction ;
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
logger.info( "STARTING! " + eb ) ;
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
    logger.info( "STARTED!" ) ;
  }

  public void stop() {
    logger.info( "STOPPING!" ) ;
    eb.unregisterHandler( address, this ) ;
    if( pool != null ) {
      pool.close() ;
    }
    logger.info( "STOPPED!" ) ;
  }

  /*
  // Insert 3 rows wrapped in a transaction
  {
    action: 'insert',
    stmt:  'INSERT INTO TEST( a, b ) VALUES ( ?, ? )',
    values: [ [ 1, 2 ], [ 2, 3 ], [ 3, 4 ] ]
  }

  // Simple select
  {
    action: 'select',
    stmt:   'SELECT * FROM TEST WHERE a = ?'
    values: [ 1 ]
  }

  // Simpler select
  {
    action: 'select',
    stmt:   'SELECT 1 FROM INFORMATION_SCHEMA.SYSTEM_USERS'
  }
  */


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
      default:
        sendError( message, "Invalid action : " + action ) ;
    }
  }

  private List<Map<String,Object>> processValues( QueryRunner runner, String stmt, JsonArray values ) throws SQLException {
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
      result = runner.query( stmt, new MapListHandler(), params ) ;
    }
    return result ;
  }

  private void doSelect( final Message<JsonObject> message ) {
    new BlockingAction<List<Map<String,Object>>>( (VertxInternal)vertx, new AsyncResultHandler<List<Map<String,Object>>>() {
      public void handle( AsyncResult<List<Map<String,Object>>> result ) {
        if( result.succeeded() ) {
          JsonObject reply = new JsonObject() ;
          JsonArray rows = new JsonArray() ;
          for( Map<String,Object> row : result.result ) {
            rows.addObject( new JsonObject( row ) ) ;
          }
          reply.putArray( "result", rows ) ;
          sendOK( message, reply ) ;
        }
        else {
          sendError( message, "Error with SELECT", result.exception ) ;
        }
      }
    } ) {
      public List<Map<String,Object>> action() throws Exception {
        QueryRunner runner = new QueryRunner( pool ) ;
        JsonArray values = message.body.getArray( "values" ) ;
        if( values == null ) {
          String statement = message.body.getString( "stmt" ) ;
          return runner.query( statement, new MapListHandler() ) ;
        }
        else {
          return processValues( runner, message.body.getString( "stmt" ), values ) ;
        }
      }
    }.run() ;
  }

  private void doUpdate( Message<JsonObject> message ) {

  }
}