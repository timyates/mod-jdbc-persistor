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

import org.vertx.java.busmods.BusModBase ;
import org.vertx.java.core.Handler ;

import java.sql.DriverManager ;
import java.sql.SQLException ;

public class JdbcBusMod extends BusModBase {
  private String address ;
  private int maxpool ;
  private String url ;

  public void start() {
    super.start() ;
    address = getOptionalStringConfig( "address", "com.bloidonia.jdbcpersistor" ) ;
    maxpool = getOptionalIntConfig( "maxpool",    20 ) ;
    url     = getOptionalStringConfig( "url",     "jdbc:hsqldb:mem:test" ) ;

    container.deployModule( "vertx.work-queue-v1.1", config, 1, new Handler<String>() {
      public void handle( String response ) {
        container.deployWorkerVerticle( JdbcProcessor.class.getName(), config, maxpool ) ;
      }
    } ) ;
  }

  public void stop() {
    try {
      DriverManager.deregisterDriver( DriverManager.getDriver( url ) ) ;
    } catch( SQLException ex ) {
      logger.info( String.format( "Could not deregister driver: %s", ex.getMessage() ) ) ;
    }
  }
}