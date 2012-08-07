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

import java.sql.Connection ;
import java.sql.PreparedStatement ;
import java.sql.ResultSet ;
import java.sql.SQLException ;
import java.sql.Statement ;

import org.vertx.java.core.logging.Logger;

public class SilentCloser {
  public static void close( Connection conn )                 { close( conn, null, null ) ; }
  public static void close( Statement stmt  )                 { close( null, stmt, null ) ; }
  public static void close( ResultSet rslt  )                 { close( null, null, rslt ) ; }
  public static void close( Connection conn, Statement stmt ) { close( conn, stmt, null ) ; }
  public static void close( Statement stmt, ResultSet rslt )  { close( null, stmt, rslt ) ; }

  public static void close( Connection conn, Statement stmt, ResultSet rslt ) {
    try { if( rslt != null ) rslt.close() ; } catch( SQLException ex ) {}
    try { if( stmt != null ) stmt.close() ; } catch( SQLException ex ) {}
    try { if( conn != null ) conn.close() ; } catch( SQLException ex ) {}
  }
}