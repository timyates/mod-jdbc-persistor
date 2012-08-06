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