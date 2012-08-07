package org.apache.commons.dbutils.handlers ;

import java.sql.ResultSet ;
import java.sql.SQLException ;

import java.util.ArrayList ;
import java.util.List ;
import java.util.Map ;

public class LimitedMapListHandler extends MapListHandler {
  private int limit ;

  public LimitedMapListHandler() {
    this( -1 ) ;
  }

  public LimitedMapListHandler( int limit ) {
    this.limit = limit ;
  }

  @Override
  public List<Map<String,Object>> handle( ResultSet rs ) throws SQLException {
    List<Map<String,Object>> rows = new ArrayList<Map<String,Object>>();
    while( rs.next() && ( limit == -1 || rows.size() < limit ) ) {
      rows.add( this.handleRow( rs ) ) ;
    }
    return rows ;
  }
}