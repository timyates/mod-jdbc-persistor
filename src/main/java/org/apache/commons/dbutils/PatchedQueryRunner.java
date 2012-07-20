/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Patch applied from https://issues.apache.org/jira/browse/DBUTILS-87 to
 * return Primary keys on insert
 */
package org.apache.commons.dbutils ;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import javax.sql.DataSource;

public class PatchedQueryRunner extends QueryRunner {
  public PatchedQueryRunner(DataSource ds) {
    super(ds);
  }

  public <T> T insert(String sql, ResultSetHandler<T> rsh) throws SQLException {
    return insert(this.prepareConnection(), true, sql, rsh, (Object[]) null);
  }

  public <T> T insert(String sql, ResultSetHandler<T> rsh, Object... params) throws SQLException {
    return insert(this.prepareConnection(), true, sql, rsh, params);
  }
   
  public <T> T insert(Connection conn, String sql, ResultSetHandler<T> rsh) throws SQLException {
    return insert(conn, false, sql, rsh, (Object[]) null);
  }
   
  public <T> T insert(Connection conn, String sql, ResultSetHandler<T> rsh, Object... params) throws SQLException {
    return insert(conn, false, sql, rsh, params);
  }

  private <T> T insert(Connection conn, boolean closeConn, String sql, ResultSetHandler<T> rsh, Object... params) throws SQLException {
    if (conn == null) {
      throw new SQLException("Null connection");
    }
    if (sql == null) {
      if (closeConn) {
        close(conn);
      }
      throw new SQLException("Null SQL statement");
    }
    if (rsh == null) {
      if (closeConn) {
        close(conn);
      }
      throw new SQLException("Null ResultSetHandler");
    }

    PreparedStatement stmt = null;
    T generatedKeys = null;
    try {
      stmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
      this.fillStatement(stmt, params);
      stmt.executeUpdate();
      ResultSet resultSet = stmt.getGeneratedKeys();
      generatedKeys = rsh.handle(resultSet);
    } catch (SQLException e) {
      this.rethrow(e, sql, params);
    } finally {
      close(stmt);
      if (closeConn) {
        close(conn);
      }
    }
    return generatedKeys;
  }
}