// Copyright (c) YugaByte, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied.  See the License for the specific language governing permissions and limitations
// under the License.
//
package com.yugabyte.sample.apps;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Logger;
import com.yugabyte.sample.apps.AppBase.TableOp;
import com.yugabyte.sample.common.SimpleLoadGenerator.Key;

/**
 * This workload writes and reads some random string keys from a postgresql table.
 */
public class SqlInserts extends AppBase {
  private static final Logger LOG = Logger.getLogger(SqlInserts.class);

  // Static initialization of this workload's config. These are good defaults for getting a decent
  // read dominated workload on a reasonably powered machine. Exact IOPS will of course vary
  // depending on the machine and what resources it has to spare.
  static {
    // Disable the read-write percentage.
    appConfig.readIOPSPercentage = -1;
    // Set the read and write threads to 2 each.
    appConfig.numReaderThreads = 2;
    appConfig.numWriterThreads = 2;
    // The number of keys to read.
    appConfig.numKeysToRead = NUM_KEYS_TO_READ_FOR_YSQL_AND_YCQL;
    // The number of keys to write. This is the combined total number of inserts and updates.
    appConfig.numKeysToWrite = NUM_KEYS_TO_WRITE_FOR_YSQL_AND_YCQL;
    // The number of unique keys to write. This determines the number of inserts (as opposed to
    // updates).
    appConfig.numUniqueKeysToWrite = NUM_UNIQUE_KEYS_FOR_YSQL_AND_YCQL;
  }

  // The default table name to create and use for CRUD ops.
  private static final String DEFAULT_TABLE_NAME = "api_oauth_access_token";

    @Override protected String getKeyspace() {
        return super.getKeyspace();
    }

    private static final String DEFAULT_TABLE_DEFN = "CREATE TABLE IF NOT EXISTS api_oauth_access_token\n"
            + "(\n"
            + "  k                      text PRIMARY KEY,\n"
            + "  v                      text ,\n"
            + "  access_token_id        bigserial not null,\n"
            + "  access_token           varchar(2048) not null default '15eujNJCmhVdqFN5T3dPkGEh6hfo3kTbSn',\n"
            + "  access_token_hashed    varchar(512) not null default '8bad98be96213362ff4aa8c638f0e0c4c1aa2ba032c86de7ceab2c38d9d1986f',\n"
            + "  additional_information varchar(1024) default 'Curabitur at ipsum ac tellus semper interdum.',\n"
            + "  client_id              varchar(255) not null default 'f0b015f4-673f-4ca3-9e2a-ecc0aec3d9c7',\n"
            + "  expires_in             timestamptz not null default '2013-11-16T04:40:29Z',\n"
            + "  grant_type             bigint not null default 56,\n"
            + "  refresh_token          varchar(2048) not null default '1FhDfbBiBVXWGZfXTG6AhYEtte7CDmpd4y',\n"
            + "  refresh_token_hashed   varchar(512) not null default '30799c54ca6547a63a5cec4afe2a294c039a028e46038e93d6dcd1bbdda62c4f',\n"
            + "  scope                  bigint default 95,\n"
            + "  userid                 bigint not null default 139238\n"
            + ") split into 40 tablets;";

    // The shared prepared select statement for fetching the data.
  private volatile Connection selConnection = null;
  private volatile PreparedStatement preparedSelect = null;

  // The shared prepared insert statement for inserting the data.
  private volatile Connection insConnection = null;
  private volatile PreparedStatement preparedInsert = null;

  // Lock for initializing prepared statement objects.
  private static final Object prepareInitLock = new Object();

  public SqlInserts() {
    buffer = new byte[appConfig.valueSize];
  }

  /**
   * Drop the table created by this app.
   */
  @Override
  public void dropTable() throws Exception {
    try (Connection connection = getPostgresConnection()) {
      connection.createStatement().execute("DROP TABLE IF EXISTS " + getTableName());
      LOG.info(String.format("Dropped table: %s", getTableName()));
    }
  }

  @Override
  public void createTablesIfNeeded(TableOp tableOp) throws Exception {
    try (Connection connection = getPostgresConnection()) {

      // (Re)Create the table (every run should start cleanly with an empty table).
      if (tableOp.equals(TableOp.DropTable)) {
          connection.createStatement().execute(
              String.format("DROP TABLE IF EXISTS %s", getTableName()));
          LOG.info("Dropping any table(s) left from previous runs if any");
      }
      connection.createStatement().execute(
          String.format(getTableDefnString(), getTableName()));

      LOG.info(String.format("Created table: %s", getTableName()));
      if (tableOp.equals(TableOp.TruncateTable)) {
      	connection.createStatement().execute(
            String.format("TRUNCATE TABLE %s", getTableName()));
        LOG.info(String.format("Truncated table: %s", getTableName()));
      }
    }
  }

  public String getTableDefnString() {
      String tableName = appConfig.createTableDefn != null ? appConfig.createTableDefn : DEFAULT_TABLE_DEFN;
      return tableName.toLowerCase();
  }
  public String getTableName() {
    String tableName = appConfig.tableName != null ? appConfig.tableName : DEFAULT_TABLE_NAME;
    return tableName.toLowerCase();
  }

  private PreparedStatement getPreparedSelect() throws Exception {
    if (preparedSelect == null) {
      close(selConnection);
      selConnection = getPostgresConnection();
      preparedSelect = selConnection.prepareStatement(
          String.format("SELECT k, v FROM %s WHERE k = ?;", getTableName()));
    }
    return preparedSelect;
  }

  @Override
  public long doRead() {
    Key key = getSimpleLoadGenerator().getKeyToRead();
    if (key == null) {
      // There are no keys to read yet.
      return 0;
    }

    try {
      PreparedStatement statement = getPreparedSelect();
      statement.setString(1, key.asString());
      try (ResultSet rs = statement.executeQuery()) {
        if (!rs.next()) {
          LOG.error("Read key: " + key.asString() + " expected 1 row in result, got 0");
          return 0;
        }

        if (!key.asString().equals(rs.getString("k"))) {
          LOG.error("Read key: " + key.asString() + ", got " + rs.getString("k"));
        }
        LOG.debug("Read key: " + key.toString());

        key.verify(rs.getString("v"));

        if (rs.next()) {
          LOG.error("Read key: " + key.asString() + " expected 1 row in result, got more");
          return 0;
        }
      }
    } catch (Exception e) {
      LOG.info("Failed reading value: " + key.getValueStr(), e);
      close(preparedSelect);
      preparedSelect = null;
      return 0;
    }
    return 1;
  }

  private PreparedStatement getPreparedInsert() throws Exception {
    if (preparedInsert == null) {
      close(insConnection);
      insConnection = getPostgresConnection();
      preparedInsert = insConnection.prepareStatement(
          String.format("INSERT INTO %s (k, v) VALUES (?, ?);", getTableName()));
    }
    return preparedInsert;
  }

  @Override
  public long doWrite(int threadIdx) {
    Key key = getSimpleLoadGenerator().getKeyToWrite();
    if (key == null) {
      return 0;
    }

    int result = 0;
    try {
      PreparedStatement statement = getPreparedInsert();
      // Prefix hashcode to ensure generated keys are random and not sequential.
      statement.setString(1, key.asString());
      statement.setString(2, key.getValueStr());
      result = statement.executeUpdate();
      LOG.debug("Wrote key: " + key.asString() + ", " + key.getValueStr() + ", return code: " +
          result);
      getSimpleLoadGenerator().recordWriteSuccess(key);
    } catch (Exception e) {
      getSimpleLoadGenerator().recordWriteFailure(key);
      LOG.info("Failed writing key: " + key.asString(), e);
      close(preparedInsert);
      preparedInsert = null;
    }
    return result;
  }

  @Override
  public List<String> getWorkloadDescription() {
    return Arrays.asList(
        "Sample key-value app built on PostgreSQL with concurrent readers and writers. The app inserts unique string keys",
        "each with a string value to a postgres table with an index on the value column.",
        "There are multiple readers and writers that update these keys and read them",
        "for a specified number of operations,default value for read ops is "+AppBase.appConfig.numKeysToRead+" and write ops is "+AppBase.appConfig.numKeysToWrite+", with the readers query the keys by the associated values that are",
        "indexed. Note that the number of reads and writes to perform can be specified as",
        "a parameter, user can run read/write(both) operations indefinitely by passing -1 to --num_reads or --num_writes or both.");
  }

  @Override
  public List<String> getWorkloadOptionalArguments() {
    return Arrays.asList(
        "--num_unique_keys " + appConfig.numUniqueKeysToWrite,
        "--num_reads " + appConfig.numKeysToRead,
        "--num_writes " + appConfig.numKeysToWrite,
        "--num_threads_read " + appConfig.numReaderThreads,
        "--num_threads_write " + appConfig.numWriterThreads,
        "--load_balance " + appConfig.loadBalance,
        "--topology_keys " + appConfig.topologyKeys,
        "--debug_driver " + appConfig.enableDriverDebug);
  }
}
