/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zeppelin.couchbase;

import com.couchbase.client.core.CouchbaseException;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryResult;
import com.couchbase.client.java.query.N1qlQueryRow;
import com.github.wnameless.json.flattener.JsonFlattener;
import org.apache.zeppelin.interpreter.Interpreter;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterPropertyBuilder;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.interpreter.InterpreterResult.Code;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Couchbase Interpreter for Zeppelin.
 *
 * @author Laurent Doguin
 */
public class CouchbaseInterpreter extends Interpreter {

  private static final Logger LOGGER = LoggerFactory.getLogger(CouchbaseInterpreter.class);

  private static final String DEFAULT_HOST = "localhost";
  private static final String DEFAULT_BUCKET = "default";


  public static final String COUCHBASE_HOST = "couchbase.host";
  public static final String COUCHBASE_BUCKET = "couchbase.bucket";
  public static final String COUCHBASE_PWD = "couchbase.pwd";

  static {
    Interpreter.register(
            "couchbase",
            "couchbase",
            CouchbaseInterpreter.class.getName(),
            new InterpreterPropertyBuilder()
                    .add(COUCHBASE_HOST, DEFAULT_HOST, "Couchbase hosts")
                    .add(COUCHBASE_BUCKET, DEFAULT_BUCKET, "Default bucket name")
                    .add(COUCHBASE_PWD, "", "Default bucket password")
                    .build());
  }

  private CouchbaseCluster cluster;

  private Bucket bucket;

  private static final List<String> NO_COMPLETION = new ArrayList<String>();

  public CouchbaseInterpreter(Properties property) {
    super(property);
  }

  @Override
  public void open() {
    try {
      cluster = CouchbaseCluster.create(getProperty(COUCHBASE_HOST));
      bucket = cluster.openBucket(getProperty(COUCHBASE_BUCKET), getProperty(COUCHBASE_PWD));
    } catch (final Exception e) {
      LOGGER.error("Open connection with Couchbase", e);
    }
  }

  @Override
  public void close() {
    if (bucket != null && !bucket.isClosed()) {
      bucket.close();
    }
    if (cluster != null) {
      cluster.disconnect();
    }
  }

  @Override
  public InterpreterResult interpret(String cmd, InterpreterContext interpreterContext) {
    LOGGER.info("Run N1QL command '" + cmd + "'");
    try {
      N1qlQuery n1qlQuery = N1qlQuery.simple(cmd);
      N1qlQueryResult result = bucket.query(n1qlQuery);
      if (!result.finalSuccess()) {
        final StringBuffer buffer = new StringBuffer();
        Iterator<JsonObject> iter = result.errors().iterator();
        while (iter.hasNext()) {
          JsonObject jo = iter.next();
          buffer.append(jo.toString());
        }
        return new InterpreterResult(Code.ERROR, InterpreterResult.Type.TEXT, buffer.toString());
      }
      Iterator<N1qlQueryRow> iter = result.rows();
      final List<Map<String, Object>> flattenDocs = new LinkedList<>();
      final Set<String> keys = new TreeSet<>();

      //First : get all the keys in order to build an ordered list of the values for each hit
      //
      while (iter.hasNext()) {
        N1qlQueryRow row = iter.next();
        Map<String, Object> flattenMap = JsonFlattener.flattenAsMap(row.value().toString());
        flattenDocs.add(flattenMap);
        for (final String key : flattenMap.keySet()) {
          keys.add(key);
        }
      }

      // Next : build the header of the table
      //
      final StringBuffer buffer = new StringBuffer();
      for (final String key : keys) {
        buffer.append(key).append('\t');
      }

      if (buffer.length() > 0) {
        buffer.replace(buffer.lastIndexOf("\t"), buffer.lastIndexOf("\t") + 1, "\n");
      } else {
        return new InterpreterResult(Code.SUCCESS, InterpreterResult.Type.TEXT, "Empty result");
      }

      // Finally : build the result by using the key set
      //
      for (final Map<String, Object> hit : flattenDocs) {
        for (final String key : keys) {
          final Object val = hit.get(key);
          if (val != null) {
            buffer.append(val);
          }
          buffer.append('\t');
        }
        buffer.replace(buffer.lastIndexOf("\t"), buffer.lastIndexOf("\t") + 1, "\n");
      }

      return new InterpreterResult(Code.SUCCESS, InterpreterResult.Type.TABLE, buffer.toString());
    } catch (final CouchbaseException e) {
      return new InterpreterResult(Code.ERROR, "Error : " + e.getMessage());
    }
  }

  @Override
  public void cancel(InterpreterContext interpreterContext) {
  }

  @Override
  public FormType getFormType() {
    return FormType.SIMPLE;
  }

  @Override
  public int getProgress(InterpreterContext interpreterContext) {
    return 0;
  }

  @Override
  public List<String> completion(String buf, int cursor) {
    return NO_COMPLETION;
  }

}
