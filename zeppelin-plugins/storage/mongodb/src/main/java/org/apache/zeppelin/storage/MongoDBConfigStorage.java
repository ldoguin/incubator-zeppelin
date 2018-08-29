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


package org.apache.zeppelin.storage;

import static com.mongodb.client.model.Filters.eq;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.UpdateOptions;
import org.apache.zeppelin.conf.ZeppelinConfiguration;
import org.apache.zeppelin.interpreter.InterpreterInfoSaving;
import org.apache.zeppelin.notebook.NotebookAuthorizationInfoSaving;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * It could be used either local file system or hadoop distributed file system,
 * because FileSystem support both local file system and hdfs.
 *
 */
public class MongoDBConfigStorage extends ConfigStorage {

  private static final Logger LOGGER = LoggerFactory.getLogger(FileSystemConfigStorage.class);

  private MongoClient mongo;
  private MongoDatabase db;
  private MongoCollection<Document> collection;
  private String interpreterSettingPath;
  private String authorizationPath;
  private String credentialPath;

  public MongoDBConfigStorage(ZeppelinConfiguration zConf) throws IOException {
    super(zConf);
    this.mongo = new MongoClient(new MongoClientURI(zConf.getMongoUri()));
    this.db = mongo.getDatabase(zConf.getMongoDatabase());
    this.collection = db.getCollection(zConf.getMongoStorageCollection());
    this.interpreterSettingPath = "interpreterSetting";
    this.authorizationPath = "authorization";
    this.credentialPath = "credentials";
  }

  @Override
  public void save(InterpreterInfoSaving settingInfos) throws IOException {
    LOGGER.info("Save Interpreter Settings to MongoDB at " + interpreterSettingPath);
    mongoSave(interpreterSettingPath, settingInfos.toJson());
  }

  @Override
  public InterpreterInfoSaving loadInterpreterSettings() throws IOException {
    Document doc = mongoGet(interpreterSettingPath);
    if (doc == null) {
      LOGGER.warn("Interpreter Setting file {} does not exist in MongoDB", interpreterSettingPath);
      return null;
    }
    LOGGER.info("Load Interpreter Setting from MongoDB: " + interpreterSettingPath);
    return buildInterpreterInfoSaving(doc.getString("conf"));
  }

  public void save(NotebookAuthorizationInfoSaving authorizationInfoSaving) throws IOException {
    LOGGER.info("Save notebook authorization to MongoDB: " + authorizationPath);
    mongoSave(authorizationPath, authorizationInfoSaving.toJson());
  }

  @Override
  public NotebookAuthorizationInfoSaving loadNotebookAuthorization() throws IOException {
    Document doc = mongoGet(authorizationPath);
    if (doc == null) {
      LOGGER.warn("Notebook Authorization file {} does not exist in MongoDB", authorizationPath);
      return null;
    }
    LOGGER.info("Load notebook authorization from MongoDB: " + authorizationPath);
    return NotebookAuthorizationInfoSaving.fromJson(doc.getString("conf"));
  }

  @Override
  public String loadCredentials() throws IOException {
    Document doc = mongoGet(credentialPath);
    if (doc == null) {
      LOGGER.warn("Credential file {} does not exist in MongoD", credentialPath);
      return null;
    }
    LOGGER.info("Load Credential from MongoDB: " + credentialPath);
    return doc.getString("conf");
  }

  @Override
  public void saveCredentials(String credentials) throws IOException {
    LOGGER.info("Save Credentials to MongoDB: " + credentialPath);
    mongoSave(credentialPath, credentials);
  }

  public Document mongoGet(String id) throws IOException {
    LOGGER.info("Save Credentials to file: " + credentialPath);
    return collection.find(eq("_id", id)).first();
  }

  public void mongoSave(String id, String json) throws IOException {
    // JSON to document
    Document doc = new Document();
    doc.append("conf", json);
    // set object id as note id
    doc.put("_id", id);
    collection.replaceOne(eq("_id", id), doc, new UpdateOptions().upsert(true));
  }

}
