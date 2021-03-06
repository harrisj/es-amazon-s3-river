/*
 * Licensed to Laurent Broudoux (the "Author") under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Author licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.github.lbroudoux.elasticsearch.river.s3.river;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.io.UnsupportedEncodingException;

import com.amazonaws.services.s3.model.AmazonS3Exception;
import org.apache.tika.metadata.Metadata;
import org.apache.commons.codec.binary.Base64;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.bulk.*;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;
import org.elasticsearch.search.SearchHit;

import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.github.lbroudoux.elasticsearch.river.s3.connector.S3ObjectSummaries;
import com.github.lbroudoux.elasticsearch.river.s3.connector.S3Connector;
import com.github.lbroudoux.elasticsearch.river.s3.river.TikaHolder;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
/**
 * A River component for scanning and indexing Amazon S3 documents into Elasticsearch.
 * @author laurent
 */
public class S3River extends AbstractRiverComponent implements River{

   private final Client client;
   
   private final String indexName;

   private final String typeName;

   private final int bulkSize;
   
   private volatile Thread feedThread;

   private volatile BulkProcessor bulkProcessor;

   private volatile boolean closed = false;
   
   private final S3RiverFeedDefinition feedDefinition;
   
   private final S3Connector s3;
   
   
   @Inject
   @SuppressWarnings({ "unchecked" })
   protected S3River(RiverName riverName, RiverSettings settings, Client client) throws Exception{
      super(riverName, settings);
      this.client = client;
      
      // setting this via ES_JAVA_OPTS didn't work, so YOLO. -JBM 3/23/15
      System.setProperty("jdk.xml.entityExpansionLimit", "0");

      // Deal with connector settings.
      if (settings.settings().containsKey("amazon-s3")){
         Map<String, Object> feed = (Map<String, Object>)settings.settings().get("amazon-s3");
         
         // Retrieve feed settings.
         String feedname = XContentMapValues.nodeStringValue(feed.get("name"), null);
         String bucket = XContentMapValues.nodeStringValue(feed.get("bucket"), null);
         String pathPrefix = XContentMapValues.nodeStringValue(feed.get("pathPrefix"), null);
         String downloadHost = XContentMapValues.nodeStringValue(feed.get("download_host"), null);
         int updateRate = XContentMapValues.nodeIntegerValue(feed.get("update_rate"), 15 * 60 * 1000);
         boolean jsonSupport = XContentMapValues.nodeBooleanValue(feed.get("json_support"), false);
         boolean trackS3Deletions = XContentMapValues.nodeBooleanValue(feed.get("deleteS3"), true);
         boolean truncateInitial = XContentMapValues.nodeBooleanValue(feed.get("truncate_initial"), false);
         
         String[] includes = S3RiverUtil.buildArrayFromSettings(settings.settings(), "amazon-s3.includes");
         String[] excludes = S3RiverUtil.buildArrayFromSettings(settings.settings(), "amazon-s3.excludes");
         
         // Retrieve connection settings.
         String accessKey = XContentMapValues.nodeStringValue(feed.get("accessKey"), null);
         String secretKey = XContentMapValues.nodeStringValue(feed.get("secretKey"), null);
         
         feedDefinition = new S3RiverFeedDefinition(feedname, bucket, pathPrefix, downloadHost,
               updateRate, Arrays.asList(includes), Arrays.asList(excludes), accessKey, secretKey, jsonSupport, trackS3Deletions, truncateInitial);
      } else {
         logger.error("You didn't define the amazon-s3 settings. Exiting... See https://github.com/lbroudoux/es-amazon-s3-river");
         indexName = null;
         typeName = null;
         bulkSize = 100;
         feedDefinition = null;
         s3 = null;
         return;
      }
      
      // Deal with index settings if provided.
      if (settings.settings().containsKey("index")) {
         Map<String, Object> indexSettings = (Map<String, Object>)settings.settings().get("index");
         
         indexName = XContentMapValues.nodeStringValue(indexSettings.get("index"), riverName.name());
         typeName = XContentMapValues.nodeStringValue(indexSettings.get("type"), S3RiverUtil.INDEX_TYPE_DOC);
         bulkSize = XContentMapValues.nodeIntegerValue(indexSettings.get("bulk_size"), 100);
      } else {
         indexName = riverName.name();
         typeName = S3RiverUtil.INDEX_TYPE_DOC;
         bulkSize = 100;
      }
      
      // We need to connect to Amazon S3 after ensure mandatory settings are here.
      if (feedDefinition.getAccessKey() == null){
         logger.error("Amazon S3 accessKey should not be null. Please fix this.");
         throw new IllegalArgumentException("Amazon S3 accessKey should not be null.");
      }
      if (feedDefinition.getSecretKey() == null){
         logger.error("Amazon S3 secretKey should not be null. Please fix this.");
         throw new IllegalArgumentException("Amazon S3 secretKey should not be null.");
      }
      if (feedDefinition.getBucket() == null){
         logger.error("Amazon S3 bucket should not be null. Please fix this.");
         throw new IllegalArgumentException("Amazon S3 bucket should not be null.");
      }
      s3 = new S3Connector(feedDefinition.getAccessKey(), feedDefinition.getSecretKey());
      try {
         s3.connectUserBucket(feedDefinition.getBucket(), feedDefinition.getPathPrefix());
      } catch (AmazonS3Exception ase){
         logger.error("Exception while connecting Amazon S3 user bucket. "
               + "Either access key, secret key or bucket name are incorrect");
         throw ase;
      }
   }
   
   @Override
   public void start(){
      if (logger.isInfoEnabled()){
         logger.info("Starting amazon s3 river scanning of {}", riverName().name());
      }
      try {
         // Create the index if it doesn't exist
         if (!client.admin().indices().prepareExists(indexName).execute().actionGet().isExists()) {

            // index settings go here
            // these analyzers won't be used in every index, but they do *exist* in every index.
            // they're just part of our toolbox to specify how to analyze fields (by name, unfortunately)
            // in S3RiverUtil.java
            client.admin().indices().prepareCreate(indexName).setSettings(
               ImmutableSettings.settingsBuilder().loadFromSource(jsonBuilder()
                .startObject()
                    .startObject("analysis")
                        .startObject("analyzer")
                            .startObject("email_analyzer")
                                .field("type", "custom")
                                .field("tokenizer", "email_tokenizer")
                                .field("filter", new String[]{"lowercase"})
                            .endObject()
                        .endObject()
                        .startObject("tokenizer")
                            .startObject("email_tokenizer")
                                .field("type", "pattern")
                                .field("pattern", "([a-zA-Z0-9_\\.+-]+@[a-zA-Z0-9-]+\\.[a-zA-Z0-9-\\.]+)")
                                .field("group", "0")
                            .endObject()
                        .endObject()                        
                    .endObject()
                .endObject().string())
            ).execute().actionGet();
         }
      } catch (Exception e) {
         if (ExceptionsHelper.unwrapCause(e) instanceof IndexAlreadyExistsException){
            // that's fine.
         } else if (ExceptionsHelper.unwrapCause(e) instanceof ClusterBlockException){
            // ok, not recovered yet..., lets start indexing and hope we recover by the first bulk.
         } else {
            logger.warn("failed to create index [{}], disabling river {}...", e, indexName, riverName().name());
            return;
         }
      }
      
      try {
         // If needed, we create the new mapping for files
         if (!feedDefinition.isJsonSupport()) {
            pushMapping(indexName, typeName, S3RiverUtil.buildS3FileMapping(typeName));
         }
      } catch (Exception e) {
         logger.warn("Failed to create mapping for [{}/{}], disabling river {}...",
               e, indexName, typeName, riverName().name());
         return;
      }

      // Creating bulk processor
      this.bulkProcessor = BulkProcessor.builder(client, new BulkProcessor.Listener() {
         @Override
         public void beforeBulk(long id, BulkRequest request) {
            logger.debug("Going to execute new bulk composed of {} actions", request.numberOfActions());
         }

         @Override
         public void afterBulk(long id, BulkRequest request, BulkResponse response) {
            logger.debug("Executed bulk composed of {} actions", request.numberOfActions());
            if (response.hasFailures()) {
               logger.warn("There was failures while executing bulk", response.buildFailureMessage());
               if (logger.isDebugEnabled()) {
                  for (BulkItemResponse item : response.getItems()) {
                     if (item.isFailed()) {
                        logger.debug("Error for {}/{}/{} for {} operation: {}", item.getIndex(),
                              item.getType(), item.getId(), item.getOpType(), item.getFailureMessage());
                     }
                  }
               }
            }
         }

         @Override
         public void afterBulk(long id, BulkRequest request, Throwable throwable) {
            logger.warn("Error executing bulk", throwable);
         }
      })
            .setBulkActions(bulkSize)
            .build();

      // We create as many Threads as there are feeds.
      feedThread = EsExecutors.daemonThreadFactory(settings.globalSettings(), "s3_slurper")
            .newThread(new S3Scanner(feedDefinition));
      feedThread.start();
   }
   
   @Override
   public void close(){
      if (logger.isInfoEnabled()){
         logger.info("Closing amazon s3 river {}", riverName().name());
      }
      closed = true;
      
      bulkProcessor.close();

      // We have to close the Thread.
      if (feedThread != null){
         feedThread.interrupt();
      }
   }
   
   /**
    * Check if a mapping already exists in an index
    * @param index Index name
    * @param type Mapping name
    * @return true if mapping exists
    */
   private boolean isMappingExist(String index, String type) {
      ClusterState cs = client.admin().cluster().prepareState()
            .setIndices(index).execute().actionGet()
            .getState();
      // Check index metadata existence.
      IndexMetaData imd = cs.getMetaData().index(index);
      if (imd == null){
         return false;
      }
      // Check mapping metadata existence.
      MappingMetaData mdd = imd.mapping(type);
      if (mdd != null){
         return true;
      }
      return false;
   }
   
   private void pushMapping(String index, String type, XContentBuilder xcontent) throws Exception {
      if (logger.isTraceEnabled()){
         logger.trace("pushMapping(" + index + ", " + type + ")");
      }
      
      // If type does not exist, we create it
      boolean mappingExist = isMappingExist(index, type);
      if (!mappingExist) {
         logger.debug("Mapping [" + index + "]/[" + type + "] doesn't exist. Creating it.");

         // Read the mapping json file if exists and use it.
         if (xcontent != null){
            if (logger.isTraceEnabled()){
               logger.trace("Mapping for [" + index + "]/[" + type + "]=" + xcontent.string());
            }
            // Create type and mapping
            PutMappingResponse response = client.admin().indices()
                  .preparePutMapping(index)
                  .setType(type)
                  .setSource(xcontent)
                  .execute().actionGet();       
            if (!response.isAcknowledged()){
               throw new Exception("Could not define mapping for type [" + index + "]/[" + type + "].");
            } else {
               if (logger.isDebugEnabled()){
                  if (mappingExist){
                     logger.debug("Mapping definition for [" + index + "]/[" + type + "] succesfully merged.");
                  } else {
                     logger.debug("Mapping definition for [" + index + "]/[" + type + "] succesfully created.");
                  }
               }
            }
         } else {
            if (logger.isDebugEnabled()){
               logger.debug("No mapping definition for [" + index + "]/[" + type + "]. Ignoring.");
            }
         }
      } else {
         if (logger.isDebugEnabled()){
            logger.debug("Mapping [" + index + "]/[" + type + "] already exists and mergeMapping is not set.");
         }
      }
      if (logger.isTraceEnabled()){
         logger.trace("/pushMapping(" + index + ", " + type + ")");
      }
   }
   
   /** */
   private class S3Scanner implements Runnable{
      
      private BulkRequestBuilder bulk;
      private S3RiverFeedDefinition feedDefinition;
      
      final String LAST_SCAN_TIME_FIELD = "_lastScanTime";
      final String INITIAL_SCAN_BOOKMARK_FIELD = "_initialScanBookmark";
      final String INITIAL_SCAN_FINISHED_FIELD = "_initialScanFinished";
      final int INITIAL_SCAN_SLEEP_INTERVAL = 2*60*1000;  

      public S3Scanner(S3RiverFeedDefinition feedDefinition){
         this.feedDefinition = feedDefinition;
      }
      
      @Override
      public void run(){

         while (true){
            int sleepInterval = feedDefinition.getUpdateRate();
            if (closed){
               return;
            }
            
            try{
               if (isStarted()) {
                  // Scan folder starting from last changes id, then record the new one.
                  // SO UGLY. I AM SORRY.
                  boolean initialScanFinished = !feedDefinition.truncateInitialScan() || getBooleanFromRiver(INITIAL_SCAN_FINISHED_FIELD);
                  String initialScanBookmark = getStringFromRiver(INITIAL_SCAN_BOOKMARK_FIELD);
                  
                  logger.debug("{}: INIT SCAN FINISHED: {} BOOKMARK: {}", riverName().name(), initialScanFinished, initialScanBookmark);

                  if (initialScanFinished) {
                     initialScanBookmark = null;
                  } else if (initialScanBookmark == null) {
                     initialScanBookmark = "";
                  }

                  Long lastScanTime = getLongFromRiver(LAST_SCAN_TIME_FIELD);
                  S3ObjectSummaries summaries = scan(lastScanTime, !initialScanFinished, initialScanBookmark, trackS3Deletions());
                  updateRiverLong(LAST_SCAN_TIME_FIELD, summaries.getLastScanTime());

                  if (summaries.getScanTruncated()) {
                     // still have more initial scanning to do
                     forceInitialScan(summaries.getLastKey());
                     sleepInterval = INITIAL_SCAN_SLEEP_INTERVAL;
                  } else {
                     logger.debug("{}: finished with initial scan", riverName().name());
                     finishInitialScan();
                  }
               } else {
                  logger.info("Amazon S3 River is disabled for {}", riverName().name());
               }
            } catch (Exception e){
               logger.warn("{}: error while indexing content from {}", riverName().name(), feedDefinition.getBucket());
               // if (logger.isDebugEnabled()){
                  logger.warn("{}: exception for folder {} is {}", riverName().name(), feedDefinition.getBucket(), e);
                  logger.warn("Stack trace: ", e);
               // 
            }
            
            try {
               logger.info("{}: sleeping for {} ms", riverName().name(), sleepInterval);
               Thread.sleep(sleepInterval);
            } catch (InterruptedException ie){
            }
         }
      }

      public void forceInitialScan(String lastKey) throws Exception {
         logger.info("{}: saving new bookmark of initial scan: {}", riverName().name(), lastKey);
         updateRiverString(INITIAL_SCAN_BOOKMARK_FIELD, lastKey);
         updateRiverBoolean(INITIAL_SCAN_FINISHED_FIELD, false);         
      }

      public boolean initialScanFinished() throws Exception {
         return getBooleanFromRiver(INITIAL_SCAN_FINISHED_FIELD);
      }

      public String initialScanBookmark() throws Exception {
         return getStringFromRiver(INITIAL_SCAN_BOOKMARK_FIELD);
      }

      public void finishInitialScan() throws Exception {
         updateRiverString(INITIAL_SCAN_BOOKMARK_FIELD, null);
         updateRiverBoolean(INITIAL_SCAN_FINISHED_FIELD, true);    
      }

      public boolean trackS3Deletions() {
         return this.feedDefinition.trackS3Deletions();
      }
      
      private boolean isStarted(){
         // Refresh index before querying it.
         client.admin().indices().prepareRefresh("_river").execute().actionGet();
         GetResponse isStartedGetResponse = client.prepareGet("_river", riverName().name(), "_s3status").execute().actionGet();
         try{
            if (!isStartedGetResponse.isExists()){
               XContentBuilder xb = jsonBuilder().startObject()
                     .startObject("amazon-s3")
                        .field("feedname", feedDefinition.getFeedname())
                        .field("status", "STARTED").endObject()
                     .endObject();
               client.prepareIndex("_river", riverName.name(), "_s3status").setSource(xb).execute();
               return true;
            } else {
               String status = (String)XContentMapValues.extractValue("amazon-s3.status", isStartedGetResponse.getSourceAsMap());
               if ("STOPPED".equals(status)){
                  return false;
               }
            }
         } catch (Exception e){
            logger.warn("failed to get status for " + riverName().name() + ", throttling....", e);
         }
         return true;
      }
      
      @SuppressWarnings("unchecked")
      private Object getObjectFromRiver(String field){
         Object result = null;
         try {
            // Do something.
            client.admin().indices().prepareRefresh("_river").execute().actionGet();
            GetResponse getResponse = client.prepareGet("_river", riverName().name(), field).execute().actionGet();
            if (getResponse.isExists()) {
               Map<String, Object> fsState = (Map<String, Object>) getResponse.getSourceAsMap().get("amazon-s3");
               result = fsState.get(field);
            } else {
               // This is first call, just log in debug mode.
               logger.debug("{} doesn't exist", field);
            }
         } catch (Exception e) {
            logger.warn("failed to get {}, throttling....", field, e);
         }

         if (logger.isDebugEnabled()){
            logger.debug("{}: {}", field, result);
         }
         return result;
      }

      @SuppressWarnings("unchecked")
      private String getStringFromRiver(String field){
         Object result = getObjectFromRiver(field);
         if (result == null) {
            return null;
         } else {
            return ((String) result);
         }
      }

      @SuppressWarnings("unchecked")
      private boolean getBooleanFromRiver(String field){
         Object result = getObjectFromRiver(field);
         if (result == null) {
            return false; // FIXME: hmm?
         } else {
            return ((boolean) result);
         }
      }

      @SuppressWarnings("unchecked")
      private Long getLongFromRiver(String field){
         Object result = getObjectFromRiver(field);
         if (result != null) {
            try{
               return Long.parseLong(result.toString());
            } catch (NumberFormatException nfe){
               logger.warn("Last recorded {} is not a Long: {}", field, result.toString());
            }
         }

         return null;
      }

      /** Scan the Amazon S3 bucket for last changes. */
      private S3ObjectSummaries scan(Long lastScanTime, boolean initialScan, String initialScanBookmark, boolean trackS3Deletions) throws Exception{
         if (logger.isDebugEnabled()){
            logger.debug("{}: scanning bucket {} for new items since {}", riverName().name(), feedDefinition.getBucket(), lastScanTime);
         }

         S3ObjectSummaries summaries = s3.getObjectSummaries(riverName().name(), lastScanTime, initialScan, initialScanBookmark, trackS3Deletions);
                  
         // Browse change and checks if its indexable before starting.
         for (S3ObjectSummary summary : summaries.getPickedSummaries()){
            if (S3RiverUtil.isIndexable(summary.getKey(), feedDefinition.getIncludes(), feedDefinition.getExcludes())){
               indexFile(summary);
            }
         }
         
         // Now, because we do not get changes but only present files, we should 
         // compare previously indexed files with latest to extract deleted ones...
         // But before, we need to produce a list of index ids corresponding to S3 keys.
         // This is pretty hard on memory if you have a directory with millions of files, so 
         // if you don't need that, I allow you to disable the syncing by setting the trackS3Deletions
         // flag to false (disables deletion syncs both ways)
         if (summaries.trackS3Deletions()) {
            List<String> previousFileIds = getAlreadyIndexFileIds();
            List<String> summariesIds = new ArrayList<String>();
            for (String key : summaries.getKeys()){
               summariesIds.add(buildIndexIdFromS3Key(key));
            }
            for (String previousFileId : previousFileIds){
               if (!summariesIds.contains(previousFileId)){
                  esDelete(indexName, typeName, previousFileId);
               }
            }
         }

         return summaries;
      }
      
      /** Retrieve the ids of files already present into index. */
      private List<String> getAlreadyIndexFileIds(){
         List<String> fileIds = new ArrayList<String>();
         // TODO : Should be later optimized for only retrieving ids and getting
         // over the 5000 hits limitation.
         SearchResponse response = client
               .prepareSearch(indexName)
               .setSearchType(SearchType.QUERY_AND_FETCH)
               .setTypes(typeName)
               .setFrom(0)
               .setSize(5000)
               .execute().actionGet();
         if (response.getHits() != null && response.getHits().getHits() != null){
            for (SearchHit hit : response.getHits().getHits()){
               fileIds.add(hit.getId());
            }
         }
         return fileIds;
      }
      
      /** Index an Amazon S3 file by retrieving its content and building the suitable Json content. */
      private String indexFile(S3ObjectSummary summary){
         String key = s3.getDecodedKey(summary);

         if (logger.isDebugEnabled()){
            logger.debug("Trying to index '{}'", key);
         }
         
         try{
            // Build a unique id from S3 unique summary key.
            String fileId = buildIndexIdFromS3Key(summary.getKey());
            // logger.info("SHA-256 key for '{}'' is {}", summary.getKey(), fileId);

            if (feedDefinition.isJsonSupport()){
               esIndex(indexName, typeName, summary.getKey(), s3.getContent(summary));
            } else {
               byte[] fileContent = s3.getContent(summary);

               if (fileContent != null) {
                  // Parse content using Tika directly.
                  Metadata fileMetadata = new Metadata();
                  String parsedContent = "";

                  try {
                    parsedContent = TikaHolder.tika().parseToString(
                        new BytesStreamInput(fileContent, false), fileMetadata);
                  } catch (Exception e) {
                    logger.warn("Tika error " + summary.getKey() + " : " + e.getMessage());
                  }

                  // convert fileMetadata to a map for jsonBuilder object
                  Map<String, Object> fileMetadataMap = new HashMap<String, Object>();
                  String[] metadata_keys = fileMetadata.names();
                  for (String k : metadata_keys) {
                     if (fileMetadata.isMultiValued(k)) {
                        fileMetadataMap.put(k,fileMetadata.getValues(k));
                     } else {
                        fileMetadataMap.put(k,fileMetadata.get(k));
                     }
                  }

                  esIndex(indexName, typeName, fileId,
                        jsonBuilder()
                           .startObject()
                              .field(S3RiverUtil.DOC_FIELD_TITLE, key.substring(key.lastIndexOf('/') + 1))
                              .field(S3RiverUtil.DOC_FIELD_MODIFIED_DATE, summary.getLastModified().getTime())
                              .field(S3RiverUtil.DOC_FIELD_SOURCE_URL, s3.getDownloadUrl(summary, feedDefinition))
                              .field(S3RiverUtil.DOC_FIELD_METADATA,   s3.getS3UserMetadata(key))
                              .startObject("file")
                                 .field("_name", summary.getKey().substring(key.lastIndexOf('/') + 1))
                                 .field("title", summary.getKey().substring(key.lastIndexOf('/') + 1))
                                 .field("metadata", fileMetadataMap)
                                 .field("file", parsedContent)
                              .endObject()
                           .endObject());

                  logger.debug("S3 River: indexed '{}'", key);

                  return fileId;
               }
            }
         } catch (Exception e) {
            logger.warn(riverName().name() + ": can not index " + key + " : " + e.getMessage());
         }
         return null;
      }
      
      /** Build a unique id from S3 unique summary key. */
      private String buildIndexIdFromS3Key(String key) throws NoSuchAlgorithmException,UnsupportedEncodingException {
         //return key.replace('/', '-').replace(' ', '-');

         // Let's just build a SHA-1 key instead
         MessageDigest md = MessageDigest.getInstance("SHA-256");
         return Base64.encodeBase64String(md.digest(key.getBytes())).replace('/', '_').replace('+', '-').replace("=", "").replace("\n", "").replace("\r", "");
      }
      
      /** Update river last changes id value.*/
      private void updateRiverObject(String field, Object value) throws Exception {
         if (logger.isDebugEnabled()){
            logger.debug("{} updating {}: {}", riverName().name(), field, value);
         }

         // We store the lastupdate date and some stats
         XContentBuilder xb = jsonBuilder()
            .startObject()
               .startObject("amazon-s3")
                  .field("feedname", feedDefinition.getFeedname())
                  .field(field, value)
               .endObject()
            .endObject();
         esIndex("_river", riverName.name(), field, xb);
         bulkProcessor.flush();  // force it to be changed
      }

      // just in case I need to modify the values passed into updateRiverObject
      private void updateRiverLong(String field, Long value) throws Exception{
         updateRiverObject(field, value);
      }

      private void updateRiverString(String field, String value) throws Exception{
         updateRiverObject(field, value);
      }

      private void updateRiverBoolean(String field, boolean value) throws Exception{
         updateRiverObject(field, value);
      }

      /** Add to bulk an IndexRequest. */
      private void esIndex(String index, String type, String id, XContentBuilder xb) throws Exception{
         if (logger.isDebugEnabled()){
            logger.debug("Indexing in ES " + index + ", " + type + ", " + id);
         }
         if (logger.isTraceEnabled()){
            logger.trace("Json indexed : {}", xb.string());
         }
         bulkProcessor.add(client.prepareIndex(index, type, id).setSource(xb).request());
      }

      /** Add to bulk an IndexRequest. */
      private void esIndex(String index, String type, String id, byte[] json) throws Exception{
         if (logger.isDebugEnabled()){
            logger.debug("Indexing in ES " + index + ", " + type + ", " + id);
         }
         if (logger.isTraceEnabled()){
            logger.trace("Json indexed : {}", json);
         }
         bulkProcessor.add(client.prepareIndex(index, type, id).setSource(json).request());
      }

      /** Add to bulk a DeleteRequest. */
      private void esDelete(String index, String type, String id) throws Exception{
         if (logger.isDebugEnabled()){
            logger.debug("Deleting from ES " + index + ", " + type + ", " + id);
         }
         bulkProcessor.add(client.prepareDelete(index, type, id).request());
      }
   }
}
