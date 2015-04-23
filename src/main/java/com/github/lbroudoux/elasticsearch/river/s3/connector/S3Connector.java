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
package com.github.lbroudoux.elasticsearch.river.s3.connector;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.s3.model.*;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.github.lbroudoux.elasticsearch.river.s3.river.S3RiverFeedDefinition;
/**
 * This is a connector for querying and retrieving files or folders from
 * an Amazon S3 bucket. Credentials are mandatory for connecting to remote drive.
 * @author laurent
 */
public class S3Connector{
  // This will only work if you can check presence in ES easily
  final int MAX_NEW_RESULTS_TO_INDEX_ON_RUN = 10000;

   private static final ESLogger logger = Loggers.getLogger(S3Connector.class);
   
   private final String accessKey;
   private final String secretKey;
   private String bucketName;
   private String pathPrefix;
   private AmazonS3Client s3Client;
   
   public S3Connector(String accessKey, String secretKey){
      this.accessKey = accessKey;
      this.secretKey = secretKey;
   }
   
   /**
    * Connect to the specified bucket using previously given accesskey and secretkey.
    * @param bucketName Name of the bucket to connect to
    * @param pathPrefix Prefix that will be later used for filtering documents
    * @throws AmazonS3Exception when access or secret keys are wrong or bucket does not exists
    */
   public void connectUserBucket(String bucketName, String pathPrefix) throws AmazonS3Exception{
      this.bucketName = bucketName;
      this.pathPrefix = pathPrefix;
      AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
      s3Client = new AmazonS3Client(credentials);
      // Getting location seems odd as we don't use it later and doesBucketExists() seems
      // more appropriate... However, this later returns true even for non existing buckets !
      s3Client.getBucketLocation(bucketName);
   }
   
   /**
    * Select and retrieves summaries of object into bucket and of given path prefix
    * that have modification date younger than lastScanTime.
    * @param lastScanTime Last modification date filter
    * @return Summaries of picked objects.
    */
   public S3ObjectSummaries getObjectSummaries(String riverName, Long lastScanTime, String initialScanBookmark, boolean trackS3Deletions) {
      List<String> keys = new ArrayList<String>();
      List<S3ObjectSummary> result = new ArrayList<S3ObjectSummary>();
      boolean initialScan = initialScanBookmark != null;

      if (initialScan) {
        trackS3Deletions = false;
        logger.info("{}: resuming initial scan of {} from {}", riverName, pathPrefix, initialScanBookmark);
      } else {
        logger.info("{}: checking {} for changes since {}", riverName, pathPrefix, lastScanTime);
      }

      // Store the scan time to return before doing big queries...
      Long lastScanTimeToReturn = System.currentTimeMillis();

      if (lastScanTime == null || initialScan) {
         lastScanTime = 0L;
      }
      
      ListObjectsRequest request = new ListObjectsRequest().withBucketName(bucketName)
            .withPrefix(pathPrefix).withEncodingType("url");
      ObjectListing listing = s3Client.listObjects(request);
      //logger.debug("Listing: {}", listing);
      int keyCount = 0;
      boolean scanTruncated = false;
      String lastKey = null;

      while (!listing.getObjectSummaries().isEmpty() || listing.isTruncated()){
         List<S3ObjectSummary> summaries = listing.getObjectSummaries();
         // if (logger.isDebugEnabled()) {
            logger.debug("Found {} items in this listObjects page (truncated? {})", summaries.size(), listing.isTruncated());
         // }

         for (S3ObjectSummary summary : summaries) {
            if (logger.isDebugEnabled()){
               // logger.debug("Getting {} last modified on {}", summary.getKey(), summary.getLastModified());
            }

            if (trackS3Deletions) {
              keys.add(summary.getKey());
            }

            if (summary.getLastModified().getTime() > lastScanTime && result.size() < MAX_NEW_RESULTS_TO_INDEX_ON_RUN) {
                 // logger.debug("  Picked !");

                if (!initialScan || initialScanBookmark.compareTo(summary.getKey()) < 0) {
                   logger.debug("  Picked {}", summary.getKey());
                   result.add(summary);
                   lastKey = summary.getKey();
                 }

              } else if (!scanTruncated && result.size() == MAX_NEW_RESULTS_TO_INDEX_ON_RUN) {
                logger.info("{}: only indexing up to {} new objects on this indexing run", riverName, MAX_NEW_RESULTS_TO_INDEX_ON_RUN);
                // initialScan = true;
                scanTruncated = true;

                if (!trackS3Deletions) {
                  // No need to keep iterating through all keys if we aren't doing deleteOnS3 
                  break;
                }
              }

            keyCount += 1;
         }

         if (initialScan && scanTruncated && !trackS3Deletions) {
           break;
         }
 
         listing = s3Client.listNextBatchOfObjects(listing);
      }

      // Wrap results and latest scan time.
      if (scanTruncated) {
        logger.info("{}: scan truncated for speed: {} files ({} new)", riverName, keyCount, result.size());
      } else {
        logger.info("{}: complete scan: {} files ({} new)", riverName, keyCount, result.size());
      }

      return new S3ObjectSummaries(lastScanTimeToReturn, lastKey, scanTruncated, trackS3Deletions, result, keys);
   }
   
   public Map<String,Object> getS3UserMetadata(String key){ 
	   return Collections.<String, Object>unmodifiableMap(s3Client.getObjectMetadata(bucketName, key).getUserMetadata());
   }

   public String getDecodedKey(S3ObjectSummary summary) {
      //return summary.getKey();  // If you deactivate using withEncodingType above
      try {
        return java.net.URLDecoder.decode(summary.getKey(), "UTF-8");
      } catch (java.io.UnsupportedEncodingException e) {
        e.printStackTrace();
        return null;
      }
   }

   /**
    * Download Amazon S3 file as byte array.
    * @param summary The summary of the S3 Object to download
    * @return This file bytes or null if something goes wrong.
    */
   public byte[] getContent(S3ObjectSummary summary) {
      String key = getDecodedKey(summary);

      // Retrieve object corresponding to key into bucket.
      if (logger.isDebugEnabled()){
         logger.debug("Downloading file content from {}", key);
      }

      S3Object object = s3Client.getObject(bucketName, key);
      
      InputStream is = null;
      ByteArrayOutputStream bos = null;

      try{
         // Get input stream on S3 Object.
         is = object.getObjectContent();
         bos = new ByteArrayOutputStream();

         byte[] buffer = new byte[4096];
         int len = is.read(buffer);
         while (len > 0) {
            bos.write(buffer, 0, len);
            len = is.read(buffer);
         }

         // Flush and return result.
         bos.flush();
         return bos.toByteArray();
      } catch (IOException e) {
         e.printStackTrace();
         return null;
      } finally {
         if (bos != null){
            try{
               bos.close();
            } catch (IOException e) {
            }
         }
         try{
            is.close();
         } catch (IOException e) {
         }
      }
   }
   
   /**
    * Get the download url of this S3 object. May return null if the
    * object bucket and key cannot be converted to a URL.
    * @param summary A S3 object
    * @param feedDefinition The holder of S3 feed definition.
    * @return The resource url if possible (access is subject to AWS credential)
    */
   public String getDownloadUrl(S3ObjectSummary summary, S3RiverFeedDefinition feedDefinition){
      String resourceUrl = s3Client.getResourceUrl(summary.getBucketName(), summary.getKey()); 
      // If a download host (actually a vhost such as cloudfront offers) is specified, use it to
      // recreate a vhosted resource url. This is made by substitution of the generic host name in url. 
      if (resourceUrl != null && feedDefinition.getDownloadHost() != null){
         int hostPosEnd = resourceUrl.indexOf("s3.amazonaws.com/") + "s3.amazonaws.com".length();
         String vhostResourceUrl = feedDefinition.getDownloadHost() + resourceUrl.substring(hostPosEnd);
         return vhostResourceUrl;
      }
      return resourceUrl;
   }
}
