/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.gcloud.poc;
/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

import com.google.api.services.bigquery.Bigquery.Datasets.List;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.StorageScopes;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.BlockingDataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;

import java.io.IOException;
import java.security.GeneralSecurityException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A starter example for writing Google Cloud Dataflow programs.
 *
 * <p>The example takes two strings, converts them to their upper-case
 * representation and logs them.
 *
 * <p>To run this starter example locally using DirectPipelineRunner, just
 * execute it without any additional parameters from your favorite development
 * environment. In Eclipse, this corresponds to the existing 'LOCAL' run
 * configuration.
 *
 * <p>To run this starter example using managed resource in Google Cloud
 * Platform, you should specify the following command-line options:
 *   --project=<YOUR_PROJECT_ID>
 *   --stagingLocation=<STAGING_LOCATION_IN_CLOUD_STORAGE>
 *   --runner=BlockingDataflowPipelineRunner
 * In Eclipse, you can just modify the existing 'SERVICE' run configuration.
 */
@SuppressWarnings("serial")
public class StarterPipeline {
  private static final Logger LOG = LoggerFactory.getLogger(StarterPipeline.class);
  private static Storage storageService;
  private static final String APPLICATION_NAME = "CLOUD_POC";
  private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
  
  public static void main(String[] args) throws Exception, GeneralSecurityException {
	  Storage client = getService();
	  System.out.println("Starting PipeLine Process!!!");
	  PipelineOptions options = PipelineOptionsFactory.create();
	  Pipeline p = Pipeline.create(options);
	  
	  System.out.println("Creating PipeLine Statements!!!");
	  p.apply(TextIO.Read.named("ReadFromText")
              .from("gs://sakthi-poc-bucket/input/Input.txt"))
	  .apply(TextIO.Write.named("WriteToText")
              .to("gs://sakthi-poc-bucket/output/")
              .withSuffix(".csv"));
	    /*options.setStagingLocation("gs://sakthi-poc-bucket/staging");
	    
	    p.apply(TextIO.Read.from("gs://sakthi-poc-bucket/shakespeare/*"))
	    .apply(TextIO.Write.to("gs://sakthi-poc-bucket/output")); */
	  System.out.println("Executing PipeLine Statements!!!");
	  p.run();  
	  System.out.println("Process Completed!!!");	  
  }
  private static Storage getService() throws IOException, GeneralSecurityException {
	  if (null == storageService) {
	    GoogleCredential credential = GoogleCredential.getApplicationDefault();
	    // Depending on the environment that provides the default credentials (e.g. Compute Engine,
	    // App Engine), the credentials may require us to specify the scopes we need explicitly.
	    // Check for this case, and inject the Cloud Storage scope if required.
	    if (credential.createScopedRequired()) {
	      credential = credential.createScoped(StorageScopes.all());
	    }
	    HttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();
	    storageService = new Storage.Builder(httpTransport, JSON_FACTORY, credential)
	        .setApplicationName(APPLICATION_NAME).build();
	  }
	  return storageService;
	} 
}
