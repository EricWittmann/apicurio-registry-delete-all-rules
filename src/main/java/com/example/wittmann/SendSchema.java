package com.example.wittmann;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.stream.Collectors;

import org.apache.hc.client5.http.classic.methods.HttpDelete;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.classic.methods.HttpPut;
import org.apache.hc.client5.http.entity.EntityBuilder;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.message.BasicHeader;
import org.json.JSONException;
import org.json.JSONObject;

public class SendSchema
{
    private final static int NODELETE = 0;
    private final static int DELETEALLVERSIONS = 1;
    private final static int DELETEONEVERSION = 2;
    private final static int DELETEALL = 3;

    private static String encodeValue(String value) {
        try {
            return URLEncoder.encode(value, StandardCharsets.UTF_8.toString());
        } catch (UnsupportedEncodingException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return null;
        }
    }

    private static void itemDelete(CloseableHttpClient httpclient, String url, String deletedItem) {
        HttpDelete  httpDelete = new HttpDelete(url);

        try (CloseableHttpResponse response2 = httpclient.execute(httpDelete)) {
            HttpEntity entity2 = response2.getEntity();
            // do something useful with the response body
            // and ensure it is fully consumed
            EntityUtils.consume(entity2);

            switch (response2.getCode()) {
                case (204):
                    System.out.println("OK.");
                    break;
                case (404):
                    System.out.println("No " + deletedItem + " found.");
                    break;
                case (405):
                    System.err.println("Error request");
                    System.out.println("Feature is disabled.");
                    System.exit(2);
                default: 
                    System.err.println("Error request");
                    System.err.println("Unrecognized response code: " + response2.getCode() + "/" + response2.getReasonPhrase());
                    System.exit(2);
            }
        } catch (IOException e) {
            System.err.println ("Exception during request");
            System.err.println(e);
            System.exit(3);
        }

    }

    private static void itemCreate(HttpPost httpPost, CloseableHttpClient httpclient, String createItem) {
        try (CloseableHttpResponse response2 = httpclient.execute(httpPost)) {
            HttpEntity entity2 = response2.getEntity();
            // do something useful with the response body
            // and ensure it is fully consumed
            EntityUtils.consume(entity2);

            switch (response2.getCode()) {
                case (200):
                case (204):
                    System.out.println("OK.");
                    break;
                case (400):
                    System.err.println("Error request");
                    System.err.println("Invalid content for " + createItem + " creation: " + response2.getCode() + "/" + response2.getReasonPhrase());
                    break;     
                case (404):
                    System.err.println("Error request");
                    System.err.println("A new version is inserted although there is no initial schema");
                    break;     
                /*
                case (405):
                    System.err.println("Error request");
                    System.out.println("Feature is disabled.");
                    System.exit(2);
                */
                case (409):
                    System.err.println("Error request");
                    System.err.println("A rule is violated when the schema is created: " + response2.getCode() + "/" + response2.getReasonPhrase());
                    System.exit(2);
                default: 
                    System.err.println("Error request");
                    System.err.println("Unrecognized response code: " + response2.getCode() + "/" + response2.getReasonPhrase());
                    System.exit(2);
            }
        } catch (IOException e) {
            System.err.println ("Exception during request");
            System.err.println(e);
            System.exit(3);
        }

    }

    public static void main( String[] args )
    {
        run("TestFlow", "key", "v1");
        run("TestFlow", "key", "v2");
    }

    public static void run(String flowName, String keyOrValue, String version) {
        String schemaRegistryURL = System.getProperty("schema.registry.url", "http://localhost:8080/apis/registry/v2");
        String appName = System.getProperty("engine.application", "SendSchema");

        int deleteMode = NODELETE;

        InputStream targetStream;

        try {   
            String resource = "schemas/" + flowName + "-" + keyOrValue + "-" + version + ".avsc";
            System.out.println("Loading Avro schema from: " + resource);
            targetStream = SendSchema.class.getClassLoader().getResourceAsStream(resource);
        } catch (Exception e) {
            e.printStackTrace();
            targetStream = null;
            System.exit(1);
        }

        try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
            String url = "";

            JSONObject jsonObj = new JSONObject (new BufferedReader(
                new InputStreamReader(targetStream, StandardCharsets.UTF_8))
                  .lines()
                  .collect(Collectors.joining("\n")));

            String artifactID;
            try {
                artifactID = jsonObj.getString("topic") + "-" + keyOrValue.toLowerCase();
                jsonObj.remove("topic");
            } catch (JSONException e) {
                artifactID = flowName + "-" + keyOrValue.toLowerCase();
            }

            System.out.println("artifactID = " + artifactID);

            String artifactVersion;
            try {
                artifactVersion = jsonObj.getString("artifactVersion");
                jsonObj.remove("artifactVersion");
            } catch (JSONException e) {
                artifactVersion = null;
            }

            if (artifactVersion == null) {
                System.err.println ("Version not found in the AVSC file");
                System.exit(5);
            }

            // authorize delete one version
            url = schemaRegistryURL + "/admin/config/properties/" + encodeValue("registry.rest.artifact.deletion.enabled");
            System.out.println("Authorize deletion of only one version of a schema in Apicurio registry.");
            HttpPut httpPut = new HttpPut(url);
            httpPut.setEntity(EntityBuilder.create().setText("{" +
                    "\"value\": \"true\"" +
            "}").build());
            httpPut.setHeader(new BasicHeader("Content-type", "application/json"));

            try (CloseableHttpResponse response2 = httpclient.execute(httpPut)) {
                HttpEntity entity2 = response2.getEntity();
                // do something useful with the response body
                // and ensure it is fully consumed
                EntityUtils.consume(entity2);
                switch (response2.getCode()) {
                    case (204):
                        System.out.println("OK.");
                        break;
                    case (404):
                        System.err.println("Error request");
                        System.err.println("property 'registry.rest.artifact.deletion.enabled' not found.");
                        System.exit(2);
                        break;
                    default: 
                        System.err.println("Error request");
                        System.err.println("Unrecognized response code: " + response2.getCode() + "/" + response2.getReasonPhrase());
                        System.exit(2);
                }
            }

            if (deleteMode != NODELETE) {
                switch (deleteMode) {
                    case DELETEALLVERSIONS:
                        url = schemaRegistryURL + "/groups/" + encodeValue(appName) + "/artifacts/" + encodeValue(artifactID);
                        System.out.println("Delete all versions for schema=" + appName + "/" + artifactID);
                        itemDelete(httpclient, url, "schema");
                        break;
                    case DELETEONEVERSION:
                        url = schemaRegistryURL + "/groups/" + encodeValue(appName) + "/artifacts/" + encodeValue(artifactID) + "/versions/" + encodeValue(artifactVersion);
                        System.out.println("Delete version=" + artifactVersion + " for schema=" + appName + "/" + artifactID);
                        itemDelete(httpclient, url, "schema");
                        break;
                    case DELETEALL:
                        url = schemaRegistryURL + "/groups/" + encodeValue(appName) + "/artifacts";
                        System.out.println("Delete all artifacts for group=" + appName);
                        itemDelete(httpclient, url, "schema");
                        break;
                    default:
                        System.err.println("deleteMode not recognized");
                        System.exit(4);
                        break;
                }
            }

            url = schemaRegistryURL + "/groups/" + encodeValue(appName) + "/artifacts/" + encodeValue(artifactID) + "/versions";
            System.out.println("Look for older versions of schema=" + appName + "/" + artifactID);

            HttpGet httpGet = new HttpGet(url);
            HttpPost httpPost = null;

            try (CloseableHttpResponse response2 = httpclient.execute(httpGet)) {
                HttpEntity entity2 = response2.getEntity();
                // do something useful with the response body
                // and ensure it is fully consumed
                EntityUtils.consume(entity2);

                switch (response2.getCode()) {
                    case (200):
                        // there are older versions with existing rules
                        url = schemaRegistryURL + "/groups/" + encodeValue(appName) + "/artifacts/" + encodeValue(artifactID) + "/versions";
                        System.out.println("Create version " + artifactVersion + " for existing schema=" + appName + "/" + artifactID);
                        httpPost = new HttpPost(url);
                        break;
                    case (404):
                        // no older versions
                        url = schemaRegistryURL + "/groups/" + encodeValue(appName) + "/artifacts?ifExists=FAIL&canonical=true";
                        System.out.println("Create version " + artifactVersion + " of a new schema=" + appName + "/" + artifactID);
                        httpPost = new HttpPost(url);
                        httpPost.setHeader(new BasicHeader("X-Registry-ArtifactID", artifactID));
                        break;
                    default: 
                        System.err.println ("Error request");
                        System.err.println("Unrecognized response code: " + response2.getCode() + "/" + response2.getReasonPhrase());
                        System.exit(2);
                }
            }

            httpPost.setHeader(new BasicHeader("Content-type", "application/json"));
            httpPost.setHeader(new BasicHeader("X-Registry-Version", artifactVersion));
            // httpPost.setHeader(new BasicHeader("X-Registry-ArtifactType", "AVRO"));
            httpPost.setHeader(new BasicHeader("X-Registry-Description", "AVRO schema for flow " + flowName + " (" + keyOrValue + ")"));
       
            httpPost.setEntity(EntityBuilder.create().setText(jsonObj.toString(3)).build());
            itemCreate(httpPost, httpclient, "schema");


            // rules
            url = schemaRegistryURL + "/groups/" + encodeValue(appName) + "/artifacts/" + encodeValue(artifactID) + "/rules";
            System.out.println("Delete all existing rules for schema=" + appName + "/" + artifactID);
            itemDelete(httpclient, url, "rule");


            // Apicurio bug: integrity rule is not deleted by previous call: delete specifically integrity rule
//            url = schemaRegistryURL + "/groups/" + encodeValue(appName) + "/artifacts/" + encodeValue(artifactID) + "/rules/INTEGRITY";
//            System.out.println("Apicurio Bug: Delete INTEGRITY rule for schema=" + appName + "/" + artifactID);
//            itemDelete(httpclient, url, "rule");

            url = schemaRegistryURL + "/groups/" + encodeValue(appName) + "/artifacts/" + encodeValue(artifactID) + "/rules/INTEGRITY";
            System.out.println("Verify INTEGRITY rule is deleted");

            httpGet = new HttpGet(url);

            try (CloseableHttpResponse response2 = httpclient.execute(httpGet)) {
                HttpEntity entity2 = response2.getEntity();
                // do something useful with the response body
                // and ensure it is fully consumed
                EntityUtils.consume(entity2);

                switch (response2.getCode()) {
                    case (200):
                        System.err.println("INTEGRITY rule is not deleted");
                        System.exit(2);
                        break;
                    case (400):
                        System.err.println("Error request");
                        System.err.println("Invalid rule type");
                        System.exit(2);
                        break;
                    case (404):
                        System.out.println("INTEGRITY rule is deleted");
                        break;
                    default: 
                        System.err.println ("Error request");
                        System.err.println("Unrecognized response code: " + response2.getCode() + "/" + response2.getReasonPhrase());
                        System.exit(2);
                }
            }

           
            url = schemaRegistryURL + "/groups/" + encodeValue(appName) + "/artifacts/" + encodeValue(artifactID) + "/rules";
            System.out.println("Add VALIDITY=FULL rule for schema=" + appName + "/" + artifactID);

            httpPost = new HttpPost(url);
            httpPost.setHeader(new BasicHeader("Content-type", "application/json"));
            httpPost.setEntity(EntityBuilder.create().setText("{" +
                "\"type\": \"VALIDITY\"," +
                "\"config\": \"FULL\"" +
            "}").build());
            itemCreate(httpPost, httpclient, "rule");

            System.out.println("Add COMPATIBILITY=BACKWARD_TRANSITIVE for schema=" + appName + "/" + artifactID);

            httpPost = new HttpPost(url);
            httpPost.setHeader(new BasicHeader("Content-type", "application/json"));
            httpPost.setEntity(EntityBuilder.create().setText("{" +
                "\"type\": \"COMPATIBILITY\"," +
                "\"config\": \"BACKWARD_TRANSITIVE\"" +
            "}").build());
            itemCreate(httpPost, httpclient, "rule");

            System.out.println("Add INTEGRITY=FULL for schema=" + appName + "/" + artifactID);

            httpPost = new HttpPost(url);
            httpPost.setHeader(new BasicHeader("Content-type", "application/json"));
            httpPost.setEntity(EntityBuilder.create().setText("{" +
                "\"type\": \"INTEGRITY\"," +
                "\"config\": \"FULL\"" +
            "}").build());
            itemCreate(httpPost, httpclient, "rule");
        } catch (IOException e) {
            System.err.println ("Exception during request");
            System.err.println(e);
            System.exit(2);
        } finally {
            try {
                targetStream.close();
            } catch (IOException e) {
                System.err.println ("IOException during file stream close");
                System.err.println(e);
                System.exit(1);
            }
        }
    }
}