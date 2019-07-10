package DMP_integration_with_CI360;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Paths;
import java.util.*;


import com.google.gson.*;
import com.sas.mkt.agent.sdk.CI360Agent;
import com.sas.mkt.agent.sdk.CI360AgentException;
import com.sas.mkt.agent.sdk.CI360StreamInterface;
import com.sas.mkt.agent.sdk.ErrorCode;
import org.eclipse.jetty.client.api.ContentResponse;
import org.apache.commons.validator.routines.EmailValidator;

/**
 * This class contains sample code used to demonstrate the usage of the CI360 Agent SDK
 * {@link CI360Agent} to interact with CI360.   The sample will connect to the CI360 event stream
 * and will print out all events that arrive from CI360.   It also accepts a few command from standard
 * input.
 * <br> <br>
 * exit - exits the sample agent
 * <br> <br>
 * send - sends an external event to CI360.   following the send command is the event to be injected.
 * The event is in JSON.  See {@link CI360Agent#injectEvent(String)}.
 * <br> <br>
 * bulk - requests a Signed S3 URL be returned for uploaded events into CI360.   Following the "bulk" command
 * is the application ID to use.   See {@link CI360Agent#requestBulkEventURL(String)}.
 *
 * @author magibs
 */
public class SimpleAgentDMP {

    static boolean exiting = false;

    /**
     * Main method: reads the configuration file; sets the settings for connecting to DMP and ci360;
     * creates an agent to listen for ci360 events; reads a taxonomy file using the CleverDataReader class;
     * defines the processEvent method, in which the intercepted event is analyzed and sent requests to DMP and ci360.
     */
    public static void main(String[] args) {
        // TODO Auto-generated method stub
        try {
            final JsonObject config = readConfig(args[0]);

            // DMP settings
            final String dmpEndpoint = config.get("dmp_endpoint_get_user_info").getAsString();
            final String dmpAuthorizationToken = config.get("dmp_authorization_token").getAsString();

            // ci360 settings
            String ci360Endpoint = config.get("ci360_endpoint").getAsString();
            String ci360TenantId = config.get("ci360_tenant_id").getAsString();
            String ci360ClientSecret = config.get("ci360_client_secret").getAsString();

            final JsonArray dmpStreams = config.get("dmp_streams").getAsJsonArray();

            // DMP attribute list sent to CI360
            List<String> ci360Attributes = new ArrayList<String>();
            JsonArray attributes = config.get("ci360_attributes").getAsJsonArray();
            for (JsonElement attribute : attributes)
                ci360Attributes.add(attribute.getAsString());

            final CI360Agent agent = new CI360Agent(ci360Endpoint, ci360TenantId, ci360ClientSecret);
            CleverDataReader cleverDataReader = new CleverDataReader("cleverdata_taxonomy_client.xlsm");

            CI360StreamInterface streamListener = new CI360StreamInterface() {
                public boolean processEvent(String event) {
                    try {
                        //  Parse event
                        JsonObject attributes = parseEvent(event);
                        if (attributes == null)
                            return true;

                        //  Get user ID
                        String userId = null;
                        userId = attributes.get("datahub_id").getAsString();

//                        if (attributes.get("subject_id") != null)
//                            userId = attributes.get("subject_id").getAsString();
//                        else
//                            userId = attributes.get("email_id").getAsString();

                        for (JsonElement dmpStreamElement : dmpStreams) {
                            JsonObject dmpStream = dmpStreamElement.getAsJsonObject();
                            if (dmpStream.get("event_name") != null && dmpStream.get("event_name").getAsString().equals(attributes.get("eventName").getAsString())) {
                                //Send to dmp
                                JsonObject responseDMP = sendDMP(dmpEndpoint, dmpAuthorizationToken, userId);
                                if (responseDMP == null)
                                    return true;

                                // Send to ci360
                                if (dmpStream.get("ci360_external_event") != null)
                                    sendCI360(dmpStream.get("ci360_external_event").getAsString(), userId, responseDMP);
                            }
                        }
                    } catch (Exception e) {
                        System.out.println(e.getMessage());
                        e.printStackTrace();

                        return true;
                    }

                    return true;
                }

                /**
                 * This method parses the information about the intercepted event.
                 * If the information does not contain identification attributes,
                 * then null is returned, which leads to the completion of event handling.
                 * @param event This parameter is a json string that contains the event fields
                 * @return JsonObject returns a json object that represents the attributes of the intercepted event
                 */
                public JsonObject parseEvent(String event) {
                    JsonParser parser = new JsonParser();
                    JsonObject eventJson = parser.parse(event).getAsJsonObject();
                    JsonObject attributes = eventJson.get("attributes").getAsJsonObject();

//                    if (!attributes.has("subject_id") && !attributes.has("email_id") && !attributes.has("datahub_id"))
                    if (!attributes.has("datahub_id")) {
                        System.out.println("Event \"" + attributes.get("eventname").getAsString() + "\" has no identity and rejected");

                        return null;
                    } else
                        System.out.println("Event \"" + attributes.get("eventname").getAsString() + "\" received");

                    return attributes;
                }

                /**
                 * This method sends a request to the DMP to get information about the user.
                 * Data from DMP comes in encrypted form, so decryption occurs in the same method.
                 * Data decryption is carried out using the CleverDataConverter class.
                 * Depending on the DMP and the data provider, you will need to write your own decryption class.
                 * The code inside was written based on the demo case.
                 * You can define your own rules for sending and processing a request.
                 * @param endpoint This parameter is the url of the DMP endpoint that is used to access server methods.
                 * @param authorizationToken This parameter is an authorization token.
                 * @param userId This parameter in the demo case is the user id for which you need to get information.
                 * @return JsonObject returns a json object that represents the decrypted response from the DMP
                 */
                public JsonObject sendDMP(String endpoint, String authorizationToken, String userId) {
                    Gson gson = new GsonBuilder().setPrettyPrinting().create();
                    JsonParser parser = new JsonParser();

                    JsonObject codedDataJsonObject = null;
                    try {
                        URL url = new URL(endpoint + userId);
                        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
                        connection.setRequestMethod("GET");
                        connection.setRequestProperty("Authorization", authorizationToken);

                        BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
                        StringBuffer response = new StringBuffer();
                        String inputLine = null;
                        while ((inputLine = in.readLine()) != null)
                            response.append(inputLine);

                        codedDataJsonObject = parser.parse(response.toString()).getAsJsonObject();
                        in.close();
                    } catch (Exception e) {
                        System.out.println(e.getMessage());
                        e.printStackTrace();

                        return null;
                    }

                    // This is where the data is decrypted.
                    CleverDataConverter cleverDataConverter = new CleverDataConverter(cleverDataReader.attributesTaxonomy, cleverDataReader.dictionaryAttributesTaxonomy);
                    JsonObject convertedData = null;
                    if (codedDataJsonObject != null) {
                        convertedData = cleverDataConverter.getConvertedCookieData(codedDataJsonObject);
                        System.out.println("Response from DMP:");
                        System.out.println(gson.toJson(convertedData));
                    } else {
                        System.out.println("User not found...");

                        return null;
                    }

                    return convertedData;
                }

                /**
                 * This method sends a request to ci360, which triggers an external event defined in ci360.
                 * To initiate an external event,
                 * it is imperative that you specify the user identification attribute and the name of the external event.
                 * @param externalEvent This parameter is the name of the external event.
                 * @param userId This parameter is an identification attribute that can be datahub_id.
                 * @param userData This parameter is a json object with attributes of user received from DMP.
                 */
                public void sendCI360(String externalEvent, String userId, JsonObject userData) throws Exception {
                    Gson gson = new GsonBuilder().setPrettyPrinting().create();
                    JsonParser parser = new JsonParser();
                    JsonObject userAttributes = userData.get("attrs").getAsJsonObject();

                    JsonObject toCI360 = new JsonObject();
                    toCI360.addProperty("eventname", externalEvent);
                    toCI360.addProperty("datahub_id", userId);

//                    if (EmailValidator.getInstance().isValid(userId))
//                        toCI360.addProperty("email_id", userId);
//                    else
//                        toCI360.addProperty("datahub_id", userId);

//                    for (String attribute : ci360Attributes) {
//                        if (userAttributes.has(attribute))
//                            toCI360.add(attribute, userAttributes.get(attribute));
//                    }

                    if (userAttributes.has("consumerelectronics_interest_type"))
                        toCI360.addProperty("attribute_1", userAttributes.get("consumerelectronics_interest_type").getAsString());
                    if (userAttributes.has("fin_acc_balance_avg_3m"))
                        toCI360.addProperty("attribute_2", userAttributes.get("fin_acc_balance_avg_3m").getAsString());
                    if (userAttributes.has("leisure_hobby"))
                        toCI360.addProperty("attribute_3", userAttributes.get("leisure_hobby").getAsString());
                    if (userAttributes.has("sd_age_estimated"))
                        toCI360.addProperty("attribute_4", userAttributes.get("sd_age_estimated").getAsString());
                    if (userAttributes.has("sd_job_pos_category"))
                        toCI360.addProperty("attribute_5", userAttributes.get("sd_job_pos_category").getAsString());
                    if (userAttributes.has("consumerelectronics_owner_type"))
                        toCI360.addProperty("attribute_6", userAttributes.get("consumerelectronics_owner_type").getAsString());


                    System.out.println("The following event will be send to CI360:");
                    System.out.println(gson.toJson(toCI360));

                    System.out.println("Response from CI360:");
                    System.out.println(gson.toJson(parser.parse(sendToCI360(toCI360.toString(), "events", agent)).getAsJsonObject()));
                }

                public void streamClosed(ErrorCode errorCode, String message) {
                    if (exiting) {
                        System.out.println("Stream closed");
                    } else {
                        System.out.println("Stream closed " + errorCode + ": " + message);
                        try {
                            Thread.sleep(15000);
                        } catch (InterruptedException e) {

                        }
                        try {
                            //Try to reconnect to the event stream.
                            agent.startStream(this, true);
                        } catch (CI360AgentException e) {
                            System.err.println("ERROR " + e.getErrorCode() + ": " + e.getMessage());
                        }
                    }
                }
            };
            agent.startStream(streamListener, true);

            // Continue until user enters "exit" to standard input.
            Scanner in = new Scanner(System.in);
            while (true) {
                String input = in.nextLine();
                if (input.equalsIgnoreCase("exit")) {
                    exiting = true;
                    agent.stopStream();
                    in.close();
                    try {
                        Thread.sleep(2000);
                    } catch (InterruptedException e) {

                    }
                    System.exit(0);
                } else if (input.startsWith("send ")) {
                    try {
                        String message = agent.injectEvent(input.substring(5));
                        System.out.println("SUCCESS: " + message);
                    } catch (CI360AgentException e) {
                        System.err.println("ERROR: " + e.getMessage());
                    }
                } else if (input.startsWith("ping")) {
                    try {
                        String message = agent.ping();
                        System.out.println("SUCCESS: " + message);
                    } catch (CI360AgentException e) {
                        System.err.println("ERROR: " + e.getMessage());
                    }
                } else if (input.startsWith("config")) {
                    try {
                        String message = agent.getAgentConfig();
                        System.out.println("SUCCESS: " + message);
                    } catch (CI360AgentException e) {
                        System.err.println("ERROR: " + e.getMessage());
                    }
                } else if (input.startsWith("healthcheck")) {
                    try {
                        String message = agent.healthcheck();
                        System.out.println("SUCCESS: " + message);
                    } catch (CI360AgentException e) {
                        System.err.println("ERROR: " + e.getMessage());
                    }
                } else if (input.startsWith("connection")) {
                    boolean status = agent.isConnected();
                    System.out.println("Connection Status: " + (status ? "UP" : "DOWN"));
                } else if (input.startsWith("diag")) {
                    try {
                        String message = agent.diagnostics();
                        System.out.println("SUCCESS: " + message);
                    } catch (CI360AgentException e) {
                        System.err.println("ERROR: " + e.getMessage());
                    }
                } else if (input.startsWith("bulk ")) {
                    try {
                        String message = agent.requestBulkEventURL(input.substring(5));
                        System.out.println("SUCCESS  URL: " + message);
                    } catch (CI360AgentException e) {
                        System.err.println("ERROR: " + e.getMessage());
                    }
                } else if (input.startsWith("sendmessage ")) {
                    try {
                        agent.sendWebSocketMessage(input.substring(12).trim());
                        System.out.println("SUCCESS: " + input.substring(12).trim());
                    } catch (CI360AgentException e) {
                        System.err.println("ERROR: " + e.getMessage());
                    }
                }
            }

        } catch (
                CI360AgentException e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
            System.exit(-1);
        } catch (FileNotFoundException e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
            System.exit(-1);
        }

    }

    private static JsonObject readConfig(String filepath) throws FileNotFoundException {
        Gson gson = new Gson();
        File jsonFile = Paths.get(filepath).toFile();
        return gson.fromJson(new FileReader(jsonFile), JsonObject.class);
    }

    private static String sendToCI360(String message, String endpoint, CI360Agent agent) throws Exception {
        ContentResponse response = agent.postRequest(endpoint, message);
        byte[] messageBytes = response.getContent();
        return new String(messageBytes);
    }
}
