import groovy.json.JsonBuilder
import org.apache.nifi.components.state.Scope
import org.apache.nifi.components.state.StateManager
import org.apache.nifi.components.state.StateMap
import org.apache.nifi.reporting.EventAccess
import org.apache.nifi.provenance.ProvenanceEventRepository
import org.apache.nifi.provenance.ProvenanceEventRecord
import java.net.HttpURLConnection
import java.net.URL
import java.util.HashMap
import java.util.List
import java.util.Map
// Starting fetch
log.info("Starting to fetch provenance events");
// Get Provenance Events
final StateManager stateManager = context.getStateManager();
final EventAccess access = context.getEventAccess();
final ProvenanceEventRepository provenance = access.getProvenanceRepository();
// Retrieve last fetched event ID from state
final StateMap state = stateManager.getState(Scope.LOCAL);
final String lastEventIdStr = state.get("lastEventId");
long lastEventId = (lastEventIdStr != null) ? Long.parseLong(lastEventIdStr) : 0;
 
// Fetch events starting from the last fetched event ID
final List<ProvenanceEventRecord> events = provenance.getEvents(lastEventId + 1, 10000);
// Process events if any were fetched
if (!events.isEmpty()) {
    // Log number of events fetched
    log.info("Fetched " + events.size() + " provenance events");
    // Get ID of the last fetched event
    long newLastEventId = events.get(events.size() - 1).getEventId();
    // Update the last fetched event ID in the state manager
    final Map<String, String> newState = new HashMap<>();
    newState.put("lastEventId", String.valueOf(newLastEventId));
    stateManager.setState(newState, Scope.LOCAL);
    log.info("Checkpoint updated. Last event ID: " + newLastEventId);
} else {
    log.info("No new events to fetch.");
}
// Convert to JSON
List eventList = []
for (ProvenanceEventRecord event : events) {
    Map eventMap = [:]
    eventMap['eventId'] = event.getEventId()
    eventMap['eventType'] = event.getEventType().toString()
    eventMap['componentId'] = event.getComponentId()
    eventMap['componentType'] = event.getComponentType()
    eventMap['flowFileUuid'] = event.getFlowFileUuid()
    eventMap['eventTime'] = event.getEventTime()
    eventMap['details'] = event.getDetails()
    // Include attributes
    Map<String, String> attributes = event.getAttributes()
    if (attributes != null && !attributes.isEmpty()) {
        eventMap['attributes'] = attributes
    }
    eventList.add(eventMap)
}
String jsonPayload = new JsonBuilder(eventList).toPrettyString()
// Set URL endpoint
String urlEndpoint = "http://localhost:9999/provenance"
// Set HTTP Connection
URL url = new URL(urlEndpoint)
HttpURLConnection connection = (HttpURLConnection) url.openConnection()
connection.setRequestMethod("POST")
connection.setRequestProperty("Content-Type", "application/json")
connection.setDoOutput(true)
// Send json using HTTP POST
connection.outputStream.withWriter("UTF-8") { writer ->
    writer.write(jsonPayload)
}
// Check for response
int responseCode = connection.responseCode
if (responseCode == HttpURLConnection.HTTP_OK) {
    log.info("Successfully sent JSON message to ${urlEndpoint}")
} else {
    log.error("Failed to send JSON message. Response code: ${responseCode}")
}
// Close connection
connection.disconnect()
