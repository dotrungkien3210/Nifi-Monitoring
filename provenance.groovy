import groovy.json.JsonBuilder
import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.components.state.Scope
import org.apache.nifi.components.state.StateManager
import org.apache.nifi.components.state.StateMap
import org.apache.nifi.reporting.EventAccess
import org.apache.nifi.provenance.ProvenanceEventRepository
import org.apache.nifi.provenance.ProvenanceEventRecord
import groovy.transform.Field
import java.net.InetAddress
import java.net.HttpURLConnection
import java.net.URL
import java.util.HashMap
import java.util.List
import java.util.Map
@Field boolean isStateCleared = false // Initialize the state as not cleared
// Declare custom properties
PropertyDescriptor urlEndpointDescriptor = new PropertyDescriptor.Builder()
    .name("urlEndpoint")
    .description("The URL endpoint to send JSON data")
    .required(true)
    .build()
PropertyDescriptor clearStateDescriptor = new PropertyDescriptor.Builder()
    .name("clearState")
    .description("Clear the current state if set to true on startup")
    .required(true)
    .allowableValues("true", "false")
    .defaultValue("false")
    .build()
// Get values from the properties
String urlEndpoint = context.getProperty(urlEndpointDescriptor).getValue()
boolean clearState = context.getProperty(clearStateDescriptor).asBoolean()
log.info("Starting fetch provenance events with URL endpoint: ${urlEndpoint} and clearState: ${clearState}")
final StateManager stateManager = context.getStateManager()
final StateMap state = stateManager.getState(Scope.LOCAL)
// Clear the state if configured to do so
if (clearState && !isStateCleared) {
    stateManager.clear(Scope.LOCAL)
    isStateCleared = true
    log.info("State cleared as per initial clearState configuration")
}
// Retrieve the last processed event ID from the state
final String lastEventIdStr = state.get("lastEventId")
long lastEventId = (lastEventIdStr != null) ? Long.parseLong(lastEventIdStr) : 0
final EventAccess access = context.getEventAccess()
final ProvenanceEventRepository provenance = access.getProvenanceRepository()
// Initialize from the first event if no events have been processed
if (lastEventId < 0) {
    List<ProvenanceEventRecord> allEvents = provenance.getEvents(0, 1)
    if (!allEvents.isEmpty()) {
        lastEventId = allEvents.get(0).getEventId() - 1
        log.info("Initialized lastEventId to the smallest existing event ID: ${lastEventId}")
    }
}
// Fetch new events from Provenance
final List<ProvenanceEventRecord> events = provenance.getEvents(lastEventId + 1, 3000)
if (!events.isEmpty()) {
    log.info("Fetched ${events.size()} provenance events")
    // Update the state with the latest event ID
    long newLastEventId = events.get(events.size() - 1).getEventId()
    final Map<String, String> newState = new HashMap<>()
    newState.put("lastEventId", String.valueOf(newLastEventId))
    stateManager.setState(newState, Scope.LOCAL)
    log.info("Checkpoint updated. Last event ID: ${newLastEventId}")
    // Retrieve the hostname of the server
    def hostname = InetAddress.getLocalHost().getHostName()
    // Convert the list of events to JSON
    List<Map> eventList = []
    for (ProvenanceEventRecord event : events) {
        Map eventMap = [:]
        eventMap['eventId'] = event.getEventId()
        eventMap['eventType'] = event.getEventType().toString()
        eventMap['componentId'] = event.getComponentId()
        eventMap['componentType'] = event.getComponentType()
        eventMap['flowFileUuid'] = event.getFlowFileUuid()
        eventMap['eventTime'] = event.getEventTime()
        eventMap['details'] = event.getDetails()
        eventMap['hostname'] = hostname
        Map<String, String> attributes = event.getAttributes()
        if (attributes != null && !attributes.isEmpty()) {
            eventMap['attributes'] = attributes
        }
        eventList.add(eventMap)
    }
    String jsonPayload = new JsonBuilder(eventList).toPrettyString()
    // Send the JSON via HTTP POST
    try {
        URL url = new URL(urlEndpoint)
        HttpURLConnection connection = (HttpURLConnection) url.openConnection()
        connection.setRequestMethod("POST")
        connection.setRequestProperty("Content-Type", "application/json")
        connection.setDoOutput(true)
        connection.outputStream.withWriter("UTF-8") { writer ->
            writer.write(jsonPayload)
        }
        int responseCode = connection.responseCode
        if (responseCode == HttpURLConnection.HTTP_OK) {
            log.info("Successfully sent JSON to ${urlEndpoint}")
        } else {
            log.error("Failed to send JSON. Response code: ${responseCode}")
        }
        connection.disconnect()
    } catch (Exception e) {
        log.error("Error while sending JSON to ${urlEndpoint}: ${e.message}")
    }
} else {
    log.info("No new events to fetch.")
}
log.info("Completed fetching and sending provenance events")
