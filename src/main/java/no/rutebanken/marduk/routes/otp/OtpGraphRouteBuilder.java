package no.rutebanken.marduk.routes.otp;

import no.rutebanken.marduk.routes.BaseRouteBuilder;
import no.rutebanken.marduk.routes.file.GtfsFileUtils;
import no.rutebanken.marduk.routes.status.Status;
import no.rutebanken.marduk.routes.status.SystemStatus;
import no.rutebanken.marduk.services.GraphStatusService;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.camel.LoggingLevel;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.stream.Collectors;

import static no.rutebanken.marduk.Constants.*;
import static org.apache.camel.builder.Builder.exceptionStackTrace;

/**
 * Trigger OTP graph building
 */
@Component
public class OtpGraphRouteBuilder extends BaseRouteBuilder {

	private static final String BUILD_CONFIG_JSON = "build-config.json";
	private static final String NORWAY_LATEST_OSM_PBF = "norway-latest.osm.pbf";

	@Value("${otp.graph.build.directory}")
	private String otpGraphBuildDirectory;

	/**
	 * This is the name which the graph file is stored remotely.
	 */
	@Value("${otp.graph.file.name:norway-latest.osm.pbf}")
	private String otpGraphFileName;

	@Value("${otp.graph.blobstore.subdirectory}")
	private String blobStoreSubdirectory;

	@Value("${osm.pbf.blobstore.subdirectory:osm}")
	private String blobStoreSubdirectoryForOsm;

	@Autowired
	GraphStatusService graphStatusService;

	private static final String PROP_MESSAGES = "RutebankenPropMessages";

	private static final String HEADER_STATUS = "RutebankenGraphBuildStatus";

	@Override
	public void configure() throws Exception {
		super.configure();

		singletonFrom("activemq:queue:OtpGraphQueue?transacted=true&maxConcurrentConsumers=1&messageListenerContainerFactoryRef=batchListenerContainerFactory").autoStartup("{{otp.graph.build.autoStartup:true}}")
				.transacted()
				.setProperty(PROP_MESSAGES, simple("${body}"))
				.process(e -> SystemStatus.builder(e).start(SystemStatus.Action.BUILD).entity(SystemStatus.Entity.GRAPH).build()).to("direct:updateSystemStatus")
				.to("direct:sendStatusStartedForJobs")
				.setProperty(TIMESTAMP, simple("${date:now:yyyyMMddHHmmss}"))
				.bean(graphStatusService, "setBuilding")
				.setProperty(OTP_GRAPH_DIR, simple(otpGraphBuildDirectory + "/${property." + TIMESTAMP + "}"))
				.log(LoggingLevel.INFO, getClass().getName(), correlation() + "Starting graph building in directory ${property." + OTP_GRAPH_DIR + "}.")
				.to("direct:fetchConfig")
				.to("direct:fetchMap")
				.to("direct:fetchLatestGtfs")
				.to("direct:mergeGtfs")
				.to("direct:buildGraph")
				.bean(graphStatusService, "setIdle")
				.log(LoggingLevel.INFO, getClass().getName(), correlation() + "Done with OTP graph building route.")
				.routeId("otp-graph-build");

		from("direct:sendStatusStartedForJobs")
				.transacted("PROPAGATION_REQUIRES_NEW")
				.setHeader(HEADER_STATUS, constant(Status.State.STARTED))
				.to("direct:sendStatusForJobs");

		from("direct:sendStatusForJobs")
				.split().exchangeProperty(PROP_MESSAGES)
				.filter(simple("${body.properties[" + CHOUETTE_REFERENTIAL + "]}"))
				.process(e -> {
					Status.State state = e.getIn().getHeader(HEADER_STATUS, Status.State.class);
					e.getIn().setHeaders(((ActiveMQMessage) e.getIn().getBody()).getProperties());
					Status.builder(e).action(Status.Action.BUILD_GRAPH).state(state).build();
				})
				.to("direct:updateStatus");

		from("direct:fetchLatestGtfs")
				.log(LoggingLevel.DEBUG, getClass().getName(), "Fetching gtfs files for all providers.")
				.setBody(simple(getAggregatedGtfsFiles()))
				.split(body())
				.to("direct:getGtfsFiles")
				.routeId("otp-graph-fetch-gtfs");

		from("direct:getGtfsFiles")
				.log(LoggingLevel.INFO, getClass().getName(), correlation() + "Fetching " + BLOBSTORE_PATH_OUTBOUND + "gtfs/${body}")
				.setProperty("fileName", body())
				.setHeader(FILE_HANDLE, simple(BLOBSTORE_PATH_OUTBOUND + "gtfs/${property.fileName}"))
				.to("direct:getBlob")
				.choice()
				.when(body().isNotEqualTo(null))
				.toD("file:" + otpGraphBuildDirectory + "?fileName=${property." + TIMESTAMP + "}/org/${property.fileName}")
				.otherwise()
				.log(LoggingLevel.INFO, getClass().getName(), correlation() + "${property.fileName} was empty when trying to fetch it from blobstore.")
				.routeId("otp-graph-get-gtfs");

		from("direct:mergeGtfs")
				.log(LoggingLevel.DEBUG, getClass().getName(), "Merging gtfs files for all providers.")
				.setBody(simple(otpGraphBuildDirectory + "/${property." + TIMESTAMP + "}/org"))
				.bean(method(GtfsFileUtils.class, "mergeGtfsFilesInDirectory"))
				.toD("file:" + otpGraphBuildDirectory + "?fileName=${property." + TIMESTAMP + "}/merged.zip")
				.routeId("otp-graph-merge-gtfs");

		from("direct:fetchConfig")
				.log(LoggingLevel.DEBUG, getClass().getName(), correlation() + "Fetching config ...")
				.to("language:constant:resource:classpath:no/rutebanken/marduk/routes/otp/" + BUILD_CONFIG_JSON)
				.toD("file:" + otpGraphBuildDirectory + "?fileName=${property." + TIMESTAMP + "}/" + BUILD_CONFIG_JSON)
				.log(LoggingLevel.DEBUG, getClass().getName(), correlation() + BUILD_CONFIG_JSON + " fetched.")
				.routeId("otp-graph-fetch-config");

		from("direct:fetchMap")
				.log(LoggingLevel.DEBUG, getClass().getName(), correlation() + "Fetching map ...")
				.setHeader(FILE_HANDLE, simple(blobStoreSubdirectoryForOsm + "/" + otpGraphFileName))
				.to("direct:getBlob")
				// Should really store to otpGraphFileName, but store to NORWAY_LATEST in fear of side effects later in the build
				.toD("file:" + otpGraphBuildDirectory + "?fileName=${property." + TIMESTAMP + "}/" + NORWAY_LATEST_OSM_PBF)
				.log(LoggingLevel.DEBUG, getClass().getName(), correlation() + NORWAY_LATEST_OSM_PBF + " fetched (original name: " + otpGraphFileName + ").")
				.routeId("otp-graph-fetch-map");


		from("direct:buildGraph")
				.log(LoggingLevel.INFO, correlation() + "Building OTP graph...")
				.doTry()
				.process(new GraphBuilderProcessor())
				.setBody(constant(""))
				.toD("file:" + otpGraphBuildDirectory + "?fileName=${property." + TIMESTAMP + "}/" + GRAPH_OBJ + ".done")
				.to("log:" + getClass().getName() + "?level=DEBUG&showAll=true&multiline=true")
				.log(LoggingLevel.INFO, correlation() + "Done building new OTP graph.")
				.setHeader(HEADER_STATUS, constant(Status.State.OK))
				.to("direct:sendStatusForJobs")
				.process(e -> SystemStatus.builder(e).state(SystemStatus.State.OK).build()).to("direct:updateSystemStatus")
				.doCatch(Exception.class)
				.log(LoggingLevel.ERROR, correlation() + "Graph building failed: " + exceptionMessage() + " stacktrace: " + exceptionStackTrace())
				.process(e -> SystemStatus.builder(e).state(SystemStatus.State.FAILED).build()).to("direct:updateSystemStatus")
				.setHeader(HEADER_STATUS, constant(Status.State.FAILED))
				.to("direct:sendStatusForJobs")
				.end()
				.routeId("otp-graph-build-otp");

	}

	String getAggregatedGtfsFiles() {
		return getProviderRepository().getProviders().stream()
				       .filter(p -> p.chouetteInfo.migrateDataToProvider == null)
				       .map(p -> p.chouetteInfo.referential + "-" + CURRENT_AGGREGATED_GTFS_FILENAME)
				       .collect(Collectors.joining(","));
	}

}
