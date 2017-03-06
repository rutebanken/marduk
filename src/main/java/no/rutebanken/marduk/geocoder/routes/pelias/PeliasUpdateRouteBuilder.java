package no.rutebanken.marduk.geocoder.routes.pelias;


import no.rutebanken.marduk.geocoder.GeoCoderConstants;
import no.rutebanken.marduk.geocoder.routes.pelias.babylon.DeploymentStatus;
import no.rutebanken.marduk.geocoder.routes.pelias.babylon.PodStatus;
import no.rutebanken.marduk.geocoder.routes.pelias.babylon.ScalingOrder;
import no.rutebanken.marduk.geocoder.routes.pelias.babylon.StartFile;
import no.rutebanken.marduk.routes.BaseRouteBuilder;
import no.rutebanken.marduk.routes.status.SystemStatus;
import org.apache.camel.Exchange;
import org.apache.camel.LoggingLevel;
import org.apache.camel.component.http4.HttpMethods;
import org.apache.camel.model.dataformat.JsonLibrary;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import static no.rutebanken.marduk.Constants.JOB_STATUS_ROUTING_DESTINATION;
import static no.rutebanken.marduk.geocoder.GeoCoderConstants.*;

@Component
public class PeliasUpdateRouteBuilder extends BaseRouteBuilder {

	/**
	 * One time per 24H on MON-FRI
	 */
	@Value("${pelias.update.cron.schedule:0+0+23+?+*+MON-FRI}")
	private String cronSchedule;

	@Value("${babylon.url:http4://babylon/babylon/api}")
	private String babylonUrl;

	@Value("${elasticsearch.scratch.deployment.name:es-scratch}")
	private String elasticsearchScratchDeploymentName;

	@Value("${elasticsearch.build.file.name:es-image-build-pod.yaml}")
	private String elasticsearchBuildFileName;

	@Value("${elasticsearch.build.job.name:=es-build-job}")
	private String elasticsearchBuildJobName;

	@Value("${tiamat.max.retries:3000}")
	private int maxRetries;

	@Value("${tiamat.retry.delay:15000}")
	private long retryDelay;

	@Autowired
	private PeliasUpdateStatusService updateStatusService;

	private static String NO_OF_REPLICAS = "RutebankenESNoOfReplicas";

	@Override
	public void configure() throws Exception {
		super.configure();

		singletonFrom("quartz2://marduk/peliasUpdate?cron=" + cronSchedule + "&trigger.timeZone=Europe/Oslo")
				.autoStartup("{{pelias.update.autoStartup:false}}")
				.log(LoggingLevel.INFO, "Quartz triggers download of administrative units.")
				.setBody(constant(PELIAS_UPDATE_START))
				.to("direct:geoCoderStart")
				.routeId("pelias-update-quartz");

		from(PELIAS_UPDATE_START.getEndpoint())
				.log(LoggingLevel.INFO, "Start updating Pelias")
				.bean(updateStatusService, "setBuilding")
				.process(e -> SystemStatus.builder(e).start(SystemStatus.Action.UPDATE).entity("Pelias").build()).to("direct:updateSystemStatus")
				.to("direct:startElasticsearchScratchInstance")
				.routeId("pelias-upload");

		from("direct:startElasticsearchScratchInstance")
				.to("direct:getElasticsearchScratchStatus")

				.choice()
				.when(simple("${body.availableReplicas} > 0"))
				// Shutdown if already running
				.log(LoggingLevel.INFO, "Elasticsearch scratch instance already running. Scaling down first.")
				.setHeader(JOB_STATUS_ROUTING_DESTINATION, constant("direct:startElasticsearchScratchInstance"))
				.to("direct:shutdownElasticsearchScratchInstance")
				.otherwise()
				.setHeader(NO_OF_REPLICAS, constant(1))
				.setHeader(JOB_STATUS_ROUTING_DESTINATION, constant("direct:insertElasticsearchIndexData"))
				.to("direct:rescaleElasticsearchScratchInstance")
				.end()

				.routeId("pelias-es-scratch-start");


		from("direct:insertElasticsearchIndexDataCompleted")
				.setHeader(JOB_STATUS_ROUTING_DESTINATION, constant("direct:buildElasticsearchImage"))
				.setProperty(GEOCODER_NEXT_TASK, constant(GeoCoderConstants.PELIAS_ES_SCRATCH_STOP))
				.routeId("pelias-es-index-complete");


		from("direct:insertElasticsearchIndexDataFailed")
				.bean(updateStatusService, "setIdle")
				.process(e -> SystemStatus.builder(e).state(SystemStatus.State.FAILED).build()).to("direct:updateSystemStatus")
				.routeId("pelias-es-index-failed");

		from("direct:shutdownElasticsearchScratchInstance")
				.setHeader(NO_OF_REPLICAS, constant(0))
				.to("direct:rescaleElasticsearchScratchInstance")
				.routeId("pelias-es-scratch-shutdown");


		from("direct:rescaleElasticsearchScratchInstance")
				.log(LoggingLevel.INFO, "Requesting Babylon to scale Elasticsearch scratch to ${header." + NO_OF_REPLICAS + "} replicas")
				.setHeader(Exchange.CONTENT_TYPE, constant("application/json"))
				.setHeader(Exchange.HTTP_METHOD, constant(HttpMethods.POST))
				.process(e -> e.getIn().setBody(new ScalingOrder(elasticsearchScratchDeploymentName, "marduk", e.getIn().getHeader(NO_OF_REPLICAS, Integer.class))))
				.marshal().json(JsonLibrary.Jackson)
				.to(babylonUrl + "/scale")
				.setProperty(GEOCODER_NEXT_TASK, constant(GeoCoderConstants.PELIAS_ES_SCRATCH_STATUS_POLL))
				.routeId("pelias-es-scratch-rescale");

		from("direct:pollElasticsearchScratchStatus")
				.to("direct:getElasticsearchScratchStatus")
				.choice()
				.when(simple("${body.availableReplicas} == ${header." + NO_OF_REPLICAS + "}"))
				.toD("${header." + JOB_STATUS_ROUTING_DESTINATION + "}")
				.otherwise()
				.setProperty(GEOCODER_RESCHEDULE_TASK, constant(true))
				.end()
				.routeId("pelias-es-scratch-status-poll");

		from("direct:getElasticsearchScratchStatus")
				.setBody(constant(null))
				.setHeader(Exchange.HTTP_METHOD, constant(HttpMethods.GET))
				.to(babylonUrl + "/status?deployment=" + elasticsearchScratchDeploymentName)
				.unmarshal().json(JsonLibrary.Jackson, DeploymentStatus.class)
				.routeId("pelias-es-scratch-status");


		from("direct:buildElasticsearchImage")
				.log(LoggingLevel.INFO, "Requesting Babylon to build new elasticsearch image for pelias")
				.setHeader(Exchange.CONTENT_TYPE, constant("application/json"))
				.setHeader(Exchange.HTTP_METHOD, constant(HttpMethods.POST))
				.setBody(constant(new StartFile(elasticsearchBuildFileName)))
				.marshal().json(JsonLibrary.Jackson)
				.to(babylonUrl + "/run")
				.setProperty(GEOCODER_NEXT_TASK, constant(GeoCoderConstants.PELIAS_ES_BUILD_POLL))
				.routeId("pelias-es-build");

		from("direct:pollElasticsearchBuildCompleted")
				.log(LoggingLevel.INFO, "Polling Babylon for status on elasticsearch build job")
				.setHeader(Exchange.CONTENT_TYPE, constant("application/json"))
				.setHeader(Exchange.HTTP_METHOD, constant(HttpMethods.GET))
				.setBody(constant(null))
				.to(babylonUrl+"/pod?name="+elasticsearchBuildJobName)
				.unmarshal().json(JsonLibrary.Jackson, PodStatus.class)
				.choice()
				.when(simple("${body.present} == false"))
				.to("direct:processPeliasDeployCompleted")
				.otherwise()
				.log(LoggingLevel.INFO, "Elasticsearch build job still running, rescheduling poll job")
				.setProperty(GEOCODER_RESCHEDULE_TASK, constant(true))
				.end()
				.routeId("pelias-es-build-poll-completed");


		from("direct:processPeliasDeployCompleted")
				.log(LoggingLevel.INFO, "Finished updating pelias")
				.process(e -> SystemStatus.builder(e).state(SystemStatus.State.OK).build()).to("direct:updateSystemStatus")
				.bean(updateStatusService, "setIdle")
				.routeId("pelias-deploy-completed");


	}


}
