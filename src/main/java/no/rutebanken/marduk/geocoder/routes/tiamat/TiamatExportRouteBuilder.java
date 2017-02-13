package no.rutebanken.marduk.geocoder.routes.tiamat;


import no.rutebanken.marduk.Constants;
import no.rutebanken.marduk.geocoder.routes.tiamat.xml.ExportJob;
import no.rutebanken.marduk.routes.BaseRouteBuilder;
import no.rutebanken.marduk.routes.status.StatusEvent;
import org.apache.camel.Exchange;
import org.apache.camel.LoggingLevel;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class TiamatExportRouteBuilder extends BaseRouteBuilder {

	/**
	 * One time per 24H on MON-FRI
	 */
	@Value("${tiamat.export.cron.schedule:0+*+*/23+?+*+MON-FRI}")
	private String cronSchedule;

	@Value("${tiamat.url}")
	private String tiamatUrl;

	private String tiamatExportPath = "jersey/publication_delivery/";

	@Override
	public void configure() throws Exception {
		super.configure();

		from("quartz2://marduk/tiamatExport?cron=" + cronSchedule + "&trigger.timeZone=Europe/Oslo")
				.log(LoggingLevel.INFO, "Quartz triggers Tiamat export.")
				.to("activemq:queue:TiamatExportQueue")
				.routeId("tiamat-export-quartz");

		singletonFrom("activemq:queue:TiamatExportQueue?transacted=true&messageListenerContainerFactoryRef=batchListenerContainerFactory")
				.autoStartup("{{tiamat.export.autoStartup:false}}")
				.transacted()
				.process(e -> StatusEvent.builder(e).start(StatusEvent.Action.EXPORT).entity("Tiamat publication delivery").build()).to("direct:sendStatusEvent")
				.log(LoggingLevel.INFO, "Start Tiamat export")
				.setHeader(Exchange.HTTP_METHOD, constant(org.apache.camel.component.http4.HttpMethods.GET))
				.setBody(constant(null))
				.to(tiamatUrl + tiamatExportPath + "async")
				.convertBodyTo(ExportJob.class)
				.setHeader(Constants.JOB_ID, simple("${body.id}"))
				.setHeader(Constants.JOB_STATUS_URL, simple(tiamatExportPath + "${body.jobUrl}"))
				.setHeader(Constants.JOB_STATUS_ROUTING_DESTINATION, constant("direct:processTiamatExportResults"))
				.log(LoggingLevel.INFO, "Started Tiamat export of file: ${body.fileName}")
				.to("activemq:queue:TiamatPollStatusQueue")
				.end()
				.routeId("tiamat-export");


		from("direct:processTiamatExportResults")
				.process(e -> StatusEvent.builder(e).state(StatusEvent.State.OK).build()).to("direct:sendStatusEvent")
				.to("activemq:queue:PeliasUpdateQueue")
				.routeId("tiamat-export-results");
	}


}
