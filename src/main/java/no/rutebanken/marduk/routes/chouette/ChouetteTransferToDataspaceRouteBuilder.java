/*
 * Licensed under the EUPL, Version 1.2 or – as soon they will be approved by
 * the European Commission - subsequent versions of the EUPL (the "Licence");
 * You may not use this work except in compliance with the Licence.
 * You may obtain a copy of the Licence at:
 *
 *   https://joinup.ec.europa.eu/software/page/eupl
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the Licence is distributed on an "AS IS" basis,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Licence for the specific language governing permissions and
 * limitations under the Licence.
 *
 */

package no.rutebanken.marduk.routes.chouette;

import no.rutebanken.marduk.Constants;
import no.rutebanken.marduk.domain.Provider;
import no.rutebanken.marduk.routes.chouette.json.Parameters;
import no.rutebanken.marduk.routes.status.JobEvent;
import no.rutebanken.marduk.routes.status.JobEvent.State;
import no.rutebanken.marduk.routes.status.JobEvent.TimetableAction;
import org.apache.camel.LoggingLevel;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import static no.rutebanken.marduk.Constants.CHOUETTE_JOB_STATUS_JOB_VALIDATION_LEVEL;
import static no.rutebanken.marduk.Constants.CHOUETTE_REFERENTIAL;
import static no.rutebanken.marduk.Constants.JSON_PART;
import static no.rutebanken.marduk.Constants.PROVIDER_ID;
import static no.rutebanken.marduk.Utils.getLastPathElementOfUrl;

/**
 * Transfers data from one space to another in Chouette
 */
@Component
public class ChouetteTransferToDataspaceRouteBuilder extends AbstractChouetteRouteBuilder {

    @Value("${chouette.url}")
    private String chouetteUrl;

    @Override
    public void configure() throws Exception {
        super.configure();

        from("google-pubsub:{{marduk.pubsub.project.id}}:ChouetteTransferExportQueue")
				.streamCache("true")
				.process(this::setCorrelationIdIfMissing)
				.removeHeader(Constants.CHOUETTE_JOB_ID)
        		.log(LoggingLevel.INFO, getClass().getName(), correlation() + "Starting Chouette transfer")
                .process(e -> {
                	e.getIn().setHeader(Constants.FILE_HANDLE,"transfer.zip");
                	Provider provider = getProviderRepository().getProvider(e.getIn().getHeader(PROVIDER_ID, Long.class));
                	Provider destProvider = getProviderRepository().getProvider(provider.getChouetteInfo().getMigrateDataToProvider());
					e.getIn().setHeader(CHOUETTE_REFERENTIAL, provider.getChouetteInfo().getReferential());
                	e.getIn().setHeader(JSON_PART, Parameters.getTransferExportParameters(provider,destProvider));

                })
				.process(e -> JobEvent.providerJobBuilder(e).timetableAction(TimetableAction.DATASPACE_TRANSFER).state(State.PENDING).build())
		        .to("direct:updateStatus")
				.log(LoggingLevel.DEBUG, correlation() + "Creating multipart request")
                .process(this::toGenericChouetteMultipart)
                .toD(chouetteUrl + "/chouette_iev/referentials/${header." + CHOUETTE_REFERENTIAL + "}/exporter/transfer")
                .process(e -> {
                    e.getIn().setHeader(Constants.CHOUETTE_JOB_STATUS_URL, e.getIn().getHeader("Location", String.class));
	                e.getIn().setHeader(Constants.CHOUETTE_JOB_ID, getLastPathElementOfUrl(e.getIn().getHeader("Location", String.class)));
                    e.getIn().setHeader(Constants.CHOUETTE_JOB_STATUS_ROUTING_DESTINATION,"direct:processTransferExportResult");
                    e.getIn().setHeader(Constants.CHOUETTE_JOB_STATUS_JOB_TYPE, JobEvent.TimetableAction.DATASPACE_TRANSFER.name());
                    })
                .log(LoggingLevel.INFO,correlation() + "Sending transfer export to poll job status")
                .to(logDebugShowAll())
		        .removeHeader("loopCounter")
				.setBody(constant(""))
                .to("google-pubsub:{{marduk.pubsub.project.id}}:ChouettePollStatusQueue")
                .routeId("chouette-send-transfer-job");

 		 from("direct:processTransferExportResult")
 		        .to(logDebugShowAll())
 		        .setBody(constant(""))
 		        .choice()
// 				.when(PredicateBuilder.and(simple("false", Boolean.class).isEqualTo(header(Constants.ENABLE_VALIDATION)),simple("${header.action_report_result} == 'OK'")))
// 		            .to("direct:checkScheduledJobsBeforeTriggeringRBSpaceValidation")
// 		            .process(e -> Status.addStatus(e, TimetableAction.DATASPACE_TRANSFER, State.OK))
 		        .when(simple("${header.action_report_result} == 'OK'"))
				 	.process(e -> JobEvent.providerJobBuilder(e).timetableAction(JobEvent.TimetableAction.DATASPACE_TRANSFER).state(State.OK).build())
	 		        .to("direct:updateStatus")
	                .process(e -> {
	                	// Update provider, now context switches to next provider level
	                	Provider currentProvider = getProviderRepository().getProvider(e.getIn().getHeader(PROVIDER_ID, Long.class));
	                	e.getIn().setHeader(Constants.ORIGINAL_PROVIDER_ID,e.getIn().getHeader(Constants.ORIGINAL_PROVIDER_ID,e.getIn().getHeader(Constants.PROVIDER_ID)));
	                	e.getIn().setHeader(Constants.PROVIDER_ID, currentProvider.getChouetteInfo().getMigrateDataToProvider());
	                })
 		            .to("direct:checkScheduledJobsBeforeTriggeringRBSpaceValidation")
 		        .when(simple("${header.action_report_result} == 'NOK'"))
 		        	.log(LoggingLevel.INFO,correlation() + "Transfer failed")
				 	.process(e -> JobEvent.providerJobBuilder(e).timetableAction(TimetableAction.DATASPACE_TRANSFER).state(State.FAILED).build())
 	 		        .to("direct:updateStatus")
 		        .otherwise()
 		            .log(LoggingLevel.ERROR,correlation() + "Something went wrong on transfer")
				 	.process(e -> JobEvent.providerJobBuilder(e).timetableAction(TimetableAction.DATASPACE_TRANSFER).state(State.FAILED).build())
 	 		        .to("direct:updateStatus")
 		        .end()
 		        .routeId("chouette-process-transfer-status");

 		 // Check that no other import jobs in status SCHEDULED exists for this referential. If so, do not trigger export
 		from("direct:checkScheduledJobsBeforeTriggeringRBSpaceValidation")
 			.setProperty("job_status_url",simple("{{chouette.url}}/chouette_iev/referentials/${header." + CHOUETTE_REFERENTIAL + "}/jobs?timetableAction=importer&status=SCHEDULED&status=STARTED"))
 			.toD("${exchangeProperty.job_status_url}")
 			.choice()
 			.when().jsonpath("$.*[?(@.status == 'SCHEDULED')].status")
 				.log(LoggingLevel.INFO,correlation() + "Transfer ok, skipping validation as ther are more import jobs active")
             .otherwise()
 				.log(LoggingLevel.INFO,correlation() + "Transfer ok, triggering validation.")
 		        .setBody(constant(""))

 		        .to(logDebugShowAll())
				.setHeader(CHOUETTE_JOB_STATUS_JOB_VALIDATION_LEVEL,constant(JobEvent.TimetableAction.VALIDATION_LEVEL_2.name()))
				.to("google-pubsub:{{marduk.pubsub.project.id}}:ChouetteValidationQueue")
             .end()
             .routeId("chouette-process-job-list-after-transfer");

    }

}


