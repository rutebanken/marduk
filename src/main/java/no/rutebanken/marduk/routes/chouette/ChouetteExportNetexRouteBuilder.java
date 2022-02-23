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
import no.rutebanken.marduk.routes.chouette.json.ActionReportWrapper;
import no.rutebanken.marduk.routes.chouette.json.Parameters;
import no.rutebanken.marduk.routes.status.JobEvent;
import org.apache.camel.Exchange;
import org.apache.camel.LoggingLevel;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import static no.rutebanken.marduk.Constants.BLOBSTORE_MAKE_BLOB_PUBLIC;
import static no.rutebanken.marduk.Constants.BLOBSTORE_PATH_NETEX_EXPORT;
import static no.rutebanken.marduk.Constants.BLOBSTORE_PATH_NETEX_EXPORT_BEFORE_VALIDATION;
import static no.rutebanken.marduk.Constants.CHOUETTE_JOB_STATUS_URL;
import static no.rutebanken.marduk.Constants.CHOUETTE_REFERENTIAL;
import static no.rutebanken.marduk.Constants.CORRELATION_ID;
import static no.rutebanken.marduk.Constants.DATASET_REFERENTIAL;
import static no.rutebanken.marduk.Constants.FILE_HANDLE;
import static no.rutebanken.marduk.Constants.JSON_PART;
import static no.rutebanken.marduk.Constants.PROVIDER_ID;
import static no.rutebanken.marduk.Constants.VALIDATION_CLIENT_HEADER;
import static no.rutebanken.marduk.Constants.VALIDATION_CLIENT_MARDUK;
import static no.rutebanken.marduk.Constants.VALIDATION_CORRELATION_ID_HEADER;
import static no.rutebanken.marduk.Constants.VALIDATION_DATASET_FILE_HANDLE_HEADER;
import static no.rutebanken.marduk.Constants.VALIDATION_PROFILE_HEADER;
import static no.rutebanken.marduk.Constants.VALIDATION_PROFILE_TIMETABLE;
import static no.rutebanken.marduk.Constants.VALIDATION_STAGE_HEADER;
import static no.rutebanken.marduk.Constants.VALIDATION_STAGE_EXPORT_NETEX_POSTVALIDATION;
import static no.rutebanken.marduk.Utils.getLastPathElementOfUrl;

@Component
public class ChouetteExportNetexRouteBuilder extends AbstractChouetteRouteBuilder {

    private final String chouetteUrl;
    private final int daysForward;
    private final int daysBack;
    private final boolean exportStops;
    private final boolean enablePostValidation;

    public ChouetteExportNetexRouteBuilder(
            @Value("${chouette.url}") String chouetteUrl,
            @Value("${chouette.enablePostValidation:true}") boolean enablePostValidation,
            @Value("${chouette.export.days.forward:365}") int daysForward,
            @Value("${chouette.export.days.back:365}") int daysBack,
            @Value("${chouette.netex.export.stops:false}") boolean exportStops) {
        this.chouetteUrl = chouetteUrl;
        this.daysForward = daysForward;
        this.daysBack = daysBack;
        this.exportStops = exportStops;
        this.enablePostValidation = enablePostValidation;
    }

    @Override
    public void configure() throws Exception {
        super.configure();

        from("entur-google-pubsub:ChouetteExportNetexQueue").streamCaching()
                .process(this::setCorrelationIdIfMissing)
                .removeHeader(Constants.CHOUETTE_JOB_ID)
                .log(LoggingLevel.INFO, getClass().getName(), correlation() + "Starting Chouette Netex export")
                .process(e -> JobEvent.providerJobBuilder(e).timetableAction(JobEvent.TimetableAction.EXPORT_NETEX).state(JobEvent.State.PENDING).build())
                .to("direct:updateStatus")

                .process(e -> e.getIn().setHeader(CHOUETTE_REFERENTIAL, getProviderRepository().getProvider(e.getIn().getHeader(PROVIDER_ID, Long.class)).chouetteInfo.referential))
                .process(e -> {
                    Provider provider = getProviderRepository().getProvider(e.getIn().getHeader(PROVIDER_ID, Long.class));
                    String defaultNetexExportParameters = Parameters.getDefaultNetexExportParameters(provider, exportStops, enablePostValidation);
                    e.getIn().setHeader(JSON_PART, defaultNetexExportParameters);
                })
                .log(LoggingLevel.DEBUG, correlation() + "Creating multipart request")
                .process(this::toGenericChouetteMultipart)
                .setHeader(Exchange.CONTENT_TYPE, simple("multipart/form-data"))
                .toD(chouetteUrl + "/chouette_iev/referentials/${header." + CHOUETTE_REFERENTIAL + "}/exporter/netexprofile")
                .process(e -> {
                    e.getIn().setHeader(CHOUETTE_JOB_STATUS_URL, e.getIn().getHeader("Location", String.class));
                    e.getIn().setHeader(Constants.CHOUETTE_JOB_ID, getLastPathElementOfUrl(e.getIn().getHeader("Location", String.class)));
                })
                .setHeader(Constants.CHOUETTE_JOB_STATUS_ROUTING_DESTINATION, constant("direct:processNetexExportResult"))
                .setHeader(Constants.CHOUETTE_JOB_STATUS_JOB_TYPE, constant(JobEvent.TimetableAction.EXPORT_NETEX.name()))
                .removeHeader("loopCounter")
                .setBody(constant(null))
                .to("entur-google-pubsub:ChouettePollStatusQueue")
                .routeId("chouette-start-export-netex");


        from("direct:processNetexExportResult")
                .choice()
                .when(simple("${header.action_report_result} == 'OK'"))
                .to("direct:processSuccessfulExport")
                .when(simple("${header.action_report_result} == 'NOK'"))
                .to("direct:processFailedExport")
                .otherwise()
                .log(LoggingLevel.ERROR, correlation() + "System error during Netex export")
                .process(e -> JobEvent.providerJobBuilder(e).timetableAction(JobEvent.TimetableAction.EXPORT_NETEX).state(JobEvent.State.FAILED).build())
                // end otherwise
                .end()
                // end choice
                .end()
                .to("direct:updateStatus")
                .removeHeader(Constants.CHOUETTE_JOB_ID)
                .routeId("chouette-process-export-netex-status");


        from("direct:processSuccessfulExport")
                .log(LoggingLevel.INFO, correlation() + "NeTEx export successful. Downloading export data")
                .log(LoggingLevel.DEBUG, correlation() + "Downloading NeTEx export data from ${header.data_url}")
                .process(this::removeAllCamelHeaders)
                .setBody(simple(""))
                .setHeader(Exchange.HTTP_METHOD, constant(org.apache.camel.component.http.HttpMethods.GET))
                .toD("${header.data_url}")
                .choice()
                .when(constant(enablePostValidation))
                .setHeader(BLOBSTORE_MAKE_BLOB_PUBLIC, simple("false", Boolean.class))
                .setHeader(FILE_HANDLE, simple(BLOBSTORE_PATH_NETEX_EXPORT + "${header." + CHOUETTE_REFERENTIAL + "}-" + Constants.CURRENT_AGGREGATED_NETEX_FILENAME))
                .to("direct:uploadBlob")
                .setBody(constant(null))
                .to("entur-google-pubsub:ChouetteMergeWithFlexibleLinesQueue")
                .to("entur-google-pubsub:ChouetteExportNetexBlocksQueue")
                .otherwise()
                .setHeader(BLOBSTORE_MAKE_BLOB_PUBLIC, simple("false", Boolean.class))
                .setHeader(FILE_HANDLE, simple(BLOBSTORE_PATH_NETEX_EXPORT_BEFORE_VALIDATION + "${header." + CHOUETTE_REFERENTIAL + "}-" + Constants.CURRENT_AGGREGATED_NETEX_FILENAME))
                .to("direct:uploadBlob")
                .setBody(constant(null))
                // end otherwise
                .end()
                // end choice
                .end()
                .to("direct:antuNetexPostValidation")
                .process(e -> JobEvent.providerJobBuilder(e).timetableAction(JobEvent.TimetableAction.EXPORT_NETEX).state(JobEvent.State.OK).build())
                .routeId("process-successful-export");

        from("direct:processFailedExport")
        .process(e -> {
                    ActionReportWrapper actionReportWrapper = e.getIn().getBody(ActionReportWrapper.class);
                    if (actionReportWrapper != null && actionReportWrapper.actionReport != null && actionReportWrapper.actionReport.failure != null) {
                        ActionReportWrapper.Failure failure = actionReportWrapper.actionReport.failure;
                        if (JobEvent.CHOUETTE_JOB_FAILURE_CODE_NO_DATA_PROCEEDED.equals(failure.code)) {
                            e.getIn().setHeader(Constants.JOB_ERROR_CODE, JobEvent.JOB_ERROR_NETEX_EXPORT_EMPTY);
                        }
                    }
                }
        )
                .log(LoggingLevel.WARN, correlation() + "Netex export failed with error code ${header." + Constants.JOB_ERROR_CODE + "}")
                .process(e -> JobEvent.providerJobBuilder(e).timetableAction(JobEvent.TimetableAction.EXPORT_NETEX).state(JobEvent.State.FAILED).build())
                .routeId("process-failed-export");

        from("direct:antuNetexPostValidation")
                .process(e -> {
                    Provider provider = getProviderRepository().getProvider(e.getIn().getHeader(PROVIDER_ID, Long.class));
                    e.getIn().setHeader(DATASET_REFERENTIAL, provider.chouetteInfo.referential);
                })
                .setHeader(VALIDATION_STAGE_HEADER, constant(VALIDATION_STAGE_EXPORT_NETEX_POSTVALIDATION))
                .setHeader(VALIDATION_CLIENT_HEADER, constant(VALIDATION_CLIENT_MARDUK))
                .setHeader(VALIDATION_PROFILE_HEADER, constant(VALIDATION_PROFILE_TIMETABLE))
                .setHeader(VALIDATION_DATASET_FILE_HANDLE_HEADER, header(FILE_HANDLE))
                .setHeader(VALIDATION_CORRELATION_ID_HEADER, header(CORRELATION_ID))
                .to("direct:setNetexValidationProfile")
                .to("entur-google-pubsub:AntuNetexValidationQueue")
                .process(e -> JobEvent.providerJobBuilder(e)
                        .timetableAction(JobEvent.TimetableAction.EXPORT_NETEX_POSTVALIDATION)
                        .state(JobEvent.State.PENDING)
                        .jobId(null)
                        .build())
                .to("direct:updateStatus")
                .routeId("antu-netex-post-validation");
    }


}
