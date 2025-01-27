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

import java.util.List;
import java.util.Locale;

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
import static no.rutebanken.marduk.Constants.VALIDATION_STAGE_EXPORT_NETEX_BLOCKS_POSTVALIDATION;
import static no.rutebanken.marduk.Constants.VALIDATION_STAGE_HEADER;
import static no.rutebanken.marduk.Utils.getLastPathElementOfUrl;

@Component
public class ChouetteExportNetexBlocksRouteBuilder extends AbstractChouetteRouteBuilder {

    private static final String PROP_EXPORT_BLOCKS = "exportBlocks";

    private final String chouetteUrl;
    private final boolean enablePostValidation;
    private final List<String> allowedCodespacesForStopExport;

    public ChouetteExportNetexBlocksRouteBuilder(
            @Value("${chouette.url}") String chouetteUrl,
            @Value("${chouette.enablePostValidation:false}") boolean enablePostValidation,
            @Value("${chouette.include.stops.codespaces:}") List<String> allowedCodespacesForStopExport
            ) {
        this.chouetteUrl = chouetteUrl;
        this.enablePostValidation = enablePostValidation;
        this.allowedCodespacesForStopExport = allowedCodespacesForStopExport;
    }

    @Override
    public void configure() throws Exception {
        super.configure();

        from("google-pubsub:{{marduk.pubsub.project.id}}:ChouetteExportNetexBlocksQueue").streamCaching()
                .process(this::setCorrelationIdIfMissing)
                .removeHeader(Constants.CHOUETTE_JOB_ID)
                .process(e -> e.setProperty(PROP_EXPORT_BLOCKS, getProviderRepository().getProvider(e.getIn().getHeader(PROVIDER_ID, Long.class)).getChouetteInfo().isEnableBlocksExport()))
                .choice()
                .when(simple("${exchangeProperty." + PROP_EXPORT_BLOCKS + "} != 'true'"))
                .log(LoggingLevel.INFO, getClass().getName(), correlation() + "Skipping Chouette Netex Blocks export")
                .otherwise()
                .log(LoggingLevel.INFO, getClass().getName(), correlation() + "Starting Chouette Netex Blocks export")
                .process(e -> JobEvent.providerJobBuilder(e).timetableAction(JobEvent.TimetableAction.EXPORT_NETEX_BLOCKS).state(JobEvent.State.PENDING).build())
                .to("direct:updateStatus")

                .process(e -> e.getIn().setHeader(CHOUETTE_REFERENTIAL, getProviderRepository().getProvider(e.getIn().getHeader(PROVIDER_ID, Long.class)).getChouetteInfo().getReferential()))
                .process(e ->  {
                    Provider provider = getProviderRepository().getProvider(e.getIn().getHeader(PROVIDER_ID, Long.class));
                    String codespace = e.getIn().getHeader(CHOUETTE_REFERENTIAL, String.class).replace("rb_", "").toUpperCase(Locale.ROOT);
                    String netexBlocksExportParameters = Parameters.getNetexBlocksExportParameters(provider, isAllowedCodespacesForStopExport(codespace), enablePostValidation);
                    e.getIn().setHeader(JSON_PART, netexBlocksExportParameters);
                })
                .log(LoggingLevel.DEBUG, correlation() + "Creating multipart request")
                .process(this::toGenericChouetteMultipart)
                .setHeader(Exchange.CONTENT_TYPE, simple("multipart/form-data"))
                .toD(chouetteUrl + "/chouette_iev/referentials/${header." + CHOUETTE_REFERENTIAL + "}/exporter/netexprofile")
                .process(e -> {
                    e.getIn().setHeader(CHOUETTE_JOB_STATUS_URL, e.getIn().getHeader("Location", String.class));
                    e.getIn().setHeader(Constants.CHOUETTE_JOB_ID, getLastPathElementOfUrl(e.getIn().getHeader("Location", String.class)));
                })
                .setHeader(Constants.CHOUETTE_JOB_STATUS_ROUTING_DESTINATION, constant("direct:processNetexBlocksExportResult"))
                .setHeader(Constants.CHOUETTE_JOB_STATUS_JOB_TYPE, constant(JobEvent.TimetableAction.EXPORT_NETEX_BLOCKS.name()))
                .removeHeader("loopCounter")
                .setBody(constant(""))
                .to("google-pubsub:{{marduk.pubsub.project.id}}:ChouettePollStatusQueue")
                .endChoice()
                .routeId("chouette-start-export-netex-block");


        from("direct:processNetexBlocksExportResult")
                .choice()
                .when(simple("${header.action_report_result} == 'OK'"))
                .to("direct:processSuccessFulBlockExport")
                .when(simple("${header.action_report_result} == 'NOK'"))
                .to("direct:processFailedBlockExport")
                .otherwise()
                .log(LoggingLevel.ERROR, correlation() + "Something went wrong on Netex blocks export")
                .process(e -> JobEvent.providerJobBuilder(e).timetableAction(JobEvent.TimetableAction.EXPORT_NETEX_BLOCKS).state(JobEvent.State.FAILED).build())
                .end()
                .to("direct:updateStatus")
                .removeHeader(Constants.CHOUETTE_JOB_ID)
                .routeId("chouette-process-export-netex-block-status");

        from("direct:processSuccessFulBlockExport")
                .log(LoggingLevel.INFO, correlation() + "NeTEx Blocks export successful. Downloading export data")
                .log(LoggingLevel.DEBUG, correlation() + "Downloading NeTEx Blocks export data from ${header.data_url}")
                .process(this::removeAllCamelHeaders)
                .setBody(simple(""))
                .setHeader(Exchange.HTTP_METHOD, constant(org.apache.camel.component.http.HttpMethods.GET))
                .toD("${header.data_url}")
                .choice()
                .when(constant(enablePostValidation))
                .setHeader(FILE_HANDLE, simple(Constants.BLOBSTORE_PATH_NETEX_BLOCKS_EXPORT + "${header." + CHOUETTE_REFERENTIAL + "}-" + Constants.CURRENT_AGGREGATED_NETEX_FILENAME))
                .to("direct:uploadInternalBlob")
                .setBody(constant(""))
                .otherwise()
                .setHeader(FILE_HANDLE, simple(Constants.BLOBSTORE_PATH_NETEX_BLOCKS_EXPORT_BEFORE_VALIDATION + "${header." + CHOUETTE_REFERENTIAL + "}-" + Constants.CURRENT_AGGREGATED_NETEX_FILENAME))
                .to("direct:uploadInternalBlob")
                .setBody(constant(""))
                // end otherwise
                .end()
                // end choice
                .end()
                .to("direct:antuNetexBlocksPostValidation")
                .process(e -> JobEvent.providerJobBuilder(e).timetableAction(JobEvent.TimetableAction.EXPORT_NETEX_BLOCKS).state(JobEvent.State.OK).build())
                .routeId("process-successful-block-export");


        from("direct:processFailedBlockExport")
                .process(e -> {
                            ActionReportWrapper actionReportWrapper = e.getIn().getBody(ActionReportWrapper.class);
                            if (actionReportWrapper != null && actionReportWrapper.getActionReport() != null && actionReportWrapper.getActionReport().getFailure() != null) {
                                ActionReportWrapper.Failure failure = actionReportWrapper.getActionReport().getFailure();
                                if (JobEvent.CHOUETTE_JOB_FAILURE_CODE_NO_DATA_PROCEEDED.equals(failure.getCode())) {
                                    e.getIn().setHeader(Constants.JOB_ERROR_CODE, JobEvent.JOB_ERROR_NETEX_EXPORT_EMPTY);
                                }
                            }
                        }
                )
                .log(LoggingLevel.WARN, correlation() + "Netex blocks export failed with error code ${header." + Constants.JOB_ERROR_CODE + "}")
                .process(e -> JobEvent.providerJobBuilder(e).timetableAction(JobEvent.TimetableAction.EXPORT_NETEX_BLOCKS).state(JobEvent.State.FAILED).build())
                .routeId("process-failed-block-export");

        from("direct:antuNetexBlocksPostValidation")
                .to("direct:copyInternalBlobToValidationBucket")
                .process(e -> {
                    Provider provider = getProviderRepository().getProvider(e.getIn().getHeader(PROVIDER_ID, Long.class));
                    e.getIn().setHeader(DATASET_REFERENTIAL, provider.getChouetteInfo().getReferential());
                })
                .setHeader(VALIDATION_STAGE_HEADER, constant(VALIDATION_STAGE_EXPORT_NETEX_BLOCKS_POSTVALIDATION))
                .setHeader(VALIDATION_CLIENT_HEADER, constant(VALIDATION_CLIENT_MARDUK))
                .setHeader(VALIDATION_PROFILE_HEADER, constant(VALIDATION_PROFILE_TIMETABLE))
                .setHeader(VALIDATION_DATASET_FILE_HANDLE_HEADER, header(FILE_HANDLE))
                .setHeader(VALIDATION_CORRELATION_ID_HEADER, header(CORRELATION_ID))
                .to("direct:setNetexValidationProfile")
                .to("google-pubsub:{{antu.pubsub.project.id}}:AntuNetexValidationQueue")
                .process(e -> JobEvent.providerJobBuilder(e)
                        .timetableAction(JobEvent.TimetableAction.EXPORT_NETEX_BLOCKS_POSTVALIDATION)
                        .state(JobEvent.State.PENDING)
                        .jobId(null)
                        .build())
                .to("direct:updateStatus")
                .routeId("antu-netex-blocks-post-validation");

    }

    private boolean isAllowedCodespacesForStopExport(String codespace) {
        return allowedCodespacesForStopExport.contains(codespace);
    }

}
