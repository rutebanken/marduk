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
import no.rutebanken.marduk.routes.file.FileType;
import no.rutebanken.marduk.routes.status.JobEvent;
import no.rutebanken.marduk.routes.status.JobEvent.State;
import no.rutebanken.marduk.routes.status.JobEvent.TimetableAction;
import org.apache.camel.Exchange;
import org.apache.camel.LoggingLevel;
import org.apache.camel.component.http.HttpMethods;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Locale;

import static no.rutebanken.marduk.Constants.CHOUETTE_JOB_STATUS_JOB_VALIDATION_LEVEL;
import static no.rutebanken.marduk.Constants.CHOUETTE_REFERENTIAL;
import static no.rutebanken.marduk.Constants.FILE_NAME;
import static no.rutebanken.marduk.Constants.FILE_TYPE;
import static no.rutebanken.marduk.Constants.JSON_PART;
import static no.rutebanken.marduk.Constants.PROVIDER_ID;
import static no.rutebanken.marduk.Constants.TARGET_CONTAINER;
import static no.rutebanken.marduk.Constants.TARGET_FILE_HANDLE;
import static no.rutebanken.marduk.Utils.getLastPathElementOfUrl;

/**
 * Submits files to Chouette
 */
@Component
public class ChouetteImportRouteBuilder extends AbstractChouetteRouteBuilder {

    private static final String DATASET_IMPORT_KEY = "DATASET_IMPORT_KEY";

    private final String chouetteUrl;
    private final String nisabaExchangeContainerName;
    private final boolean enablePreValidation;
    private final List<String> allowedCodespacesForStopUpdate;

    public ChouetteImportRouteBuilder(@Value("${chouette.url}") String chouetteUrl,
                                      @Value("${chouette.enablePreValidation:true}") boolean enablePreValidation,
                                      @Value("${chouette.include.stops.codespaces:}") List<String> allowedCodespacesForStopUpdate,
                                      @Value("${blobstore.gcs.nisaba.exchange.container.name}") String nisabaExchangeContainerName) {
        this.chouetteUrl = chouetteUrl;
        this.enablePreValidation = enablePreValidation;
        this.nisabaExchangeContainerName = nisabaExchangeContainerName;
        this.allowedCodespacesForStopUpdate= allowedCodespacesForStopUpdate;
    }

    @Override
    public void configure() throws Exception {
        super.configure();

        from("direct:chouetteCleanStopPlaces")
                .log(LoggingLevel.INFO, correlation() + "Starting Chouette stop place clean")
                .process(this::removeAllCamelHeaders)
                .setBody(constant(""))
                .setHeader(Exchange.HTTP_METHOD, constant(HttpMethods.POST))
                .setProperty("chouette_url", simple(chouetteUrl + "/chouette_iev/referentials/clean/stop_areas"))
                .toD("${exchangeProperty.chouette_url}")
                .routeId("chouette-clean-stop-places");

        from("direct:chouetteCleanAllReferentials")
                .process(e -> e.getIn().setBody(getProviderRepository().getProviders()))
                .split().body().parallelProcessing().executorService("allProvidersExecutorService")
                .process(this::removeAllCamelHeaders)
                .setHeader(Constants.PROVIDER_ID, simple("${body.id}"))
                .validate(header("filter").in("all", "level1", "level2"))
                .choice()
                .when(header("filter").isEqualTo("level1"))
                .filter(simple("${body.chouetteInfo.migrateDataToProvider} != null"))
                .setBody(constant(""))
                .to("direct:chouetteCleanReferential")
                .endChoice()
                .when(header("filter").isEqualTo("level2"))
                .filter(simple("${body.chouetteInfo.migrateDataToProvider} == null"))
                .setBody(constant(""))
                .to("direct:chouetteCleanReferential")
                .endChoice()
                .otherwise()
                .setBody(constant(""))
                .to("direct:chouetteCleanReferential")
                .end()
                .routeId("chouette-clean-referentials-for-all-providers");

        from("direct:chouetteCleanReferential")
                .log(LoggingLevel.INFO, correlation() + "Starting Chouette dataspace clean")
                .process(e -> {
                    Provider provider = getProviderRepository().getProvider(e.getIn().getHeader(PROVIDER_ID, Long.class));
                    e.getIn().setHeader(CHOUETTE_REFERENTIAL, provider.getChouetteInfo().getReferential());
                })
                .process(this::removeAllCamelHeaders)
                .setHeader(Exchange.HTTP_METHOD, constant(HttpMethods.POST))
                .setProperty("chouette_url", simple(chouetteUrl + "/chouette_iev/referentials/${header." + CHOUETTE_REFERENTIAL + "}/clean"))
                .toD("${exchangeProperty.chouette_url}")
                .routeId("chouette-clean-dataspace");

        from("google-pubsub:{{marduk.pubsub.project.id}}:ChouetteImportQueue")
                .streamCache("true")
                .log(LoggingLevel.INFO, correlation() + "Starting Chouette import")
                .removeHeader(Constants.CHOUETTE_JOB_ID)
                .process(e -> JobEvent.providerJobBuilder(e).timetableAction(JobEvent.TimetableAction.IMPORT).state(State.PENDING).build())
                .to("direct:updateStatus")
                .to("direct:getInternalBlob")
                .choice()
                .when(body().isNull())
                .log(LoggingLevel.WARN, correlation() + "Import failed because blob could not be found")
                .process(e -> JobEvent.providerJobBuilder(e).timetableAction(TimetableAction.IMPORT).state(State.FAILED).build())
                .otherwise()
                .process(e -> {
                    Provider provider = getProviderRepository().getProvider(e.getIn().getHeader(PROVIDER_ID, Long.class));
                    e.getIn().setHeader(CHOUETTE_REFERENTIAL, provider.getChouetteInfo().getReferential());
                })
                .to(logDebugShowAll())
                .to("direct:addImportParameters")
                .end()
                .routeId("chouette-import-dataspace");

        from("direct:addImportParameters")
                .process(e -> {
                    String fileName = e.getIn().getHeader(FILE_NAME, String.class);
                    String fileType = e.getIn().getHeader(FILE_TYPE, String.class);
                    Long providerId = e.getIn().getHeader(PROVIDER_ID, Long.class);
                    String codespace = e.getIn().getHeader(CHOUETTE_REFERENTIAL, String.class).toUpperCase(Locale.ROOT);
                    // always activate pre-validation for GTFS files as this is not supported in Antu
                    String importParameters = getImportParameters(fileName, fileType, providerId, FileType.GTFS.name().equals(fileType) || enablePreValidation, isAllowedCodespaceForStopUpdate(codespace));
                    e.getIn().setHeader(JSON_PART, importParameters);
                })
                .log(LoggingLevel.DEBUG, correlation() + "import parameters: " + header(JSON_PART))
                .to("direct:sendImportJobRequest")
                .routeId("chouette-import-addToExchange-parameters");

        from("direct:sendImportJobRequest")
                // remove the error handler so that the whole route is retried in case of error
                // to ensure that multipart input streams are reset before retrying the web service call to chouette.
                .errorHandler(noErrorHandler())
                .log(LoggingLevel.DEBUG, correlation() + "Creating multipart request")
                .process(this::toImportMultipart)
                .setHeader(Exchange.CONTENT_TYPE, simple("multipart/form-data"))
                .to(logDebugShowAll())
                .setProperty("chouette_url", simple(chouetteUrl + "/chouette_iev/referentials/${header." + CHOUETTE_REFERENTIAL + "}/importer/${header." + FILE_TYPE + ".toLowerCase()}"))
                .log(LoggingLevel.DEBUG, correlation() + "Calling Chouette with URL: ${exchangeProperty.chouette_url}")
                .setHeader(Exchange.HTTP_METHOD, constant(HttpMethods.POST))
                // Attempt to retrigger delivery in case of errors
                .toD("${exchangeProperty.chouette_url}")
                .to(logDebugShowAll())
                .process(e -> {
                    e.getIn().setHeader(Constants.CHOUETTE_JOB_STATUS_URL, e.getIn().getHeader("Location", String.class));
                    e.getIn().setHeader(Constants.CHOUETTE_JOB_ID, getLastPathElementOfUrl(e.getIn().getHeader("Location", String.class)));
                })
                .setHeader(Constants.CHOUETTE_JOB_STATUS_ROUTING_DESTINATION, constant("direct:processImportResult"))
                .setHeader(Constants.CHOUETTE_JOB_STATUS_JOB_TYPE, constant(JobEvent.TimetableAction.IMPORT.name()))
                .removeHeader("loopCounter")
                .setBody(constant(""))
                .to("google-pubsub:{{marduk.pubsub.project.id}}:ChouettePollStatusQueue")
                .routeId("chouette-send-import-job");


        // Start common


        from("direct:processImportResult")
                .to(logDebugShowAll())
                .setBody(constant(""))
                .choice()
                .when(simple("${header.action_report_result} == 'OK' && ${header.validation_report_result} == 'OK'"))
                .to("direct:copyOriginalDataset")
                .to("direct:checkScheduledJobsBeforeTriggeringNextAction")
                .process(e -> JobEvent.providerJobBuilder(e).timetableAction(JobEvent.TimetableAction.IMPORT).state(State.OK).build())
                .when(simple("${header.action_report_result} == 'OK' && ${header.validation_report_result} == 'NOK'"))
                .log(LoggingLevel.INFO, correlation() + "Import ok but validation failed")
                .process(e -> JobEvent.providerJobBuilder(e).timetableAction(TimetableAction.IMPORT).state(State.FAILED).build())
                .when(simple("${header.action_report_result} == 'NOK'"))
                .choice()
                .when(simple("${header.action_report_result} == 'NOK'"))
                .log(LoggingLevel.WARN, correlation() + "Import not ok")
                .process(e -> JobEvent.providerJobBuilder(e).timetableAction(JobEvent.TimetableAction.IMPORT).state(State.FAILED).build())
                .endChoice()
                .otherwise()
                .log(LoggingLevel.ERROR, correlation() + "Something went wrong on import")
                .process(e -> JobEvent.providerJobBuilder(e).timetableAction(TimetableAction.IMPORT).state(State.FAILED).build())
                .end()
                .to("direct:updateStatus")
                .routeId("chouette-process-import-status");

        // Check that no other import jobs in status SCHEDULED exists for this referential. If so, do not trigger export
        from("direct:checkScheduledJobsBeforeTriggeringNextAction")
                .setProperty("job_status_url", simple("{{chouette.url}}/chouette_iev/referentials/${header." + CHOUETTE_REFERENTIAL + "}/jobs?timetableAction=importer&status=SCHEDULED&status=STARTED"))
                .toD("${exchangeProperty.job_status_url}")
                .choice()
                .when().jsonpath("$.*[?(@.status == 'SCHEDULED')].status")
                .log(LoggingLevel.INFO, correlation() + "Import ok, skipping next step as there are more import jobs active")
                .otherwise()
                .setBody(constant(""))
                .log(LoggingLevel.INFO, correlation() + "Import ok, triggering validation")
                .setHeader(CHOUETTE_JOB_STATUS_JOB_VALIDATION_LEVEL, constant(JobEvent.TimetableAction.VALIDATION_LEVEL_1.name()))
                .to("google-pubsub:{{marduk.pubsub.project.id}}:ChouetteValidationQueue")
                .end()
                .routeId("chouette-process-job-list-after-import");

        // copy the original NeTEx archive and publish it on an exchange bucket
        from("direct:copyOriginalDataset")
                .setProperty("chouette_url", simple(chouetteUrl + "/chouette_iev/referentials/${header." + CHOUETTE_REFERENTIAL + "}/last_update_date"))
                .toD("${exchangeProperty.chouette_url}")
                .convertBodyTo(String.class)
                .setHeader(DATASET_IMPORT_KEY, simple("${header." + CHOUETTE_REFERENTIAL + "}_${body.replace(':','_')}"))
                .setHeader(TARGET_FILE_HANDLE, simple("imported/${header." + CHOUETTE_REFERENTIAL + "}/${header." +  DATASET_IMPORT_KEY + "}.zip"))
                .setHeader(TARGET_CONTAINER, constant(nisabaExchangeContainerName))
                .to("direct:copyVersionedInternalBlobToAnotherBucket")
                .routeId("chouette-copy-original-dataset");

    }

    private boolean isAllowedCodespaceForStopUpdate(String codespace) {
        return allowedCodespacesForStopUpdate.contains(codespace);
    }

    private String getImportParameters(String fileName, String fileType, Long providerId, boolean enablePreValidation, boolean allowUpdatingStopPlace) {
        Provider provider = getProviderRepository().getProvider(providerId);
        return Parameters.createImportParameters(fileName, fileType, provider, enablePreValidation, allowUpdatingStopPlace);
    }

}


