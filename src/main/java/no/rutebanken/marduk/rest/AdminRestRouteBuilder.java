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

package no.rutebanken.marduk.rest;

import no.rutebanken.marduk.Constants;
import no.rutebanken.marduk.domain.BlobStoreFiles;
import no.rutebanken.marduk.domain.BlobStoreFiles.File;
import no.rutebanken.marduk.routes.BaseRouteBuilder;
import no.rutebanken.marduk.routes.chouette.json.JobResponse;
import no.rutebanken.marduk.routes.chouette.json.Status;
import no.rutebanken.marduk.routes.status.JobEvent;
import no.rutebanken.marduk.security.AuthorizationClaim;
import no.rutebanken.marduk.security.AuthorizationService;
import org.apache.camel.Body;
import org.apache.camel.Exchange;
import org.apache.camel.ExchangePattern;
import org.apache.camel.LoggingLevel;
import org.apache.camel.model.rest.RestBindingMode;
import org.apache.camel.model.rest.RestParamType;
import org.rutebanken.helper.organisation.AuthorizationConstants;
import org.rutebanken.helper.organisation.NotAuthenticatedException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.stereotype.Component;

import javax.ws.rs.NotFoundException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static javax.ws.rs.core.MediaType.MULTIPART_FORM_DATA;
import static no.rutebanken.marduk.Constants.CHOUETTE_JOB_STATUS_JOB_VALIDATION_LEVEL;
import static no.rutebanken.marduk.Constants.CHOUETTE_REFERENTIAL;
import static no.rutebanken.marduk.Constants.CORRELATION_ID;
import static no.rutebanken.marduk.Constants.FILE_HANDLE;
import static no.rutebanken.marduk.Constants.PROVIDER_ID;
import static no.rutebanken.marduk.Constants.PROVIDER_IDS;

/**
 * REST interface for backdoor triggering of messages
 */
@Component
public class AdminRestRouteBuilder extends BaseRouteBuilder {


    private static final String JSON = "application/json";
    private static final String X_OCTET_STREAM = "application/x-octet-stream";
    private static final String PLAIN = "text/plain";
    private static final String SWAGGER_DATA_TYPE_STRING = "string";
    private static final String SWAGGER_DATA_TYPE_INTEGER = "integer";

    @Autowired
    private AuthorizationService authorizationService;

    @Override
    public void configure() throws Exception {
        super.configure();

        onException(AccessDeniedException.class)
                .handled(true)
                .setHeader(Exchange.HTTP_RESPONSE_CODE, constant(403))
                .setHeader(Exchange.CONTENT_TYPE, constant(PLAIN))
                .transform(exceptionMessage());

        onException(NotAuthenticatedException.class)
                .handled(true)
                .setHeader(Exchange.HTTP_RESPONSE_CODE, constant(401))
                .setHeader(Exchange.CONTENT_TYPE, constant(PLAIN))
                .transform(exceptionMessage());

        onException(NotFoundException.class)
                .handled(true)
                .setHeader(Exchange.HTTP_RESPONSE_CODE, constant(404))
                .setHeader(Exchange.CONTENT_TYPE, constant(PLAIN))
                .transform(exceptionMessage());


        restConfiguration()
                .component("servlet")
                .contextPath("/services")
                .bindingMode(RestBindingMode.json)
                .endpointProperty("matchOnUriPrefix", "true")
                .apiContextPath("/swagger.json")
                .apiProperty("api.title", "Marduk Admin API").apiProperty("api.version", "1.0")
                .apiContextRouteId("doc-api")
        ;

        rest("")
                .apiDocs(false)
                .description("Wildcard definitions necessary to get Jetty to match authorization filters to endpoints with path params")
                .get().route().routeId("admin-route-authorize-get").throwException(new NotFoundException()).endRest()
                .post().route().routeId("admin-route-authorize-post").throwException(new NotFoundException()).endRest()
                .put().route().routeId("admin-route-authorize-put").throwException(new NotFoundException()).endRest()
                .delete().route().routeId("admin-route-authorize-delete").throwException(new NotFoundException()).endRest();

    rest("/timetable_admin")
                .post("/idempotentfilter/clean")
                .description("Clean unique filename and digest Idempotent Stores")
                .responseMessage().code(200).endResponseMessage()
                .responseMessage().code(500).message("Internal error").endResponseMessage()
                .route().routeId("admin-application-clean-unique-filename-and-digest-idempotent-repos")
                .process(e -> authorizationService.verifyAtLeastOne(new AuthorizationClaim(AuthorizationConstants.ROLE_ROUTE_DATA_ADMIN)))
                .to("direct:cleanIdempotentFileStore")
                .setBody(constant(null))
                .endRest()

                .post("/validate/level1")
                .description("Triggers the validate->transfer process for all level1 providers in Chouette")
                .consumes(PLAIN)
                .produces(PLAIN)
                .responseMessage().code(200).message("Command accepted").endResponseMessage()
                .route()
                .to("direct:authorizeRequest")
                .process(e -> authorizationService.verifyAtLeastOne(new AuthorizationClaim(AuthorizationConstants.ROLE_ROUTE_DATA_ADMIN)))
                .log(LoggingLevel.INFO, correlation() + "Chouette start validation level1 for all providers")
                .removeHeaders(Constants.CAMEL_ALL_HTTP_HEADERS)
                .to(ExchangePattern.InOnly, "direct:chouetteValidateLevel1ForAllProviders")
                .setBody(constant(null))
                .routeId("admin-chouette-validate-level1-all-providers")
                .endRest()

                .post("/validate/level2")
                .description("Triggers the validate->export process for all level2 providers in Chouette")
                .consumes(PLAIN)
                .produces(PLAIN)
                .responseMessage().code(200).message("Command accepted").endResponseMessage()
                .route()
                .to("direct:authorizeRequest")
                .process(e -> authorizationService.verifyAtLeastOne(new AuthorizationClaim(AuthorizationConstants.ROLE_ROUTE_DATA_ADMIN)))
                .log(LoggingLevel.INFO, correlation() + "Chouette start validation level2 for all providers")
                .removeHeaders(Constants.CAMEL_ALL_HTTP_HEADERS)
                .to(ExchangePattern.InOnly, "direct:chouetteValidateLevel2ForAllProviders")
                .setBody(constant(null))
                .routeId("admin-chouette-validate-level2-all-providers")
                .endRest()

                .get("/jobs")
                .description("List Chouette jobs for all providers. Filters defaults to status=SCHEDULED,STARTED")
                .param()
                .required(Boolean.FALSE)
                .name("status")
                .type(RestParamType.query)
                .description("Chouette job statuses")
                .allowableValues(Arrays.stream(Status.values()).map(Status::name).collect(Collectors.toList()))
                .endParam()
                .param()
                .required(Boolean.FALSE)
                .name("action")
                .type(RestParamType.query)
                .description("Chouette job types")
                .allowableValues("importer", "exporter", "validator")
                .endParam()
                .outType(ProviderAndJobs[].class)
                .consumes(PLAIN)
                .produces(JSON)
                .responseMessage().code(200).endResponseMessage()
                .responseMessage().code(500).message("Internal error").endResponseMessage()
                .route()
                .process(e -> authorizationService.verifyAtLeastOne(new AuthorizationClaim(AuthorizationConstants.ROLE_ROUTE_DATA_ADMIN)))
                .log(LoggingLevel.DEBUG, correlation() + "Get chouette active jobs all providers")
                .removeHeaders(Constants.CAMEL_ALL_HTTP_HEADERS)
                .process(e -> e.getIn().setHeader("status", e.getIn().getHeader("status") != null ? e.getIn().getHeader("status") : Arrays.asList("STARTED", "SCHEDULED")))
                .to("direct:chouetteGetJobsAll")
                .routeId("admin-chouette-list-jobs-all")
                .endRest()

                .delete("/jobs")
                .description("Cancel all Chouette jobs for all providers")
                .responseMessage().code(200).message("All jobs canceled").endResponseMessage()
                .responseMessage().code(500).message("Could not cancel all jobs").endResponseMessage()
                .route()
                .process(e -> authorizationService.verifyAtLeastOne(new AuthorizationClaim(AuthorizationConstants.ROLE_ROUTE_DATA_ADMIN)))
                .log(LoggingLevel.INFO, correlation() + "Cancel all chouette jobs for all providers")
                .removeHeaders(Constants.CAMEL_ALL_HTTP_HEADERS)
                .to("direct:chouetteCancelAllJobsForAllProviders")
                .routeId("admin-chouette-cancel-all-jobs-all")
                .setBody(constant(null))
                .endRest()

                .delete("/completed_jobs")
                .description("Remove completed Chouette jobs for all providers. ")
                .param()
                .required(Boolean.FALSE)
                .name("keepJobs")
                .type(RestParamType.query)
                .dataType(SWAGGER_DATA_TYPE_INTEGER)
                .description("No of jobs to keep, regardless of age")
                .endParam()
                .param()
                .required(Boolean.FALSE)
                .name("keepDays")
                .type(RestParamType.query)
                .dataType(SWAGGER_DATA_TYPE_INTEGER)
                .description("No of days to keep jobs for")
                .endParam()
                .responseMessage().code(200).message("Completed jobs removed").endResponseMessage()
                .responseMessage().code(500).message("Could not remove complete jobs").endResponseMessage()
                .route()
                .process(e -> authorizationService.verifyAtLeastOne(new AuthorizationClaim(AuthorizationConstants.ROLE_ROUTE_DATA_ADMIN)))
                .log(LoggingLevel.INFO, correlation() + "Removing old chouette jobs for all providers")
                .removeHeaders(Constants.CAMEL_ALL_HTTP_HEADERS)
                .to("direct:chouetteRemoveOldJobs")
                .routeId("admin-chouette-remove-old-jobs")
                .setBody(constant(null))
                .endRest()


                .post("/clean/{filter}")
                .description("Triggers the clean ALL dataspace process in Chouette. Only timetable data are deleted, not job data (imports, exports, validations) or stop places")
                .param()
                .required(Boolean.TRUE)
                .name("filter")
                .type(RestParamType.path)
                .description("Optional filter to clean only level 1, level 2 or all spaces (no parameter value)")
                .allowableValues("all", "level1", "level2")

                .endParam()
                .consumes(PLAIN)
                .produces(PLAIN)
                .responseMessage().code(200).message("Command accepted").endResponseMessage()
                .responseMessage().code(500).message("Internal error - check filter").endResponseMessage()
                .route()
                .process(e -> authorizationService.verifyAtLeastOne(new AuthorizationClaim(AuthorizationConstants.ROLE_ROUTE_DATA_ADMIN)))
                .log(LoggingLevel.INFO, correlation() + "Chouette clean all dataspaces")
                .removeHeaders(Constants.CAMEL_ALL_HTTP_HEADERS)
                .to("direct:chouetteCleanAllReferentials")
                .setBody(constant(null))
                .routeId("admin-chouette-clean-all")
                .endRest()

                .post("/stop_places/clean")
                .description("Triggers the cleaning of ALL stop places in Chouette")
                .consumes(PLAIN)
                .produces(PLAIN)
                .responseMessage().code(200).message("Command accepted").endResponseMessage()
                .responseMessage().code(500).message("Internal error - check filter").endResponseMessage()
                .route()
                .process(e -> authorizationService.verifyAtLeastOne(new AuthorizationClaim(AuthorizationConstants.ROLE_ROUTE_DATA_ADMIN)))
                .log(LoggingLevel.INFO, correlation() + "Chouette clean all stop places")
                .removeHeaders(Constants.CAMEL_ALL_HTTP_HEADERS)
                .to("direct:chouetteCleanStopPlaces")
                .setBody(constant(null))
                .routeId("admin-chouette-clean-stop-places")
                .endRest()

                .get("/line_statistics/{filter}")
                .description("List stats about data in chouette for multiple providers")
                .param().name("providerIds")
                .type(RestParamType.query).dataType(SWAGGER_DATA_TYPE_INTEGER)
                .required(Boolean.FALSE)
                .description("Comma separated list of id for providers to fetch line stats for")
                .endParam()
                .param()
                .name("filter")
                .required(Boolean.TRUE)
                .type(RestParamType.path)
                .description("Filter to fetch statistics for only level 1, level 2 or all spaces")
                .allowableValues("all", "level1", "level2")
                .endParam()
                .bindingMode(RestBindingMode.off)
                .consumes(PLAIN)
                .produces(JSON)
                .responseMessage().code(200).endResponseMessage()
                .responseMessage().code(500).message("Internal error").endResponseMessage()
                .route()
                .to("direct:authorizeRequest")
                .log(LoggingLevel.INFO, correlation() + "get stats for multiple providers")
                .removeHeaders(Constants.CAMEL_ALL_HTTP_HEADERS)
                .choice()
                .when(simple("${header.providerIds}"))
                .process(e -> e.getIn().setHeader(PROVIDER_IDS, e.getIn().getHeader("providerIds", "", String.class).split(",")))
                .end()
                .to("direct:chouetteGetStats")
                .routeId("admin-chouette-stats-multiple-providers")
                .endRest()

                .post("/line_statistics/refresh")
                .description("Recalculate stats about data in chouette for all providers")
                .bindingMode(RestBindingMode.off)
                .consumes(PLAIN)
                .produces(PLAIN)
                .responseMessage().code(200).endResponseMessage()
                .responseMessage().code(500).message("Internal error").endResponseMessage()
                .route()
                .to("direct:authorizeRequest")
                .log(LoggingLevel.INFO, correlation() + "refresh stats cache")
                .removeHeaders(Constants.CAMEL_ALL_HTTP_HEADERS)
                .to("direct:chouetteRefreshStatsCache")
                .routeId("admin-chouette-stats-refresh-cache")
                .endRest()

                .get("/export/files")
                .description("List files containing exported time table data and graphs")
                .outType(BlobStoreFiles.class)
                .consumes(PLAIN)
                .produces(JSON)
                .responseMessage().code(200).endResponseMessage()
                .responseMessage().code(500).message("Internal error").endResponseMessage()
                .route()
                .to("direct:authorizeRequest")
                .log(LoggingLevel.INFO, correlation() + "get time table and graph files")
                .removeHeaders(Constants.CAMEL_ALL_HTTP_HEADERS)
                .to("direct:listTimetableExportAndGraphBlobs")
                .routeId("admin-chouette-timetable-files-get")
                .endRest()


                .post("/export/gtfs/extended")
                .description("Prepare and upload GTFS extened export")
                .consumes(PLAIN)
                .produces(PLAIN)
                .responseMessage().code(200).endResponseMessage()
                .responseMessage().code(500).message("Internal error").endResponseMessage()
                .route()
                .to("direct:authorizeRequest")
                .log(LoggingLevel.INFO, "Triggered GTFS extended export")
                .removeHeaders(Constants.CAMEL_ALL_HTTP_HEADERS)
                .to(ExchangePattern.InOnly, "entur-google-pubsub:GtfsExportMergedQueue")
                .routeId("admin-timetable-gtfs-extended-export")
                .endRest()


                .post("/export/gtfs/basic")
                .description("Prepare and upload GTFS basic export")
                .consumes(PLAIN)
                .produces(PLAIN)
                .responseMessage().code(200).endResponseMessage()
                .responseMessage().code(500).message("Internal error").endResponseMessage()
                .route()
                .to("direct:authorizeRequest")
                .log(LoggingLevel.INFO, "Triggered GTFS basic export")
                .removeHeaders(Constants.CAMEL_ALL_HTTP_HEADERS)
                .to(ExchangePattern.InOnly, "entur-google-pubsub:GtfsBasicExportMergedQueue")
                .routeId("admin-timetable-gtfs-basic-export")
                .endRest()

                .post("/export/gtfs/google")
                .description("Prepare and upload GTFS export to Google")
                .consumes(PLAIN)
                .produces(PLAIN)
                .responseMessage().code(200).endResponseMessage()
                .responseMessage().code(500).message("Internal error").endResponseMessage()
                .route()
                .to("direct:authorizeRequest")
                .log(LoggingLevel.INFO, "Triggered GTFS export to Google")
                .removeHeaders(Constants.CAMEL_ALL_HTTP_HEADERS)
                .to(ExchangePattern.InOnly, "entur-google-pubsub:GtfsGoogleExportQueue")
                .routeId("admin-timetable-google-export")
                .endRest()

                .post("/export/gtfs/google-qa")
                .description("Prepare and upload GTFS QA export to Google")
                .consumes(PLAIN)
                .produces(PLAIN)
                .responseMessage().code(200).endResponseMessage()
                .responseMessage().code(500).message("Internal error").endResponseMessage()
                .route()
                .to("direct:authorizeRequest")
                .log(LoggingLevel.INFO, "Triggered GTFS QA export to Google")
                .removeHeaders(Constants.CAMEL_ALL_HTTP_HEADERS)
                .to(ExchangePattern.InOnly, "entur-google-pubsub:GtfsGoogleQaExportQueue")
                .routeId("admin-timetable-google-qa-export")
                .endRest()

                .post("/export/google/publish")
                .description("Upload GTFS export to Google")
                .consumes(PLAIN)
                .produces(PLAIN)
                .responseMessage().code(200).endResponseMessage()
                .responseMessage().code(500).message("Internal error").endResponseMessage()
                .route()
                .to("direct:authorizeRequest")
                .log(LoggingLevel.INFO, "Triggered publish of GTFS to Google")
                .removeHeaders(Constants.CAMEL_ALL_HTTP_HEADERS)
                .to(ExchangePattern.InOnly, "entur-google-pubsub:GtfsGooglePublishQueue")
                .routeId("admin-timetable-google-publish")
                .endRest()

                .post("/export/google-qa/publish/")
                .description("Upload GTFS QA export to Google")
                .consumes(PLAIN)
                .produces(PLAIN)
                .responseMessage().code(200).endResponseMessage()
                .responseMessage().code(500).message("Internal error").endResponseMessage()
                .route()
                .to("direct:authorizeRequest")
                .log(LoggingLevel.INFO, "Triggered publish of GTFS QA export to Google")
                .removeHeaders(Constants.CAMEL_ALL_HTTP_HEADERS)
                .to(ExchangePattern.InOnly, "entur-google-pubsub:GtfsGooglePublishQaQueue")
                .routeId("admin-timetable-google-qa-publish")
                .endRest()


                .post("/export/netex/merged")
                .description("Prepare and upload a merged Netex file for Norway")
                .consumes(PLAIN)
                .produces(PLAIN)
                .responseMessage().code(200).endResponseMessage()
                .responseMessage().code(500).message("Internal error").endResponseMessage()
                .route()
                .to("direct:authorizeRequest")
                .log(LoggingLevel.INFO, "Triggered Netex export of merged file for Norway")
                .removeHeaders(Constants.CAMEL_ALL_HTTP_HEADERS)
                .to(ExchangePattern.InOnly, "entur-google-pubsub:NetexExportMergedQueue")
                .routeId("admin-timetable-netex-merged-export")
                .endRest()

                .post("routing_graph/build_base")
                .description("Triggers building of the OTP base graph using map data (osm + height)")
                .consumes(PLAIN)
                .produces(PLAIN)
                .responseMessage().code(200).message("Command accepted").endResponseMessage()
                .route()
                .process(e -> authorizationService.verifyAtLeastOne(new AuthorizationClaim(AuthorizationConstants.ROLE_ROUTE_DATA_ADMIN)))
                .log(LoggingLevel.INFO, "Triggered build of OTP base graph with map data")
                .removeHeaders(Constants.CAMEL_ALL_HTTP_HEADERS)
                .setBody(simple(""))
                .to(ExchangePattern.InOnly, "entur-google-pubsub:OtpBaseGraphBuildQueue")
                .routeId("admin-build-base-graph")
                .endRest()

                .post("routing_graph/build")
                .description("Triggers building of the OTP graph using existing NeTEx and and a pre-prepared base graph with map data")
                .consumes(PLAIN)
                .produces(PLAIN)
                .responseMessage().code(200).message("Command accepted").endResponseMessage()
                .route()
                .process(e -> authorizationService.verifyAtLeastOne(new AuthorizationClaim(AuthorizationConstants.ROLE_ROUTE_DATA_ADMIN)))
                .log(LoggingLevel.INFO, "OTP build graph from NeTEx")
                .removeHeaders(Constants.CAMEL_ALL_HTTP_HEADERS)
                .setBody(simple(""))
                .to(ExchangePattern.InOnly, "entur-google-pubsub:OtpGraphBuildQueue")
                .routeId("admin-build-graph-netex")
                .endRest()

                .post("/upload/{codespace}")
                .description("Upload NeTEx file")
                .param().name("codespace").type(RestParamType.path).description("Provider Codespace").dataType(SWAGGER_DATA_TYPE_STRING).endParam()
                .consumes(MULTIPART_FORM_DATA)
                .produces(PLAIN)
                .bindingMode(RestBindingMode.off)
                .responseMessage().code(200).endResponseMessage()
                .responseMessage().code(500).message("Internal server error").endResponseMessage()
                .route()
                .streamCaching()
                .setHeader(CHOUETTE_REFERENTIAL, header("codespace"))
                .log(LoggingLevel.INFO, correlation() + "Received file from provider ${header.codespace} through the HTTP endpoint")
                .validate(e -> getProviderRepository().getProviderId(e.getIn().getHeader(CHOUETTE_REFERENTIAL, String.class)) != null)
                .process(e -> e.getIn().setHeader(PROVIDER_ID, getProviderRepository().getProviderId(e.getIn().getHeader(CHOUETTE_REFERENTIAL, String.class))))
                .to("direct:authorizeRequest")
                .log(LoggingLevel.INFO, correlation() + "Authorization OK for HTTP endpoint, uploading files and starting import pipeline")
                .removeHeaders(Constants.CAMEL_ALL_HTTP_HEADERS)
                .to("direct:uploadFilesAndStartImport")
                .routeId("admin-upload-file")
                .endRest();

        rest("/timetable_admin/{providerId}")
                .post("/import")
                .description("Triggers the import->validate->export process in Chouette for each blob store file handle. Use /files call to obtain available files. Files are imported in the same order as they are provided")
                .param().name("providerId").type(RestParamType.path).description("Provider id as obtained from the nabu service").dataType(SWAGGER_DATA_TYPE_INTEGER).endParam()
                .type(BlobStoreFiles.class)
                .outType(String.class)
                .consumes(JSON)
                .produces(PLAIN)
                .responseMessage().code(200).message("Job accepted").endResponseMessage()
                .responseMessage().code(500).message("Invalid providerId").endResponseMessage()
                .route()
                .removeHeaders(Constants.CAMEL_ALL_HTTP_HEADERS)
                .setHeader(PROVIDER_ID, header("providerId"))
                .to("direct:authorizeRequest")
                .validate(e -> getProviderRepository().getProvider(e.getIn().getHeader(PROVIDER_ID, Long.class)) != null)
                .split(method(ImportFilesSplitter.class, "splitFiles"))

                .process(e -> e.getIn().setHeader(FILE_HANDLE, Constants.BLOBSTORE_PATH_INBOUND
                                                                       + getProviderRepository().getReferential(e.getIn().getHeader(PROVIDER_ID, Long.class))
                                                                       + "/" + e.getIn().getBody(String.class)))
                .process(e -> e.getIn().setHeader(CORRELATION_ID, UUID.randomUUID().toString()))
                .log(LoggingLevel.INFO, correlation() + "Chouette start import fileHandle=${body}")

                .process(e -> {
                    String fileNameForStatusLogging = "reimport-" + e.getIn().getBody(String.class);
                    e.getIn().setHeader(Constants.FILE_NAME, fileNameForStatusLogging);
                })
                .setBody(constant(null))

                .to(ExchangePattern.InOnly, "entur-google-pubsub:ProcessFileQueue")
                .routeId("admin-chouette-import")
                .endRest()

                .get("/files")
                .description("List files available for reimport into Chouette")
                .param().name("providerId").type(RestParamType.path).description("Provider id as obtained from the nabu service").dataType(SWAGGER_DATA_TYPE_INTEGER).endParam()
                .outType(BlobStoreFiles.class)
                .consumes(PLAIN)
                .produces(JSON)
                .responseMessage().code(200).endResponseMessage()
                .responseMessage().code(500).message("Invalid providerId").endResponseMessage()
                .route()
                .setHeader(PROVIDER_ID, header("providerId"))
                .to("direct:authorizeRequest")
                .validate(e -> getProviderRepository().getProvider(e.getIn().getHeader(PROVIDER_ID, Long.class)) != null)
                .log(LoggingLevel.INFO, correlation() + "blob store get files")
                .removeHeaders(Constants.CAMEL_ALL_HTTP_HEADERS)
                .to("direct:listBlobsFlat")
                .routeId("admin-chouette-import-list")
                .endRest()

                .post("/files")
                .description("Upload file for import into Chouette")
                .param().name("providerId").type(RestParamType.path).description("Provider id as obtained from the nabu service").dataType(SWAGGER_DATA_TYPE_INTEGER).endParam()
                .consumes(MULTIPART_FORM_DATA)
                .produces(PLAIN)
                .bindingMode(RestBindingMode.off)
                .responseMessage().code(200).endResponseMessage()
                .responseMessage().code(500).message("Invalid providerId").endResponseMessage()
                .route()
                .streamCaching()
                .setHeader(PROVIDER_ID, header("providerId"))
                .to("direct:authorizeRequest")
                .validate(e -> getProviderRepository().getProvider(e.getIn().getHeader(PROVIDER_ID, Long.class)) != null)
                .process(e -> e.getIn().setHeader(CHOUETTE_REFERENTIAL, getProviderRepository().getReferential(e.getIn().getHeader(PROVIDER_ID, Long.class))))
                .log(LoggingLevel.INFO, correlation() + "upload files and start import pipeline")
                .removeHeaders(Constants.CAMEL_ALL_HTTP_HEADERS)
                .to("direct:uploadFilesAndStartImport")
                .routeId("admin-chouette-upload-file")
                .endRest()

                .get("/files/{fileName}")
                .description("Download file for reimport into Chouette")
                .param().name("providerId").type(RestParamType.path).description("Provider id as obtained from the nabu service").dataType(SWAGGER_DATA_TYPE_INTEGER).endParam()
                .param().name("fileName").type(RestParamType.path).description("Name of file to fetch").dataType(SWAGGER_DATA_TYPE_STRING).endParam()
                .consumes(PLAIN)
                .produces(X_OCTET_STREAM)
                .responseMessage().code(200).endResponseMessage()
                .responseMessage().code(500).message("Invalid fileName").endResponseMessage()
                .route()
                .setHeader(PROVIDER_ID, header("providerId"))
                .to("direct:authorizeRequest")
                .validate(e -> getProviderRepository().getProvider(e.getIn().getHeader(PROVIDER_ID, Long.class)) != null)
                .process(e -> e.getIn().setHeader("fileName", URLDecoder.decode(e.getIn().getHeader("fileName", String.class), StandardCharsets.UTF_8)))
                .process(e -> e.getIn().setHeader(FILE_HANDLE, Constants.BLOBSTORE_PATH_INBOUND
                                                                       + getProviderRepository().getReferential(e.getIn().getHeader(PROVIDER_ID, Long.class))
                                                                       + "/" + e.getIn().getHeader("fileName", String.class)))
                .log(LoggingLevel.INFO, correlation() + "blob store download file by name")
                .removeHeaders(Constants.CAMEL_ALL_HTTP_HEADERS)
                .to("direct:getBlob")
                .choice().when(simple("${body} == null")).setHeader(Exchange.HTTP_RESPONSE_CODE, constant(404)).endChoice()
                .routeId("admin-chouette-file-download")
                .endRest()

                .get("/line_statistics")
                .description("List stats about data in chouette for a given provider")
                .param().name("providerId").type(RestParamType.path).description("Provider id as obtained from the nabu service").dataType(SWAGGER_DATA_TYPE_INTEGER).endParam()
                .bindingMode(RestBindingMode.off)
                .consumes(PLAIN)
                .produces(JSON)
                .responseMessage().code(200).endResponseMessage()
                .responseMessage().code(500).message("Invalid providerId").endResponseMessage()
                .route()
                .setHeader(PROVIDER_ID, header("providerId"))
                .to("direct:authorizeRequest")
                .validate(e -> getProviderRepository().getProvider(e.getIn().getHeader(PROVIDER_ID, Long.class)) != null)
                .log(LoggingLevel.INFO, correlation() + "get stats")
                .removeHeaders(Constants.CAMEL_ALL_HTTP_HEADERS)
                .to("direct:chouetteGetStatsSingleProvider")
                .routeId("admin-chouette-stats")
                .endRest()

                .get("/jobs")
                .description("List Chouette jobs for a given provider")
                .param().name("providerId").type(RestParamType.path).description("Provider id as obtained from the nabu service").dataType(SWAGGER_DATA_TYPE_INTEGER).endParam()
                .param()
                .required(Boolean.FALSE)
                .name("status")
                .type(RestParamType.query)
                .description("Chouette job statuses")
                .allowableValues(Arrays.stream(Status.values()).map(Status::name).collect(Collectors.toList()))
                .endParam()
                .param()
                .required(Boolean.FALSE)
                .name("action")
                .type(RestParamType.query)
                .description("Chouette job types")
                .allowableValues("importer", "exporter", "validator")
                .endParam()
                .outType(JobResponse[].class)
                .consumes(PLAIN)
                .produces(JSON)
                .responseMessage().code(200).endResponseMessage()
                .responseMessage().code(500).message("Invalid providerId").endResponseMessage()
                .route()
                .setHeader(PROVIDER_ID, header("providerId"))
                .to("direct:authorizeRequest")
                .validate(e -> getProviderRepository().getProvider(e.getIn().getHeader(PROVIDER_ID, Long.class)) != null)
                .log(LoggingLevel.INFO, correlation() + "Get chouette jobs status=${header.status} action=${header.action}")
                .removeHeaders(Constants.CAMEL_ALL_HTTP_HEADERS)
                .to("direct:chouetteGetJobsForProvider")
                .routeId("admin-chouette-list-jobs")
                .endRest()

                .delete("/jobs")
                .description("Cancel all Chouette jobs for a given provider")
                .param().name("providerId").type(RestParamType.path).description("Provider id as obtained from the nabu service").dataType(SWAGGER_DATA_TYPE_INTEGER).endParam()
                .consumes(PLAIN)
                .produces(PLAIN)
                .responseMessage().code(200).message("Job deleted").endResponseMessage()
                .responseMessage().code(500).message("Invalid jobId").endResponseMessage()
                .route()
                .setHeader(PROVIDER_ID, header("providerId"))
                .to("direct:authorizeRequest")
                .validate(e -> getProviderRepository().getProvider(e.getIn().getHeader(PROVIDER_ID, Long.class)) != null)
                .log(LoggingLevel.INFO, correlation() + "Cancel all chouette jobs")
                .removeHeaders(Constants.CAMEL_ALL_HTTP_HEADERS)
                .to("direct:chouetteCancelAllJobsForProvider")
                .routeId("admin-chouette-cancel-all-jobs")
                .endRest()

                .delete("/jobs/{jobId}")
                .description("Cancel a Chouette job for a given provider")
                .param().name("providerId").type(RestParamType.path).description("Provider id as obtained from the nabu service").dataType(SWAGGER_DATA_TYPE_INTEGER).endParam()
                .param().name("jobId").type(RestParamType.path).description("Job id as returned in any of the /jobs GET calls").dataType(SWAGGER_DATA_TYPE_INTEGER).endParam()
                .consumes(PLAIN)
                .produces(PLAIN)
                .responseMessage().code(200).message("Job deleted").endResponseMessage()
                .responseMessage().code(500).message("Invalid jobId").endResponseMessage()
                .route()
                .setHeader(PROVIDER_ID, header("providerId"))
                .to("direct:authorizeRequest")
                .validate(e -> getProviderRepository().getProvider(e.getIn().getHeader(PROVIDER_ID, Long.class)) != null)
                .setHeader(Constants.CHOUETTE_JOB_ID, header("jobId"))
                .log(LoggingLevel.INFO, correlation() + "Cancel chouette job")
                .removeHeaders(Constants.CAMEL_ALL_HTTP_HEADERS)
                .to("direct:chouetteCancelJob")
                .routeId("admin-chouette-cancel-job")
                .endRest()

                .post("/export")
                .description("Triggers the export process in Chouette. Note that NO validation is performed before export, and that the data must be guaranteed to be error free")
                .param().name("providerId").type(RestParamType.path).description("Provider id as obtained from the nabu service").dataType(SWAGGER_DATA_TYPE_INTEGER).endParam()
                .consumes(PLAIN)
                .produces(PLAIN)
                .responseMessage().code(200).message("Command accepted").endResponseMessage()
                .route()
                .setHeader(PROVIDER_ID, header("providerId"))
                .to("direct:authorizeRequest")
                .validate(e -> getProviderRepository().getProvider(e.getIn().getHeader(PROVIDER_ID, Long.class)) != null)
                .log(LoggingLevel.INFO, correlation() + "Chouette start export")
                .removeHeaders(Constants.CAMEL_ALL_HTTP_HEADERS)
                .to(ExchangePattern.InOnly, "entur-google-pubsub:ChouetteExportNetexQueue")
                .routeId("admin-chouette-export")
                .endRest()

                .post("/validate")
                .description("Triggers the validate->export process in Chouette")
                .param().name("providerId").type(RestParamType.path).description("Provider id as obtained from the nabu service").dataType(SWAGGER_DATA_TYPE_INTEGER).endParam()
                .consumes(PLAIN)
                .produces(PLAIN)
                .responseMessage().code(200).message("Command accepted").endResponseMessage()
                .route()
                .setHeader(PROVIDER_ID, header("providerId"))
                .to("direct:authorizeRequest")
                .validate(e -> getProviderRepository().getProvider(e.getIn().getHeader(PROVIDER_ID, Long.class)) != null)
                .log(LoggingLevel.INFO, correlation() + "Chouette start validation")
                .removeHeaders(Constants.CAMEL_ALL_HTTP_HEADERS)

                .choice().when(e -> getProviderRepository().getProvider(e.getIn().getHeader(PROVIDER_ID, Long.class)).chouetteInfo.migrateDataToProvider == null)
                .setHeader(CHOUETTE_JOB_STATUS_JOB_VALIDATION_LEVEL, constant(JobEvent.TimetableAction.VALIDATION_LEVEL_2.name()))
                .otherwise()
                .setHeader(CHOUETTE_JOB_STATUS_JOB_VALIDATION_LEVEL, constant(JobEvent.TimetableAction.VALIDATION_LEVEL_1.name()))
                .end()
                .to(ExchangePattern.InOnly, "entur-google-pubsub:ChouetteValidationQueue")
                .routeId("admin-chouette-validate")
                .endRest()

                .post("/clean")
                .description("Triggers the clean dataspace process in Chouette. Only timetable data are deleted, not job data (imports, exports, validations)")
                .param().name("providerId").type(RestParamType.path).description("Provider id as obtained from the nabu service").dataType(SWAGGER_DATA_TYPE_INTEGER).endParam()
                .consumes(PLAIN)
                .produces(PLAIN)
                .responseMessage().code(200).message("Command accepted").endResponseMessage()
                .route()
                .setHeader(PROVIDER_ID, header("providerId"))
                .validate(e -> getProviderRepository().getProvider(e.getIn().getHeader(PROVIDER_ID, Long.class)) != null)
                .log(LoggingLevel.INFO, correlation() + "Chouette clean dataspace")
                .removeHeaders(Constants.CAMEL_ALL_HTTP_HEADERS)
                .to("direct:chouetteCleanReferential")
                .routeId("admin-chouette-clean")
                .endRest()

                .post("/transfer")
                .description("Triggers transfer of data from one dataspace to the next")
                .param().name("providerId").type(RestParamType.path).description("Provider id as obtained from the nabu service").dataType(SWAGGER_DATA_TYPE_INTEGER).endParam()
                .consumes(PLAIN)
                .produces(PLAIN)
                .responseMessage().code(200).message("Command accepted").endResponseMessage()
                .route()
                .setHeader(PROVIDER_ID, header("providerId"))
                .log(LoggingLevel.INFO, correlation() + "Chouette transfer dataspace")
                .removeHeaders(Constants.CAMEL_ALL_HTTP_HEADERS)
                .setHeader(PROVIDER_ID, header("providerId"))
                .validate(e -> getProviderRepository().getProvider(e.getIn().getHeader(PROVIDER_ID, Long.class)) != null)
                .to(ExchangePattern.InOnly, "entur-google-pubsub:ChouetteTransferExportQueue")
                .routeId("admin-chouette-transfer")
                .endRest();


        rest("/map_admin")
                .post("/download")
                .description("Triggers downloading of the latest OSM data")
                .consumes(PLAIN)
                .produces(PLAIN)
                .responseMessage().code(200).message("Command accepted").endResponseMessage()
                .route()
                .process(e -> authorizationService.verifyAtLeastOne(new AuthorizationClaim(AuthorizationConstants.ROLE_ROUTE_DATA_ADMIN)))
                .log(LoggingLevel.INFO, "OSM update map data")
                .removeHeaders(Constants.CAMEL_ALL_HTTP_HEADERS)
                .to("direct:considerToFetchOsmMapOverNorway")
                .routeId("admin-fetch-osm")
                .endRest();


        from("direct:authorizeRequest")
                .doTry()
                .process(e -> authorizationService.verifyAtLeastOne(new AuthorizationClaim(AuthorizationConstants.ROLE_ROUTE_DATA_ADMIN),
                        new AuthorizationClaim(AuthorizationConstants.ROLE_ROUTE_DATA_EDIT, e.getIn().getHeader(PROVIDER_ID, Long.class))))
                .routeId("admin-authorize-request");

    }

    public static class ImportFilesSplitter {
        public List<String> splitFiles(@Body BlobStoreFiles files) {
            return files.getFiles().stream().map(File::getName).collect(Collectors.toList());
        }
    }
}


