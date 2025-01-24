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
import no.rutebanken.marduk.Utils;
import no.rutebanken.marduk.domain.BlobStoreFiles;
import no.rutebanken.marduk.domain.BlobStoreFiles.File;
import no.rutebanken.marduk.domain.OtpGraphsInfo;
import no.rutebanken.marduk.routes.BaseRouteBuilder;
import no.rutebanken.marduk.routes.chouette.json.JobResponse;
import no.rutebanken.marduk.routes.chouette.json.Status;
import no.rutebanken.marduk.routes.status.JobEvent;
import no.rutebanken.marduk.security.MardukAuthorizationService;
import org.apache.camel.*;
import org.apache.camel.model.rest.RestBindingMode;
import org.apache.camel.model.rest.RestParamType;
import org.rutebanken.helper.organisation.NotAuthenticatedException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.stereotype.Component;

import jakarta.ws.rs.NotFoundException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

import static jakarta.ws.rs.core.MediaType.MULTIPART_FORM_DATA;
import static no.rutebanken.marduk.Constants.*;
import static org.apache.camel.support.builder.PredicateBuilder.isEqualTo;

/**
 * API endpoint for managing the transit data import pipeline.
 */
@Component
public class AdminRestRouteBuilder extends BaseRouteBuilder {


    private static final String JSON = "application/json";
    private static final String X_OCTET_STREAM = "application/x-octet-stream";
    private static final String PLAIN = "text/plain";
    private static final String OPENAPI_DATA_TYPE_STRING = "string";
    private static final String OPENAPI_DATA_TYPE_INTEGER = "integer";

    @Value("${server.port:8080}")
    private String port;

    @Value("${server.host:0.0.0.0}")
    private String host;

    @Autowired
    private MardukAuthorizationService mardukAuthorizationService;

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
                .apiContextPath("/openapi.json")
                .apiProperty("api.title", "Marduk Admin API").apiProperty("api.version", "1.0");

        rest("")
                .apiDocs(false)
                .description("Wildcard definitions necessary to get Jetty to match authorization filters to endpoints with path params")
                .get()
                .to("direct:adminRouteAuthorizeGet")
                .post()
                .to("direct:adminRouteAuthorizePost")
                .put()
                .to("direct:adminRouteAuthorizePut")
                .delete()
                .to("direct:adminRouteAuthorizeDelete");

        String commonApiDocEndpoint = "http:" + host + ":" + port + "/services/openapi.json?bridgeEndpoint=true";

        rest("/timetable_admin")
                .post("/idempotentfilter/clean")
                .description("Clean unique filename and digest Idempotent Stores")
                .responseMessage().code(200).endResponseMessage()
                .responseMessage().code(500).message("Internal error").endResponseMessage()
                .to("direct:adminApplicationCleanUniqueFilenameAndDigestIdempotentRepos")

                .post("/validate/level1")
                .description("Triggers the validate->transfer process for all level1 providers in Chouette")
                .consumes(PLAIN)
                .produces(PLAIN)
                .responseMessage().code(200).message("Command accepted").endResponseMessage()
                .to("direct:adminChouetteValidateLevel1AllProviders")

                .post("/validate/level2")
                .description("Triggers the validate->export process for all level2 providers in Chouette")
                .consumes(PLAIN)
                .produces(PLAIN)
                .responseMessage().code(200).message("Command accepted").endResponseMessage()
                .to("direct:adminChouetteValidateLevel2AllProviders")

                .get("/jobs")
                .description("List Chouette jobs for all providers. Filters defaults to status=SCHEDULED,STARTED")
                .param()
                .required(Boolean.FALSE)
                .name("status")
                .type(RestParamType.query)
                .description("Chouette job statuses")
                .allowableValues(Arrays.stream(Status.values()).map(Status::name).toList())
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
                .to("direct:adminChouetteListJobsAll")

                .delete("/jobs")
                .description("Cancel all Chouette jobs for all providers")
                .responseMessage().code(200).message("All jobs canceled").endResponseMessage()
                .responseMessage().code(500).message("Could not cancel all jobs").endResponseMessage()
                .to("direct:adminChouetteCancelAllJobsAll")

                .delete("/completed_jobs")
                .description("Remove completed Chouette jobs for all providers. ")
                .param()
                .required(Boolean.FALSE)
                .name("keepJobs")
                .type(RestParamType.query)
                .dataType(OPENAPI_DATA_TYPE_INTEGER)
                .description("No of jobs to keep, regardless of age")
                .endParam()
                .param()
                .required(Boolean.FALSE)
                .name("keepDays")
                .type(RestParamType.query)
                .dataType(OPENAPI_DATA_TYPE_INTEGER)
                .description("No of days to keep jobs for")
                .endParam()
                .responseMessage().code(200).message("Completed jobs removed").endResponseMessage()
                .responseMessage().code(500).message("Could not remove complete jobs").endResponseMessage()
                .to("direct:adminChouetteRemoveOldJobs")

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
                .to("direct:adminChouetteCleanAll")

                .post("/stop_places/clean")
                .description("Triggers the cleaning of ALL stop places in Chouette")
                .consumes(PLAIN)
                .produces(PLAIN)
                .responseMessage().code(200).message("Command accepted").endResponseMessage()
                .responseMessage().code(500).message("Internal error - check filter").endResponseMessage()
                .to("direct:adminChouetteCleanStopPlaces")

                .get("/line_statistics/{filter}")
                .description("List stats about data in chouette for multiple providers")
                .param().name("providerIds")
                .type(RestParamType.query).dataType(OPENAPI_DATA_TYPE_INTEGER)
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
                .to("direct:adminChouetteStatsMultipleProviders")

                .post("/line_statistics/refresh")
                .description("Recalculate stats about data in chouette for all providers")
                .bindingMode(RestBindingMode.off)
                .consumes(PLAIN)
                .produces(PLAIN)
                .responseMessage().code(200).endResponseMessage()
                .responseMessage().code(500).message("Internal error").endResponseMessage()
                .to("direct:adminChouetteStatsRefreshCache")

                .get("/export/files")
                .description("List files containing exported time table data and graphs")
                .outType(BlobStoreFiles.class)
                .consumes(PLAIN)
                .produces(JSON)
                .responseMessage().code(200).endResponseMessage()
                .responseMessage().code(500).message("Internal error").endResponseMessage()
                .to("direct:adminChouetteTimetableFilesGet")

                .post("/export/gtfs/extended")
                .description("Prepare and upload GTFS extened export")
                .consumes(PLAIN)
                .produces(PLAIN)
                .responseMessage().code(200).endResponseMessage()
                .responseMessage().code(500).message("Internal error").endResponseMessage()
                .to("direct:adminTimetableGtfsExtendedExport")

                .post("/export/gtfs/basic")
                .description("Prepare and upload GTFS basic export")
                .consumes(PLAIN)
                .produces(PLAIN)
                .responseMessage().code(200).endResponseMessage()
                .responseMessage().code(500).message("Internal error").endResponseMessage()
                .to("direct:adminTimetableGtfsBasicExport")

                .post("/export/netex/merged")
                .description("Prepare and upload a merged Netex file for Norway")
                .consumes(PLAIN)
                .produces(PLAIN)
                .responseMessage().code(200).endResponseMessage()
                .responseMessage().code(500).message("Internal error").endResponseMessage()
                .to("direct:adminTimetableNetexMergedExport")

                .post("routing_graph/build_base")
                .description("Triggers building of the OTP base graph using map data (osm + height)")
                .consumes(PLAIN)
                .produces(PLAIN)
                .responseMessage().code(200).message("Command accepted").endResponseMessage()
                .to("direct:adminBuildBaseGraph")

                .post("routing_graph/build")
                .description("Triggers building of the OTP graph using existing NeTEx and and a pre-prepared base graph with map data")
                .consumes(PLAIN)
                .produces(PLAIN)
                .responseMessage().code(200).message("Command accepted").endResponseMessage()
                .to("direct:adminBuildGraphNetex")

                .post("routing_graph/build_candidate/{graphType}")
                .description("Triggers graph building for a candidate OTP version")
                .param().name("graphType").type(RestParamType.path).description("Type of graph").dataType(OPENAPI_DATA_TYPE_STRING).endParam()
                .consumes(PLAIN)
                .produces(PLAIN)
                .responseMessage().code(200).message("Command accepted").endResponseMessage()
                .to("direct:adminBuildGraphCandidate")

                .get("routing_graph/graphs")
                .description("List latest generated OTP2 graphs")
                .outType(OtpGraphsInfo[].class)
                .consumes(PLAIN)
                .produces(JSON)
                .responseMessage().code(200).endResponseMessage()
                .to("direct:adminListGraphs")

                .post("/upload/{codespace}")
                .description("Upload NeTEx file")
                .param().name("codespace").type(RestParamType.path).description("Provider Codespace").dataType(OPENAPI_DATA_TYPE_STRING).endParam()
                .consumes(MULTIPART_FORM_DATA)
                .produces(PLAIN)
                .bindingMode(RestBindingMode.off)
                .responseMessage().code(200).endResponseMessage()
                .responseMessage().code(500).message("Internal server error").endResponseMessage()
                .to("direct:adminUploadFile")

                .get("/download_netex_blocks/{codespace}")
                .description("Download NeTEx dataset with blocks")
                .param().name("codespace").type(RestParamType.path).description("Codespace of the organization producing the NeTEx dataset with blocks").dataType(OPENAPI_DATA_TYPE_STRING).endParam()
                .consumes(PLAIN)
                .produces(X_OCTET_STREAM)
                .responseMessage().code(200).endResponseMessage()
                .responseMessage().code(500).message("Invalid codespace").endResponseMessage()
                .to("direct:adminChouetteNetexBlocksDownload")

                .get("/openapi.json")
                .apiDocs(false)
                .bindingMode(RestBindingMode.off)
                .to(commonApiDocEndpoint);

        rest("/timetable_admin/{providerId}")
                .post("/import")
                .description("Triggers the import->validate->export process in Chouette for each blob store file handle. Use /files call to obtain available files. Files are imported in the same order as they are provided")
                .param().name("providerId").type(RestParamType.path).description("Provider id as obtained from the nabu service").dataType(OPENAPI_DATA_TYPE_INTEGER).endParam()
                .type(BlobStoreFiles.class)
                .outType(String.class)
                .consumes(JSON)
                .produces(PLAIN)
                .responseMessage().code(200).message("Job accepted").endResponseMessage()
                .responseMessage().code(500).message("Invalid providerId").endResponseMessage()
                .to("direct:adminDatasetImport")

                .post("/flex/import")
                .description("Triggers the import->validate->export for each blob store file handle. Use /files call to obtain available files. Files are imported in the same order as they are provided")
                .param().name("providerId").type(RestParamType.path).description("Provider id as obtained from the nabu service").dataType(OPENAPI_DATA_TYPE_INTEGER).endParam()
                .type(BlobStoreFiles.class)
                .outType(String.class)
                .consumes(JSON)
                .produces(PLAIN)
                .responseMessage().code(200).message("Job accepted").endResponseMessage()
                .responseMessage().code(500).message("Invalid providerId").endResponseMessage()
                .to("direct:adminFlexImport")

                .get("/files")
                .description("List files available for reimport")
                .param().name("providerId").type(RestParamType.path).description("Provider id as obtained from the nabu service").dataType(OPENAPI_DATA_TYPE_INTEGER).endParam()
                .outType(BlobStoreFiles.class)
                .consumes(PLAIN)
                .produces(JSON)
                .responseMessage().code(200).endResponseMessage()
                .responseMessage().code(500).message("Invalid providerId").endResponseMessage()
                .to("direct:adminDatasetImportList")

                .post("/files")
                .description("Upload file for import into Chouette")
                .param().name("providerId").type(RestParamType.path).description("Provider id as obtained from the nabu service").dataType(OPENAPI_DATA_TYPE_INTEGER).endParam()
                .consumes(MULTIPART_FORM_DATA)
                .produces(PLAIN)
                .bindingMode(RestBindingMode.off)
                .responseMessage().code(200).endResponseMessage()
                .responseMessage().code(500).message("Invalid providerId").endResponseMessage()
                .to("direct:adminDatasetUploadFile")

                .post("/flex/files")
                .description("Upload flexible line file for import")
                .param().name("providerId").type(RestParamType.path).description("Provider id as obtained from the nabu service").dataType(OPENAPI_DATA_TYPE_INTEGER).endParam()
                .consumes(MULTIPART_FORM_DATA)
                .produces(PLAIN)
                .bindingMode(RestBindingMode.off)
                .responseMessage().code(200).endResponseMessage()
                .responseMessage().code(500).message("Invalid providerId").endResponseMessage()
                .to("direct:adminUploadFlexFile")

                .get("/files/{fileName}")
                .description("Download file for reimport")
                .param().name("providerId").type(RestParamType.path).description("Provider id as obtained from the nabu service").dataType(OPENAPI_DATA_TYPE_INTEGER).endParam()
                .param().name("fileName").type(RestParamType.path).description("Name of file to fetch").dataType(OPENAPI_DATA_TYPE_STRING).endParam()
                .consumes(PLAIN)
                .produces(X_OCTET_STREAM)
                .responseMessage().code(200).endResponseMessage()
                .responseMessage().code(500).message("Invalid fileName").endResponseMessage()
                .to("direct:adminDatasetFileDownload")

                .get("/line_statistics")
                .description("List stats about data in chouette for a given provider")
                .param().name("providerId").type(RestParamType.path).description("Provider id as obtained from the nabu service").dataType(OPENAPI_DATA_TYPE_INTEGER).endParam()
                .bindingMode(RestBindingMode.off)
                .consumes(PLAIN)
                .produces(JSON)
                .responseMessage().code(200).endResponseMessage()
                .responseMessage().code(500).message("Invalid providerId").endResponseMessage()
                .to("direct:adminChouetteStats")

                .get("/jobs")
                .description("List Chouette jobs for a given provider")
                .param().name("providerId").type(RestParamType.path).description("Provider id as obtained from the nabu service").dataType(OPENAPI_DATA_TYPE_INTEGER).endParam()
                .param()
                .required(Boolean.FALSE)
                .name("status")
                .type(RestParamType.query)
                .description("Chouette job statuses")
                .allowableValues(Arrays.stream(Status.values()).map(Status::name).toList())
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
                .to("direct:adminChouetteListJobs")

                .delete("/jobs")
                .description("Cancel all Chouette jobs for a given provider")
                .param().name("providerId").type(RestParamType.path).description("Provider id as obtained from the nabu service").dataType(OPENAPI_DATA_TYPE_INTEGER).endParam()
                .consumes(PLAIN)
                .produces(PLAIN)
                .responseMessage().code(200).message("Job deleted").endResponseMessage()
                .responseMessage().code(500).message("Invalid jobId").endResponseMessage()
                .to("direct:adminChouetteCancelAllJobs")

                .delete("/jobs/{jobId}")
                .description("Cancel a Chouette job for a given provider")
                .param().name("providerId").type(RestParamType.path).description("Provider id as obtained from the nabu service").dataType(OPENAPI_DATA_TYPE_INTEGER).endParam()
                .param().name("jobId").type(RestParamType.path).description("Job id as returned in any of the /jobs GET calls").dataType(OPENAPI_DATA_TYPE_INTEGER).endParam()
                .consumes(PLAIN)
                .produces(PLAIN)
                .responseMessage().code(200).message("Job deleted").endResponseMessage()
                .responseMessage().code(500).message("Invalid jobId").endResponseMessage()
                .to("direct:adminChouetteCancelJob")

                .post("/export")
                .description("Triggers the export process in Chouette. Note that NO validation is performed before export, and that the data must be guaranteed to be error free")
                .param().name("providerId").type(RestParamType.path).description("Provider id as obtained from the nabu service").dataType(OPENAPI_DATA_TYPE_INTEGER).endParam()
                .consumes(PLAIN)
                .produces(PLAIN)
                .responseMessage().code(200).message("Command accepted").endResponseMessage()
                .to("direct:adminChouetteExport")

                .post("/validate")
                .description("Triggers the validate->export process in Chouette")
                .param().name("providerId").type(RestParamType.path).description("Provider id as obtained from the nabu service").dataType(OPENAPI_DATA_TYPE_INTEGER).endParam()
                .consumes(PLAIN)
                .produces(PLAIN)
                .responseMessage().code(200).message("Command accepted").endResponseMessage()
                .to("direct:adminChouetteValidate")

                .post("/clean")
                .description("Triggers the clean dataspace process in Chouette. Only timetable data are deleted, not job data (imports, exports, validations)")
                .param().name("providerId").type(RestParamType.path).description("Provider id as obtained from the nabu service").dataType(OPENAPI_DATA_TYPE_INTEGER).endParam()
                .consumes(PLAIN)
                .produces(PLAIN)
                .responseMessage().code(200).message("Command accepted").endResponseMessage()
                .to("direct:adminChouetteClean")

                .post("/transfer")
                .description("Triggers transfer of data from one dataspace to the next")
                .param().name("providerId").type(RestParamType.path).description("Provider id as obtained from the nabu service").dataType(OPENAPI_DATA_TYPE_INTEGER).endParam()
                .consumes(PLAIN)
                .produces(PLAIN)
                .responseMessage().code(200).message("Command accepted").endResponseMessage()
                .to("direct:adminChouetteTransfer");

        rest("/map_admin")
                .post("/download")
                .description("Triggers downloading of the latest OSM data")
                .consumes(PLAIN)
                .produces(PLAIN)
                .responseMessage().code(200).message("Command accepted").endResponseMessage()
                .to("direct:adminFetchOsm");

        from("direct:adminRouteAuthorizeGet")
                .throwException(new NotFoundException())
                .routeId("admin-route-authorize-get");

        from("direct:adminRouteAuthorizePost")
                .throwException(new NotFoundException())
                .routeId("admin-route-authorize-post");

        from("direct:adminRouteAuthorizePut")
                .throwException(new NotFoundException())
                .routeId("admin-route-authorize-put");

        from("direct:adminRouteAuthorizeDelete")
                .throwException(new NotFoundException())
                .routeId("admin-route-authorize-delete");

        from("direct:adminApplicationCleanUniqueFilenameAndDigestIdempotentRepos")
                .to("direct:authorizeAdminRequest")
                .to("direct:cleanIdempotentFileStore")
                .setBody(constant(""))
                .routeId("admin-application-clean-unique-filename-and-digest-idempotent-repos");

        from("direct:adminChouetteValidateLevel1AllProviders")
                .to("direct:authorizeAdminRequest")
                .log(LoggingLevel.INFO, correlation() + "Chouette start validation level1 for all providers")
                .process(this::removeAllCamelHttpHeaders)
                .to(ExchangePattern.InOnly, "direct:chouetteValidateLevel1ForAllProviders")
                .setBody(constant(""))
                .routeId("admin-chouette-validate-level1-all-providers");

        from("direct:adminChouetteValidateLevel2AllProviders")
                .to("direct:authorizeAdminRequest")
                .log(LoggingLevel.INFO, correlation() + "Chouette start validation level2 for all providers")
                .process(this::removeAllCamelHttpHeaders)
                .to(ExchangePattern.InOnly, "direct:chouetteValidateLevel2ForAllProviders")
                .setBody(constant(""))
                .routeId("admin-chouette-validate-level2-all-providers");

        from("direct:adminChouetteListJobsAll")
                .to("direct:authorizeAdminRequest")
                .log(LoggingLevel.DEBUG, correlation() + "Get chouette active jobs all providers")
                .process(this::removeAllCamelHttpHeaders)
                .process(e -> e.getIn().setHeader("status", e.getIn().getHeader("status") != null ? e.getIn().getHeader("status") : Arrays.asList("STARTED", "SCHEDULED")))
                .to("direct:chouetteGetJobsAll")
                .routeId("admin-chouette-list-jobs-all");

        from("direct:adminChouetteCancelAllJobsAll")
                .to("direct:authorizeAdminRequest")
                .log(LoggingLevel.INFO, correlation() + "Cancel all chouette jobs for all providers")
                .process(this::removeAllCamelHttpHeaders)
                .to("direct:chouetteCancelAllJobsForAllProviders")
                .setBody(constant(""))
                .routeId("admin-chouette-cancel-all-jobs-all");

        from("direct:adminChouetteRemoveOldJobs")
                .to("direct:authorizeAdminRequest")
                .log(LoggingLevel.INFO, correlation() + "Removing old chouette jobs for all providers")
                .process(this::removeAllCamelHttpHeaders)
                .to("direct:chouetteRemoveOldJobs")
                .setBody(constant(""))
                .routeId("admin-chouette-remove-old-jobs");

        from("direct:adminChouetteCleanAll")
                .to("direct:authorizeAdminRequest")
                .log(LoggingLevel.INFO, correlation() + "Chouette clean all dataspaces")
                .process(this::removeAllCamelHttpHeaders)
                .to("direct:chouetteCleanAllReferentials")
                .setBody(constant(""))
                .routeId("admin-chouette-clean-all");

        from("direct:adminChouetteCleanStopPlaces")
                .to("direct:authorizeAdminRequest")
                .log(LoggingLevel.INFO, correlation() + "Chouette clean all stop places")
                .process(this::removeAllCamelHttpHeaders)
                .to("direct:chouetteCleanStopPlaces")
                .setBody(constant(""))
                .routeId("admin-chouette-clean-stop-places");


        from("direct:adminChouetteStatsMultipleProviders")
                .to("direct:authorizeAdminRequest")
                .process(this::removeAllCamelHttpHeaders)
                .choice()
                .when(simple("${header.providerIds}"))
                .process(e -> e.getIn().setHeader(PROVIDER_IDS, e.getIn().getHeader("providerIds", "", String.class).split(",")))
                .end()
                .log(LoggingLevel.INFO, correlation() + "Get lines statistics for multiple providers (providers whitelist: [${header." + PROVIDER_IDS + "}])")
                .to("direct:chouetteGetStats")
                .routeId("admin-chouette-stats-multiple-providers");

        from("direct:adminChouetteStatsRefreshCache")
                .to("direct:authorizeAdminRequest")
                .log(LoggingLevel.INFO, correlation() + "refresh stats cache")
                .process(this::removeAllCamelHttpHeaders)
                .to("direct:chouetteRefreshStatsCache")
                .routeId("admin-chouette-stats-refresh-cache");

        from("direct:adminChouetteTimetableFilesGet")
                .process(this::setNewCorrelationId)
                .to("direct:authorizeAdminRequest")
                .log(LoggingLevel.INFO, correlation() + "List time table and graph files")
                .process(this::removeAllCamelHttpHeaders)
                .to("direct:listTimetableExportAndGraphBlobs")
                .routeId("admin-chouette-timetable-files-get");

        from("direct:adminDatasetFileDownload")
                .setHeader(PROVIDER_ID, header("providerId"))
                .to("direct:authorizeAdminRequest")
                .to("direct:validateProvider")
                .process(e -> e.getIn().setHeader("fileName", URLDecoder.decode(e.getIn().getHeader("fileName", String.class), StandardCharsets.UTF_8)))
                .process(e -> e.getIn().setHeader(FILE_HANDLE, Constants.BLOBSTORE_PATH_INBOUND
                        + getProviderRepository().getReferential(e.getIn().getHeader(PROVIDER_ID, Long.class))
                        + "/" + e.getIn().getHeader("fileName", String.class)))
                .log(LoggingLevel.INFO, correlation() + "blob store download file by name")
                .process(this::removeAllCamelHttpHeaders)
                .to("direct:getInternalBlob")
                .choice().when(simple("${body} == null")).setHeader(Exchange.HTTP_RESPONSE_CODE, constant(404)).endChoice()
                .routeId("admin-chouette-file-download");

        from("direct:adminTimetableGtfsExtendedExport")
                .to("direct:authorizeAdminRequest")
                .log(LoggingLevel.INFO, "Triggered GTFS extended export")
                .process(this::removeAllCamelHttpHeaders)
                .to(ExchangePattern.InOnly, "google-pubsub:{{marduk.pubsub.project.id}}:GtfsExportMergedQueue")
                .routeId("admin-timetable-gtfs-extended-export");

        from("direct:adminTimetableGtfsBasicExport")
                .to("direct:authorizeAdminRequest")
                .log(LoggingLevel.INFO, "Triggered GTFS basic export")
                .process(this::removeAllCamelHttpHeaders)
                .to(ExchangePattern.InOnly, "google-pubsub:{{marduk.pubsub.project.id}}:GtfsBasicExportMergedQueue")
                .routeId("admin-timetable-gtfs-basic-export");

        from("direct:adminTimetableNetexMergedExport")
                .to("direct:authorizeAdminRequest")
                .log(LoggingLevel.INFO, "Triggered Netex export of merged file for Norway")
                .process(this::removeAllCamelHttpHeaders)
                .to(ExchangePattern.InOnly, "google-pubsub:{{marduk.pubsub.project.id}}:NetexExportMergedQueue")
                .routeId("admin-timetable-netex-merged-export");

        from("direct:adminBuildBaseGraph")
                .to("direct:authorizeAdminRequest")
                .log(LoggingLevel.INFO, "Triggered build of OTP base graph with map data")
                .process(this::removeAllCamelHttpHeaders)
                .setBody(simple(""))
                .to(ExchangePattern.InOnly, "google-pubsub:{{marduk.pubsub.project.id}}:Otp2BaseGraphBuildQueue")
                .routeId("admin-build-base-graph");

        from("direct:adminBuildGraphNetex")
                .to("direct:authorizeAdminRequest")
                .log(LoggingLevel.INFO, "OTP build graph from NeTEx")
                .process(this::removeAllCamelHttpHeaders)
                .setBody(simple(""))
                .to(ExchangePattern.InOnly, "google-pubsub:{{marduk.pubsub.project.id}}:Otp2GraphBuildQueue")
                .routeId("admin-build-graph-netex");

        from("direct:adminBuildGraphCandidate")
                .to("direct:authorizeAdminRequest")
                .process(this::removeAllCamelHttpHeaders)
                .setBody(simple(""))
                .choice()
                .when(isEqualTo(header("graphType"), constant("otp2_base")))
                .log(LoggingLevel.INFO, "OTP2 build candidate base graph")
                .to(ExchangePattern.InOnly, "google-pubsub:{{marduk.pubsub.project.id}}:Otp2BaseGraphCandidateBuildQueue")
                .when(isEqualTo(header("graphType"), constant("otp2_netex")))
                .log(LoggingLevel.INFO, "OTP2 build candidate NeTEx graph")
                .to(ExchangePattern.InOnly, "google-pubsub:{{marduk.pubsub.project.id}}:Otp2GraphCandidateBuildQueue")
                .otherwise()
                .setBody(constant("Unknown Graph Type"))
                .setHeader(Exchange.HTTP_RESPONSE_CODE, constant(400))
                .setHeader(Exchange.CONTENT_TYPE, constant(PLAIN))
                .end()
                .routeId("admin-build-graph-candidate");

        from("direct:adminListGraphs")
                .process(this::setNewCorrelationId)
                .to("direct:authorizeAdminRequest")
                .log(LoggingLevel.INFO, correlation() + "List graphs status")
                .process(this::removeAllCamelHttpHeaders)
                .to("direct:listGraphs")
                .routeId("admin-chouette-graph-list");

        from("direct:adminUploadFile")
                .streamCache("true")
                .process(this::setNewCorrelationId)
                .setHeader(CHOUETTE_REFERENTIAL, header("codespace"))
                .log(LoggingLevel.INFO, correlation() + "Received file from provider ${header.codespace} through the HTTP endpoint")
                .to("direct:validateReferential")
                .process(e -> e.getIn().setHeader(PROVIDER_ID, getProviderRepository().getProviderId(e.getIn().getHeader(CHOUETTE_REFERENTIAL, String.class))))
                .to("direct:authorizeEditorRequest")
                .log(LoggingLevel.INFO, correlation() + "Authorization OK for HTTP endpoint, uploading files and starting import pipeline")
                .process(this::removeAllCamelHttpHeaders)
                .setHeader(FILE_APPLY_DUPLICATES_FILTER, simple("${properties:duplicate.filter.rest:true}", Boolean.class))
                .to("direct:uploadFilesAndStartImport")
                .routeId("admin-upload-file")
                .autoStartup("{{netex.import.http.autoStartup:true}}");

        from("direct:adminChouetteNetexBlocksDownload")
                .setHeader(CHOUETTE_REFERENTIAL, header("codespace"))
                .log(LoggingLevel.INFO, correlation() + "Received Blocks download request for provider ${header." + CHOUETTE_REFERENTIAL + "} through the HTTP endpoint")
                .to("direct:validateReferential")
                .process(e -> e.getIn().setHeader(PROVIDER_ID, getProviderRepository().getProviderId(e.getIn().getHeader(CHOUETTE_REFERENTIAL, String.class))))
                .to("direct:authorizeBlocksDownloadRequest")
                .process(e -> e.getIn().setHeader(FILE_HANDLE, Constants.BLOBSTORE_PATH_NETEX_BLOCKS_EXPORT
                        + "rb_" + e.getIn().getHeader(CHOUETTE_REFERENTIAL, String.class).toLowerCase()
                        + "-" + Constants.CURRENT_AGGREGATED_NETEX_FILENAME))
                .log(LoggingLevel.INFO, correlation() + "Downloading NeTEx dataset with blocks: ${header." + FILE_HANDLE + "}")
                .process(this::removeAllCamelHttpHeaders)
                .to("direct:getInternalBlob")
                .choice().when(simple("${body} == null")).setHeader(Exchange.HTTP_RESPONSE_CODE, constant(404)).endChoice()
                .routeId("admin-chouette-netex-blocks-download");

        from("direct:adminDatasetImport")
                .process(this::removeAllCamelHttpHeaders)
                .setHeader(PROVIDER_ID, header("providerId"))
                .to("direct:authorizeAdminRequest")
                .to("direct:validateProvider")
                .split(method(ImportFilesSplitter.class, "splitFiles"))

                .process(e -> e.getIn().setHeader(FILE_HANDLE, Constants.BLOBSTORE_PATH_INBOUND
                        + getProviderRepository().getReferential(e.getIn().getHeader(PROVIDER_ID, Long.class))
                        + "/" + e.getIn().getBody(String.class)))
                .process(this::setNewCorrelationId)
                .log(LoggingLevel.INFO, correlation() + "Chouette start import fileHandle=${body}")

                .process(e -> {
                    String fileNameForStatusLogging = "reimport-" + e.getIn().getBody(String.class);
                    e.getIn().setHeader(Constants.FILE_NAME, fileNameForStatusLogging);
                })
                .setBody(constant(""))

                .to(ExchangePattern.InOnly, "google-pubsub:{{marduk.pubsub.project.id}}:ProcessFileQueue")
                .routeId("admin-chouette-import");

        from("direct:adminFlexImport")
                .routeId("admin-flex-import")
                .setHeader(IMPORT_TYPE, constant(IMPORT_TYPE_NETEX_FLEX))
                .to("direct:adminDatasetImport");

        from("direct:adminChouetteStats")
                .process(this::setNewCorrelationId)
                .setHeader(PROVIDER_ID, header("providerId"))
                .to("direct:authorizeEditorRequest")
                .to("direct:validateProvider")
                .log(LoggingLevel.INFO, correlation() + "Get line statistics for provider ${header.providerId}")
                .process(this::removeAllCamelHttpHeaders)
                .to("direct:chouetteGetStatsSingleProvider")
                .routeId("admin-chouette-stats");

        from("direct:adminDatasetImportList")
                .process(this::setNewCorrelationId)
                .setHeader(PROVIDER_ID, header("providerId"))
                .to("direct:authorizeAdminRequest")
                .to("direct:validateProvider")
                .log(LoggingLevel.INFO, correlation() + "List files in blob store")
                .process(this::removeAllCamelHttpHeaders)
                .to("direct:listInternalBlobsFlat")
                .routeId("admin-chouette-import-list");

        from("direct:adminUploadFlexFile")
                .routeId("admin-upload-flex-file")
                .setHeader(IMPORT_TYPE, constant(IMPORT_TYPE_NETEX_FLEX))
                .to("direct:adminDatasetUploadFile");

        from("direct:adminDatasetUploadFile")
                .streamCache("true")
                .process(this::setNewCorrelationId)
                .setHeader(PROVIDER_ID, header("providerId"))
                .to("direct:authorizeEditorRequest")
                .to("direct:validateProvider")
                .process(e -> e.getIn().setHeader(CHOUETTE_REFERENTIAL, getProviderRepository().getReferential(e.getIn().getHeader(PROVIDER_ID, Long.class))))
                .log(LoggingLevel.INFO, correlation() + "Upload files and start import pipeline")
                .process(this::removeAllCamelHttpHeaders)
                .setHeader(FILE_APPLY_DUPLICATES_FILTER, simple("${properties:duplicate.filter.web:true}", Boolean.class))
                .setHeader(FILE_APPLY_DUPLICATES_FILTER_ON_NAME_ONLY, constant(true))
                .to("direct:uploadFilesAndStartImport")
                .routeId("admin-chouette-upload-file");

        from("direct:adminChouetteListJobs")
                .process(this::setNewCorrelationId)
                .setHeader(PROVIDER_ID, header("providerId"))
                .to("direct:authorizeAdminRequest")
                .to("direct:validateProvider")
                .log(LoggingLevel.INFO, correlation() + "Get chouette jobs status=${header.status} action=${header.action}")
                .process(this::removeAllCamelHttpHeaders)
                .to("direct:chouetteGetJobsForProvider")
                .routeId("admin-chouette-list-jobs");

        from("direct:adminChouetteCancelAllJobs")
                .setHeader(PROVIDER_ID, header("providerId"))
                .to("direct:authorizeAdminRequest")
                .to("direct:validateProvider")
                .log(LoggingLevel.INFO, correlation() + "Cancel all chouette jobs")
                .process(this::removeAllCamelHttpHeaders)
                .to("direct:chouetteCancelAllJobsForProvider")
                .routeId("admin-chouette-cancel-all-jobs");

        from("direct:adminChouetteCancelJob")
                .setHeader(PROVIDER_ID, header("providerId"))
                .to("direct:authorizeAdminRequest")
                .to("direct:validateProvider")
                .setHeader(Constants.CHOUETTE_JOB_ID, header("jobId"))
                .log(LoggingLevel.INFO, correlation() + "Cancel chouette job")
                .process(this::removeAllCamelHttpHeaders)
                .to("direct:chouetteCancelJob")
                .routeId("admin-chouette-cancel-job");

        from("direct:adminChouetteExport")
                .setHeader(PROVIDER_ID, header("providerId"))
                .to("direct:authorizeAdminRequest")
                .to("direct:validateProvider")
                .log(LoggingLevel.INFO, correlation() + "Chouette start export")
                .process(this::removeAllCamelHttpHeaders)
                .to(ExchangePattern.InOnly, "google-pubsub:{{marduk.pubsub.project.id}}:ChouetteExportNetexQueue")
                .routeId("admin-chouette-export");

        from("direct:adminChouetteValidate")
                .setHeader(PROVIDER_ID, header("providerId"))
                .to("direct:authorizeEditorRequest")
                .to("direct:validateProvider")
                .log(LoggingLevel.INFO, correlation() + "Chouette start validation")
                .process(this::removeAllCamelHttpHeaders)

                .choice().when(e -> getProviderRepository().getProvider(e.getIn().getHeader(PROVIDER_ID, Long.class)).getChouetteInfo().getMigrateDataToProvider() == null)
                .setHeader(CHOUETTE_JOB_STATUS_JOB_VALIDATION_LEVEL, constant(JobEvent.TimetableAction.VALIDATION_LEVEL_2.name()))
                .otherwise()
                .setHeader(CHOUETTE_JOB_STATUS_JOB_VALIDATION_LEVEL, constant(JobEvent.TimetableAction.VALIDATION_LEVEL_1.name()))
                .end()
                .to(ExchangePattern.InOnly, "google-pubsub:{{marduk.pubsub.project.id}}:ChouetteValidationQueue")
                .routeId("admin-chouette-validate");

        from("direct:adminFetchOsm")
                .process(this::setNewCorrelationId)
                .to("direct:authorizeAdminRequest")
                .log(LoggingLevel.INFO, "OSM update map data")
                .process(this::removeAllCamelHttpHeaders)
                .to("direct:considerToFetchOsmMapOverNorway")
                .routeId("admin-fetch-osm");

        from("direct:adminChouetteClean")
                .setHeader(PROVIDER_ID, header("providerId"))
                .to("direct:validateProvider")
                .to("direct:authorizeAdminRequest")
                .log(LoggingLevel.INFO, correlation() + "Chouette clean dataspace")
                .process(this::removeAllCamelHttpHeaders)
                .to("direct:chouetteCleanReferential")
                .routeId("admin-chouette-clean");

        from("direct:adminChouetteTransfer")
                .setHeader(PROVIDER_ID, header("providerId"))
                .log(LoggingLevel.INFO, correlation() + "Chouette transfer dataspace")
                .process(this::removeAllCamelHttpHeaders)
                .setHeader(PROVIDER_ID, header("providerId"))
                .to("direct:validateProvider")
                .to("direct:authorizeAdminRequest")
                .to(ExchangePattern.InOnly, "google-pubsub:{{marduk.pubsub.project.id}}:ChouetteTransferExportQueue")
                .routeId("admin-chouette-transfer");


        from("direct:validateProvider")
                .validate(e -> getProviderRepository().getProvider(e.getIn().getHeader(PROVIDER_ID, Long.class)) != null)
                .predicateExceptionFactory((exchange, predicate, nodeId) -> new NotFoundException("Unknown provider id"))
                .id("validate-provider")
                .routeId("admin-validate-provider");

        from("direct:validateReferential")
                .validate(e -> getProviderRepository().getProviderId(e.getIn().getHeader(CHOUETTE_REFERENTIAL, String.class)) != null)
                .predicateExceptionFactory((exchange, predicate, nodeId) -> new NotFoundException("Unknown chouette referential"))
                .id("validate-referential")
                .routeId("admin-validate-referential");

        from("direct:authorizeAdminRequest")
                .doTry()
                .process(e -> mardukAuthorizationService.verifyAdministratorPrivileges())
                .setHeader(USERNAME, method(Utils.class, "getUsername"))
                .routeId("admin-authorize-admin-request");

        from("direct:authorizeEditorRequest")
                .doTry()
                .process(e -> mardukAuthorizationService.verifyRouteDataEditorPrivileges(e.getIn().getHeader(PROVIDER_ID, Long.class)))
                .setHeader(USERNAME, method(Utils.class, "getUsername"))
                .routeId("admin-authorize-editor-request");

        from("direct:authorizeBlocksDownloadRequest")
                .doTry()
                .log(LoggingLevel.INFO, "Authorizing NeTEx blocks download for provider ${header." + CHOUETTE_REFERENTIAL + "} ")
                .process(e -> mardukAuthorizationService.verifyBlockViewerPrivileges(e.getIn().getHeader(PROVIDER_ID, Long.class)))
                .routeId("admin-authorize-blocks-download-request");

    }


    public static class ImportFilesSplitter {
        public List<String> splitFiles(@Body BlobStoreFiles files) {
            return files.getFiles().stream().map(File::getName).toList();
        }
    }
}


