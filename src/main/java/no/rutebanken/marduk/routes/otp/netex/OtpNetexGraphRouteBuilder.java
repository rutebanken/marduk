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

package no.rutebanken.marduk.routes.otp.netex;

import no.rutebanken.marduk.Constants;
import no.rutebanken.marduk.routes.BaseRouteBuilder;
import no.rutebanken.marduk.routes.otp.GraphBuilderProcessor;
import no.rutebanken.marduk.routes.status.JobEvent;
import org.apache.camel.LoggingLevel;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.util.List;

import static no.rutebanken.marduk.Constants.BLOBSTORE_PATH_OUTBOUND;
import static no.rutebanken.marduk.Constants.CHOUETTE_REFERENTIAL;
import static no.rutebanken.marduk.Constants.CORRELATION_ID;
import static no.rutebanken.marduk.Constants.FILE_HANDLE;
import static no.rutebanken.marduk.Constants.FILE_PARENT_COLLECTION;
import static no.rutebanken.marduk.Constants.FOLDER_NAME;
import static no.rutebanken.marduk.Constants.GRAPH_OBJ;
import static no.rutebanken.marduk.Constants.OTP_GRAPH_DIR;
import static no.rutebanken.marduk.Constants.TIMESTAMP;
import static org.apache.camel.Exchange.FILE_PARENT;
import static org.apache.camel.builder.Builder.exceptionStackTrace;

/**
 * Trigger OTP graph building based on NeTEx data.
 */
@Component
public class OtpNetexGraphRouteBuilder extends BaseRouteBuilder {

    private static final String BUILD_CONFIG_JSON = "build-config.json";

    @Value("${otp.netex.graph.build.directory:files/otpgraph/netex}")
    private String otpGraphBuildDirectory;

    @Value("${otp.graph.blobstore.subdirectory:graphs}")
    private String blobStoreSubdirectory;

    @Value("${otp.netex.graph.build.config:}")
    private String otpGraphBuildConfig;

    @Value("${netex.export.file.path:netex/rb_norway-aggregated-netex.zip}")
    private String netexExportMergedFilePath;

    // File name for import. OTP requires specific file name pattern to parse file as netex (ends with netex_no.zip)
    @Value("${otp.netex.import.file.name:netex_no.zip}")
    private String otpGraphImportFileName;

    @Value("#{'${otp.graph.netex.additional.files.subdirectory.names:}'.split(',')}")
    private List<String> additionalFilesSubDirectories;


    private static final String PROP_MESSAGES = "RutebankenPropMessages";

    private static final String PROP_STATUS = "RutebankenGraphBuildStatus";

    @Override
    public void configure() throws Exception {
        super.configure();

        from("direct:buildOtpGraph")
                .setProperty(PROP_MESSAGES, simple("${body}"))
                .setProperty(TIMESTAMP, simple("${date:now:yyyyMMddHHmmssSSS}"))

                .to("direct:sendOtpNetexGraphBuildStartedEventsInNewTransaction")
                .setProperty(OTP_GRAPH_DIR, simple(otpGraphBuildDirectory + "/${header." + CORRELATION_ID + "}_${property." + TIMESTAMP + "}"))
                .log(LoggingLevel.INFO, getClass().getName(), correlation() + "Starting graph building in directory ${property." + OTP_GRAPH_DIR + "}.")
                .to("direct:fetchBaseGraph")
                .to("direct:fetchLatestNetex")
                .to("direct:fetchBuildConfigForOtpNetexGraph")
                .to("direct:fetchAdditionalFilesForOtpGraphBuild")
                .to("direct:buildNetexGraphAndSendStatus")
                .log(LoggingLevel.INFO, getClass().getName(), "Done with OTP graph building route.")
                .routeId("otp-netex-graph-build");

        from("direct:sendOtpNetexGraphBuildStartedEventsInNewTransaction")

                .process(e -> JobEvent.systemJobBuilder(e).jobDomain(JobEvent.JobDomain.GRAPH).action("BUILD_GRAPH").state(JobEvent.State.STARTED).correlationId(e.getProperty(TIMESTAMP, String.class)).build()).to("direct:updateStatus")
                .setProperty(PROP_STATUS, constant(JobEvent.State.STARTED))
                .to("direct:sendStatusForOtpNetexJobs")
                .routeId("otp-netex-graph-send-started-events");

        from("direct:sendStatusForOtpNetexJobs")
                .doTry() // <- doTry seems necessary for correct transactional handling. not sure why...
                .split().exchangeProperty(PROP_MESSAGES)
                .filter(simple("${headers[" + CHOUETTE_REFERENTIAL + "]}"))
                .process(e -> {
                    JobEvent.State state = e.getProperty(PROP_STATUS, JobEvent.State.class);
                    JobEvent.providerJobBuilder(e).timetableAction(JobEvent.TimetableAction.BUILD_GRAPH).state(state).build();
                })
                .to("direct:updateStatus")
                .routeId("otp-netex-graph-send-status-for-timetable-jobs");

        from("direct:fetchLatestNetex")
                .to("direct:exportMergedNetex")
                .setHeader(FILE_HANDLE, simple(BLOBSTORE_PATH_OUTBOUND + netexExportMergedFilePath))
                .log(LoggingLevel.INFO, getClass().getName(), correlation() + "Fetching latest netex file for norway: " + " ${header." + FILE_HANDLE + "}")
                .to("direct:getBlob")
                .choice()
                .when(body().isNotEqualTo(null))
                .toD("file:${property." + OTP_GRAPH_DIR + "}/" + otpGraphImportFileName)
                .otherwise()
                .log(LoggingLevel.INFO, getClass().getName(), correlation() + "${property.fileName} was empty when trying to fetch it from blobstore.")
                .routeId("otp-netex-graph-get-netex");

        from("direct:fetchBuildConfigForOtpNetexGraph")
                .choice().when(constant(StringUtils.isEmpty(otpGraphBuildConfig)))
                .log(LoggingLevel.DEBUG, getClass().getName(), correlation() + "Fetching config ...")
                .to("language:constant:resource:classpath:no/rutebanken/marduk/routes/otp/" + BUILD_CONFIG_JSON)
                .otherwise()
                .log(LoggingLevel.WARN, getClass().getName(), correlation() + "Using overridden otp build config from property")
                .setBody(constant(otpGraphBuildConfig))
                .end()
                .toD("file:${property." + OTP_GRAPH_DIR + "}/" + BUILD_CONFIG_JSON)
                .log(LoggingLevel.DEBUG, getClass().getName(), correlation() + BUILD_CONFIG_JSON + " fetched.")
                .routeId("otp-netex-graph-fetch-config");

        from("direct:fetchBaseGraph")
                .log(LoggingLevel.DEBUG, getClass().getName(), correlation() + "Fetching base graph ...")
                .setHeader(FILE_HANDLE, simple(blobStoreSubdirectory + "/" + Constants.BASE_GRAPH_OBJ))
                .to("direct:getBlob")
                .toD("file:${property." + OTP_GRAPH_DIR + "}/" + Constants.BASE_GRAPH_OBJ)
                .log(LoggingLevel.DEBUG, getClass().getName(), correlation() + " fetched base graph")
                .routeId("otp-netex-graph-fetch-base-graph");

        from("direct:buildNetexGraphAndSendStatus")
                .log(LoggingLevel.INFO, correlation() + "Building OTP graph...")
                .doTry()
                .to("direct:buildNetexGraph")
                .setProperty(PROP_STATUS, constant(JobEvent.State.OK))
                .to("direct:sendStatusForOtpNetexJobs")
                .doCatch(Exception.class)
                .log(LoggingLevel.ERROR, correlation() + "Graph building failed: " + exceptionMessage() + " stacktrace: " + exceptionStackTrace())
                .process(e -> JobEvent.systemJobBuilder(e).jobDomain(JobEvent.JobDomain.GRAPH).action("BUILD_GRAPH").state(JobEvent.State.FAILED).correlationId(e.getProperty(TIMESTAMP, String.class)).build()).to("direct:updateStatus")
                .setProperty(PROP_STATUS, constant(JobEvent.State.FAILED))
                .to("direct:sendStatusForOtpNetexJobs")
                .setHeader(FILE_PARENT, exchangeProperty(OTP_GRAPH_DIR))
                .to("direct:cleanUpLocalDirectory")
                .end()
                .routeId("otp-netex-graph-build-and-send-status");

        from("direct:buildNetexGraph")
                .process(new GraphBuilderProcessor())
                .setBody(constant(""))
                .toD("file:${property." + OTP_GRAPH_DIR + "}/" + GRAPH_OBJ + ".done")
                .to("log:" + getClass().getName() + "?level=DEBUG&showAll=true&multiline=true")
                .log(LoggingLevel.INFO, correlation() + "Done building new OTP graph.")
                .routeId("otp-netex-graph-build-otp");


        from("direct:fetchAdditionalFilesForOtpGraphBuild")
                .log(LoggingLevel.INFO, getClass().getName(), correlation() + "Fetching additional files graph building from: " + additionalFilesSubDirectories)
                .filter(constant(!CollectionUtils.isEmpty(additionalFilesSubDirectories) && additionalFilesSubDirectories.stream().anyMatch(d -> !StringUtils.isEmpty(d))))
                .setHeader(FILE_PARENT_COLLECTION, constant(additionalFilesSubDirectories))
                .to("direct:listBlobsInFolders")
                .split().simple("${body.files}")
                .filter(simple("${body.toString().trim()} != ''"))
                .setProperty("tmpFileName", simple("${body.fileNameOnly}"))
                .filter(simple("${body.fileNameOnly}"))
                .setHeader(FILE_HANDLE, simple("${body.name}"))
                .to("direct:getBlob")
                .toD("file:${property." + OTP_GRAPH_DIR + "}/${exchangeProperty.tmpFileName}")
                .log(LoggingLevel.INFO, getClass().getName(), correlation() + "Fetched additional file:${header." + FILE_HANDLE + "}")
                .routeId("otp-graph-build-fetch-additional-files");
    }
}
