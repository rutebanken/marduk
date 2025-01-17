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

package no.rutebanken.marduk.routes.netex;

import no.rutebanken.marduk.Constants;
import no.rutebanken.marduk.routes.BaseRouteBuilder;
import no.rutebanken.marduk.routes.file.ZipFileUtils;
import no.rutebanken.marduk.routes.status.JobEvent;
import org.apache.camel.Exchange;
import org.apache.camel.LoggingLevel;
import org.apache.camel.builder.PredicateBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.InputStream;

import static no.rutebanken.marduk.Constants.BLOBSTORE_PATH_CHOUETTE;
import static no.rutebanken.marduk.Constants.BLOBSTORE_PATH_OUTBOUND;
import static no.rutebanken.marduk.Constants.CHOUETTE_REFERENTIAL;
import static no.rutebanken.marduk.Constants.CORRELATION_ID;
import static no.rutebanken.marduk.Constants.FILE_HANDLE;
import static no.rutebanken.marduk.Constants.FOLDER_NAME;
import static no.rutebanken.marduk.Constants.PROVIDER_ID;
import static no.rutebanken.marduk.Constants.VALIDATION_CLIENT_HEADER;
import static no.rutebanken.marduk.Constants.VALIDATION_CLIENT_MARDUK;
import static no.rutebanken.marduk.Constants.VALIDATION_CORRELATION_ID_HEADER;
import static no.rutebanken.marduk.Constants.VALIDATION_DATASET_FILE_HANDLE_HEADER;
import static no.rutebanken.marduk.Constants.VALIDATION_PROFILE_HEADER;
import static no.rutebanken.marduk.Constants.VALIDATION_PROFILE_TIMETABLE_FLEX_MERGING;
import static no.rutebanken.marduk.Constants.VALIDATION_STAGE_EXPORT_MERGED_POSTVALIDATION;
import static no.rutebanken.marduk.Constants.VALIDATION_STAGE_HEADER;

/**
 * Merge NeTEx dataset exported from Chouette with NeTEx dataset with flexible lines
 */
@Component
public class NetexMergeChouetteWithFlexibleLineExportRouteBuilder extends BaseRouteBuilder {

    private static final String UNPACKED_WITH_FLEXIBLE_LINES_SUB_FOLDER = "/unpacked-with-flexible-lines";
    private static final String MERGED_NETEX_SUB_FOLDER = "/result";

    private static final String BLOBSTORE_PATH_UTTU = "uttu/";
    private static final String PROP_HAS_CHOUETTE_DATA = "PROP_HAS_CHOUETTE_DATA";
    private static final String PROP_AS_FLEXIBLE_DATA = "PROP_HAS_FLEXIBLE_DATA";

    private static final String EXPORT_FILE_NAME = "netex/${header." + CHOUETTE_REFERENTIAL + "}-" + Constants.CURRENT_AGGREGATED_NETEX_FILENAME;
    private static final String EXPORT_MERGED_FOR_VALIDATION = BLOBSTORE_PATH_UTTU +   "netex/${header." + CHOUETTE_REFERENTIAL + "}" + "/${header." + CORRELATION_ID + "}_${date:now:yyyyMMddHHmmssSSS}-" + Constants.CURRENT_AGGREGATED_NETEX_FILENAME;


    @Value("${netex.export.download.directory:files/netex/merged}")
    private String localWorkingDirectory;

    @Value("${netex.export.merge.flexible.lines.enabled:false}")
    private String mergeFlexibleLinesEnabled;


    @Override
    public void configure() throws Exception {
        super.configure();

        from("google-pubsub:{{marduk.pubsub.project.id}}:ChouetteMergeWithFlexibleLinesQueue")
                .to("direct:mergeChouetteExportWithFlexibleLinesExport")
                .routeId("netex-export-merge-chouette-with-flexible-lines-queue");

        from("direct:mergeChouetteExportWithFlexibleLinesExport")
                .streamCache("true")
                .process(this::setCorrelationIdIfMissing)
                .log(LoggingLevel.INFO, getClass().getName(), correlation() + "Merging chouette NeTEx export with FlexibleLines")
                .validate(header(Constants.CHOUETTE_REFERENTIAL).isNotNull())

                .process(e -> e.getIn().setHeader(PROVIDER_ID, getProviderRepository().getProviderId(e.getIn().getHeader(CHOUETTE_REFERENTIAL, String.class))))
                .validate(header(Constants.PROVIDER_ID).isNotNull())


                .setProperty(FOLDER_NAME, simple(localWorkingDirectory + "/${header." + CORRELATION_ID + "}_${date:now:yyyyMMddHHmmssSSS}"))
                .doTry()
                .to("direct:unpackChouetteExportToWorkingFolder")
                .to("direct:unpackFlexibleLinesExportToWorkingFolder")

                .choice()

                .when(PredicateBuilder.and(exchangeProperty(PROP_HAS_CHOUETTE_DATA).isNotNull(), exchangeProperty(PROP_AS_FLEXIBLE_DATA).isNotNull()))
                .to("direct:uploadMergedFileToValidationFolder")
                .to("direct:antuMergedNetexPostValidation")

                .otherwise()
                .to("direct:uploadMergedFileToOutboundBucket")
                .to("direct:publishMergedDataset")

                .endDoTry()
                .doFinally()
                .process(e -> e.getIn().setHeader(Exchange.FILE_PARENT, e.getProperty(FOLDER_NAME)))
                .to("direct:cleanUpLocalDirectory")
                .end()

                .routeId("netex-export-merge-chouette-with-flexible-lines");


        from("direct:uploadMergedFileToOutboundBucket")
                .streamCache("true")
                .process(e -> new File(e.getProperty(FOLDER_NAME, String.class) + MERGED_NETEX_SUB_FOLDER).mkdir())
                .process(e -> e.getIn().setBody(
                        ZipFileUtils.zipFilesInFolder(
                                e.getProperty(FOLDER_NAME, String.class) + UNPACKED_WITH_FLEXIBLE_LINES_SUB_FOLDER,
                                e.getProperty(FOLDER_NAME, String.class) + MERGED_NETEX_SUB_FOLDER + "/merged.zip")))
                .setHeader(FILE_HANDLE, simple(BLOBSTORE_PATH_OUTBOUND + EXPORT_FILE_NAME))
                .to("direct:uploadBlob")
                .routeId("netex-upload-merged-netex-to-outbound-bucket");


        from("direct:uploadMergedFileToValidationFolder")
                .process(e -> new File(e.getProperty(FOLDER_NAME, String.class) + MERGED_NETEX_SUB_FOLDER).mkdir())
                .process(e -> e.getIn().setBody(
                        ZipFileUtils.zipFilesInFolder(
                                e.getProperty(FOLDER_NAME, String.class) + UNPACKED_WITH_FLEXIBLE_LINES_SUB_FOLDER,
                                e.getProperty(FOLDER_NAME, String.class) + MERGED_NETEX_SUB_FOLDER + "/merged.zip")))
                .setHeader(FILE_HANDLE, simple(EXPORT_MERGED_FOR_VALIDATION))
                .to("direct:uploadInternalBlob")
                .routeId("netex-merged-upload-to-validation-folder");


        from("direct:unpackChouetteExportToWorkingFolder")
                .setHeader(FILE_HANDLE, simple(BLOBSTORE_PATH_CHOUETTE + EXPORT_FILE_NAME))
                .to("direct:getInternalBlob")
                .choice()
                .when(body().isNotEqualTo(null))
                .process(e -> ZipFileUtils.unzipFile(e.getIn().getBody(InputStream.class), e.getProperty(FOLDER_NAME, String.class) + UNPACKED_WITH_FLEXIBLE_LINES_SUB_FOLDER))
                .setProperty(PROP_HAS_CHOUETTE_DATA, constant("true"))
                .otherwise()
                .log(LoggingLevel.INFO, getClass().getName(), correlation() + "${header." + FILE_HANDLE + "} was empty when trying to fetch it from blobstore.")
                .routeId("netex-export-merge-chouette-with-flexible-lines-unpack-chouette-export");


        from("direct:unpackFlexibleLinesExportToWorkingFolder")
                .choice().when(constant(mergeFlexibleLinesEnabled))
                .to("direct:doUnpackFlexibleLinesExportToWorkingFolder")
                .otherwise()
                .log(LoggingLevel.INFO, getClass().getName(), correlation() + "Skipping merge with flexible lines as this is disabled.")
                .routeId("netex-export-merge-chouette-with-flexible-lines-unpack-flexible-lines-export");

        // do unpack in a sub-route to avoid having nested choice(), which does not work
        from("direct:doUnpackFlexibleLinesExportToWorkingFolder")
                .setHeader(FILE_HANDLE, simple(BLOBSTORE_PATH_OUTBOUND + "netex/${header." + CHOUETTE_REFERENTIAL + "}-" + Constants.CURRENT_FLEXIBLE_LINES_NETEX_FILENAME))
                .to("direct:fetchExternalBlob")
                .choice()
                .when(body().isNotEqualTo(null))
                .process(e -> ZipFileUtils.unzipFile(e.getIn().getBody(InputStream.class), e.getProperty(FOLDER_NAME, String.class) + UNPACKED_WITH_FLEXIBLE_LINES_SUB_FOLDER))
                .setProperty(PROP_AS_FLEXIBLE_DATA, constant("true"))
                .otherwise()
                .log(LoggingLevel.INFO, getClass().getName(), correlation() + "No flexible line data found: ${header." + FILE_HANDLE + "} was empty when trying to fetch it from blobstore.")
                .routeId("netex-export-merge-chouette-with-flexible-lines-do-unpack-flexible-lines-export");

        // start the validation in antu
        from("direct:antuMergedNetexPostValidation")
                .log(LoggingLevel.INFO, correlation() + "validating Merged NeTEx dataset")
                .to("direct:copyInternalBlobToValidationBucket")
                .setHeader(VALIDATION_STAGE_HEADER, constant(VALIDATION_STAGE_EXPORT_MERGED_POSTVALIDATION))
                .setHeader(VALIDATION_CLIENT_HEADER, constant(VALIDATION_CLIENT_MARDUK))
                .setHeader(VALIDATION_PROFILE_HEADER, constant(VALIDATION_PROFILE_TIMETABLE_FLEX_MERGING))
                .setHeader(VALIDATION_DATASET_FILE_HANDLE_HEADER, header(FILE_HANDLE))
                .setHeader(VALIDATION_CORRELATION_ID_HEADER, header(CORRELATION_ID))
                .to("google-pubsub:{{antu.pubsub.project.id}}:AntuNetexValidationQueue")
                .process(e -> JobEvent.providerJobBuilder(e)
                        .timetableAction(JobEvent.TimetableAction.EXPORT_NETEX_MERGED_POSTVALIDATION)
                        .state(JobEvent.State.PENDING)
                        .jobId(null)
                        .build())
                .to("direct:updateStatus")
                .routeId("antu-merged-netex-post-validation");

    }
}
