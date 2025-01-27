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
import no.rutebanken.marduk.routes.file.FileType;
import no.rutebanken.marduk.routes.status.JobEvent;
import org.apache.camel.LoggingLevel;
import org.apache.camel.builder.PredicateBuilder;
import org.springframework.stereotype.Component;

import static no.rutebanken.marduk.Constants.*;

@Component
public class AntuNetexValidationStatusRouteBuilder extends AbstractChouetteRouteBuilder {

    protected static final String STATUS_VALIDATION_STARTED = "started";
    protected static final String STATUS_VALIDATION_OK = "ok";
    protected static final String STATUS_VALIDATION_FAILED = "failed";

    @Override
    public void configure() throws Exception {
        super.configure();

        from("google-pubsub:{{marduk.pubsub.project.id}}:AntuNetexValidationStatusQueue")
                .validate(header(Constants.VALIDATION_DATASET_FILE_HANDLE_HEADER).isNotNull())
                .validate(header(Constants.VALIDATION_CORRELATION_ID_HEADER).isNotNull())
                .process(e -> e.getIn().setHeader(PROVIDER_ID, getProviderRepository().getProviderId(e.getIn().getHeader(DATASET_REFERENTIAL, String.class))))
                .setHeader(CORRELATION_ID, header(VALIDATION_CORRELATION_ID_HEADER))
                .setHeader(FILE_HANDLE, header(VALIDATION_DATASET_FILE_HANDLE_HEADER))
                .setHeader(FILE_TYPE, constant(FileType.NETEXPROFILE))
                .setHeader(CHOUETTE_REFERENTIAL, header(DATASET_REFERENTIAL))
                .process(e -> e.getIn().setHeader(FILE_NAME, getFileName(e.getIn().getHeader(VALIDATION_DATASET_FILE_HANDLE_HEADER, String.class))))
                .choice()
                .when(body().isEqualTo(constant(STATUS_VALIDATION_STARTED)))
                .to("direct:antuNetexValidationStarted")
                .when(body().isEqualTo(constant(STATUS_VALIDATION_OK)))
                .to("direct:antuNetexValidationComplete")
                .when(body().isEqualTo(constant(STATUS_VALIDATION_FAILED)))
                .to("direct:antuNetexValidationFailed")
                .routeId("antu-netex-validation-status");

        from("direct:antuNetexValidationStarted")
                .log(LoggingLevel.INFO, getClass().getName(), correlation() + "Antu NeTEx validation started for referential ${header." + DATASET_REFERENTIAL + "}")

                .choice()
                .when(header(VALIDATION_STAGE_HEADER).isEqualTo(VALIDATION_STAGE_PREVALIDATION))
                .process(e -> JobEvent.providerJobBuilder(e)
                        .timetableAction(JobEvent.TimetableAction.PREVALIDATION)
                        .state(JobEvent.State.STARTED)
                        .jobId(null)
                        .build())
                .when(header(VALIDATION_STAGE_HEADER).isEqualTo(VALIDATION_STAGE_EXPORT_NETEX_POSTVALIDATION))
                .process(e -> JobEvent.providerJobBuilder(e)
                        .timetableAction(JobEvent.TimetableAction.EXPORT_NETEX_POSTVALIDATION)
                        .state(JobEvent.State.STARTED)
                        .jobId(null)
                        .build())
                .when(header(VALIDATION_STAGE_HEADER).isEqualTo(VALIDATION_STAGE_EXPORT_NETEX_BLOCKS_POSTVALIDATION))
                .process(e -> JobEvent.providerJobBuilder(e)
                        .timetableAction(JobEvent.TimetableAction.EXPORT_NETEX_BLOCKS_POSTVALIDATION)
                        .state(JobEvent.State.STARTED)
                        .jobId(null)
                        .build())
                .when(header(VALIDATION_STAGE_HEADER).isEqualTo(VALIDATION_STAGE_FLEX_POSTVALIDATION))
                .process(e -> JobEvent.providerJobBuilder(e)
                        .timetableAction(JobEvent.TimetableAction.EXPORT_NETEX_POSTVALIDATION)
                        .state(JobEvent.State.STARTED)
                        .jobId(null)
                        .build())
                .when(header(VALIDATION_STAGE_HEADER).isEqualTo(VALIDATION_STAGE_EXPORT_MERGED_POSTVALIDATION))
                .process(e -> JobEvent.providerJobBuilder(e)
                        .timetableAction(JobEvent.TimetableAction.EXPORT_NETEX_MERGED_POSTVALIDATION)
                        .state(JobEvent.State.STARTED)
                        .jobId(null)
                        .build())
                .otherwise()
                .log(LoggingLevel.ERROR, getClass().getName(), correlation() + "Unknown validation stage ${header." + VALIDATION_STAGE_HEADER + "}")
                .stop()
                //end otherwise
                .end()
                // end choice
                .end()
                .to("direct:updateStatus")
                .routeId("antu-netex-validation-started");

        from("direct:antuNetexValidationComplete")
                .log(LoggingLevel.INFO, getClass().getName(), correlation() + "Antu NeTEx validation complete for referential ${header." + DATASET_REFERENTIAL + "}")
                .choice()

                .when(header(VALIDATION_STAGE_HEADER).isEqualTo(VALIDATION_STAGE_PREVALIDATION))
                .process(e -> JobEvent.providerJobBuilder(e).timetableAction(JobEvent.TimetableAction.PREVALIDATION).state(JobEvent.State.OK).build())
                .filter(PredicateBuilder.not(simple("{{chouette.enablePreValidation:false}}")))
                .log(LoggingLevel.INFO, correlation() + "Posting " + FILE_HANDLE + " ${header." + FILE_HANDLE + "} and " + FILE_TYPE + " ${header." + FILE_TYPE + "} on chouette import queue.")
                .to("google-pubsub:{{marduk.pubsub.project.id}}:ChouetteImportQueue")
                .endChoice()

                .when(header(VALIDATION_STAGE_HEADER).isEqualTo(VALIDATION_STAGE_EXPORT_NETEX_POSTVALIDATION))
                .process(e -> JobEvent.providerJobBuilder(e).timetableAction(JobEvent.TimetableAction.EXPORT_NETEX_POSTVALIDATION).state(JobEvent.State.OK).build())
                .filter(PredicateBuilder.not(simple("{{chouette.enablePostValidation:false}}")))
                .setHeader(TARGET_FILE_HANDLE, simple(BLOBSTORE_PATH_NETEX_EXPORT + "${header." + CHOUETTE_REFERENTIAL + "}-" + Constants.CURRENT_AGGREGATED_NETEX_FILENAME))
                .to("direct:copyInternalBlobInBucket")
                .to("google-pubsub:{{marduk.pubsub.project.id}}:ChouetteMergeWithFlexibleLinesQueue")
                .to("google-pubsub:{{marduk.pubsub.project.id}}:ChouetteExportNetexBlocksQueue")
                .endChoice()

                .when(header(VALIDATION_STAGE_HEADER).isEqualTo(VALIDATION_STAGE_EXPORT_NETEX_BLOCKS_POSTVALIDATION))
                .process(e -> JobEvent.providerJobBuilder(e).timetableAction(JobEvent.TimetableAction.EXPORT_NETEX_BLOCKS_POSTVALIDATION).state(JobEvent.State.OK).build())
                .filter(PredicateBuilder.not(simple("{{chouette.enablePostValidation:false}}")))
                .setHeader(TARGET_FILE_HANDLE, simple(Constants.BLOBSTORE_PATH_NETEX_BLOCKS_EXPORT + "${header." + CHOUETTE_REFERENTIAL + "}-" + Constants.CURRENT_AGGREGATED_NETEX_FILENAME))
                .to("direct:copyInternalBlobInBucket")
                .endChoice()

                .when(header(VALIDATION_STAGE_HEADER).isEqualTo(VALIDATION_STAGE_FLEX_POSTVALIDATION))
                .process(e -> JobEvent.providerJobBuilder(e).timetableAction(JobEvent.TimetableAction.EXPORT_NETEX_POSTVALIDATION).state(JobEvent.State.OK).build())
                .filter(header(VALIDATION_IMPORT_TYPE).isEqualTo(IMPORT_TYPE_NETEX_FLEX))
                    .setHeader(CHOUETTE_REFERENTIAL, simple("rb_${header." + CHOUETTE_REFERENTIAL + "}"))
                .end()
                .setHeader(TARGET_FILE_HANDLE, simple(Constants.BLOBSTORE_PATH_OUTBOUND + "netex/" + "${header." + CHOUETTE_REFERENTIAL + "}-" + Constants.CURRENT_FLEXIBLE_LINES_NETEX_FILENAME))
                .choice()
                // when importing a dataset uploaded from the operator portal, the original file was stored in
                // the internal bucket and must be copied to the exchange bucket
                .when(header(VALIDATION_IMPORT_TYPE).isEqualTo(IMPORT_TYPE_NETEX_FLEX))
                .setHeader(TARGET_CONTAINER, simple("${properties:blobstore.gcs.exchange.container.name}"))
                .to("direct:copyInternalBlobToAnotherBucket")
                // otherwise the original file comes from uttu and was stored in the inbound folder of the exchange bucket
                // and must be copied to the outbound folder in the exchange bucket
                .otherwise()
                .to("direct:copyExternalBlobInBucket")
                .end()
                .to("google-pubsub:{{marduk.pubsub.project.id}}:ChouetteMergeWithFlexibleLinesQueue")
                .endChoice()

                .when(header(VALIDATION_STAGE_HEADER).isEqualTo(VALIDATION_STAGE_EXPORT_MERGED_POSTVALIDATION))
                .process(e -> JobEvent.providerJobBuilder(e).timetableAction(JobEvent.TimetableAction.EXPORT_NETEX_MERGED_POSTVALIDATION).state(JobEvent.State.OK).build())
                .choice()
                .when(header(VALIDATION_IMPORT_TYPE).isEqualTo(IMPORT_TYPE_NETEX_FLEX))
                    .setHeader(CHOUETTE_REFERENTIAL, simple("rb_${header." + CHOUETTE_REFERENTIAL + "}"))
                .end()
                .setHeader(TARGET_FILE_HANDLE, simple(Constants.BLOBSTORE_PATH_OUTBOUND + "netex/" + "${header." + CHOUETTE_REFERENTIAL + "}-" + Constants.CURRENT_AGGREGATED_NETEX_FILENAME))
                .setHeader(TARGET_CONTAINER, simple("${properties:blobstore.gcs.container.name}"))
                .to("direct:copyInternalBlobToAnotherBucket")
                .to("google-pubsub:{{marduk.pubsub.project.id}}:PublishMergedNetexQueue")
                .endChoice()

                .otherwise()
                .log(LoggingLevel.ERROR, getClass().getName(), correlation() + "Unknown validation stage ${header." + VALIDATION_STAGE_HEADER + "}")
                .stop()

                //end otherwise
                .end()
                // end choice
                .end()
                .to("direct:updateStatus")
                .routeId("antu-netex-validation-complete");

        from("direct:antuNetexValidationFailed")
                .log(LoggingLevel.INFO, getClass().getName(), correlation() + "Antu NeTEx validation failed for referential ${header." + DATASET_REFERENTIAL + "}")
                .choice()
                .when(header(VALIDATION_STAGE_HEADER).isEqualTo(VALIDATION_STAGE_PREVALIDATION))
                .process(e -> JobEvent.providerJobBuilder(e).timetableAction(JobEvent.TimetableAction.PREVALIDATION).state(JobEvent.State.FAILED).build())
                .when(header(VALIDATION_STAGE_HEADER).isEqualTo(VALIDATION_STAGE_EXPORT_NETEX_POSTVALIDATION))
                .process(e -> JobEvent.providerJobBuilder(e).timetableAction(JobEvent.TimetableAction.EXPORT_NETEX_POSTVALIDATION).state(JobEvent.State.FAILED).build())
                .when(header(VALIDATION_STAGE_HEADER).isEqualTo(VALIDATION_STAGE_EXPORT_NETEX_BLOCKS_POSTVALIDATION))
                .process(e -> JobEvent.providerJobBuilder(e).timetableAction(JobEvent.TimetableAction.EXPORT_NETEX_BLOCKS_POSTVALIDATION).state(JobEvent.State.FAILED).build())
                .when(header(VALIDATION_STAGE_HEADER).isEqualTo(VALIDATION_STAGE_FLEX_POSTVALIDATION))
                .process(e -> JobEvent.providerJobBuilder(e).timetableAction(JobEvent.TimetableAction.EXPORT_NETEX_POSTVALIDATION).state(JobEvent.State.FAILED).build())
                .when(header(VALIDATION_STAGE_HEADER).isEqualTo(VALIDATION_STAGE_EXPORT_MERGED_POSTVALIDATION))
                .process(e -> JobEvent.providerJobBuilder(e).timetableAction(JobEvent.TimetableAction.EXPORT_NETEX_MERGED_POSTVALIDATION).state(JobEvent.State.FAILED).build())
                .otherwise()
                .log(LoggingLevel.ERROR, getClass().getName(), correlation() + "Unknown validation stage ${header." + VALIDATION_STAGE_HEADER + "}")
                //end otherwise
                .end()
                // end choice
                .end()
                .to("direct:updateStatus")
                .routeId("antu-netex-validation-failed");

    }

    /**
     * Extract the NeTEx file name from the NeTEx file path.
     *
     * @param filePath the Netex file path.
     * @return the NeTEx file name.
     */
    private String getFileName(String filePath) {
        return filePath.substring(filePath.lastIndexOf('/') + 1);
    }
}
