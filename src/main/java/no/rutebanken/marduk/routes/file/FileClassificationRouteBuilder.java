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

package no.rutebanken.marduk.routes.file;


import no.rutebanken.marduk.Constants;
import no.rutebanken.marduk.domain.Provider;
import no.rutebanken.marduk.routes.BaseRouteBuilder;
import no.rutebanken.marduk.routes.file.beans.FileTypeClassifierBean;
import no.rutebanken.marduk.routes.status.JobEvent;
import org.apache.camel.LoggingLevel;
import org.apache.camel.ValidationException;
import org.apache.camel.builder.PredicateBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Locale;

import static no.rutebanken.marduk.Constants.*;

/**
 * Receives file handle, pulls file from blob store, classifies files and performs initial validation.
 */
@Component
public class FileClassificationRouteBuilder extends BaseRouteBuilder {

    private final List<String> swedishCodespaces;
    private final List<String> finnishCodespaces;

    public FileClassificationRouteBuilder(
            @Value("${antu.validation.sweden.codespaces:}") List<String> swedishCodespaces,
            @Value("${antu.validation.finland.codespaces:OYM}")List<String> finnishCodespaces) {
        this.swedishCodespaces = swedishCodespaces;
        this.finnishCodespaces = finnishCodespaces;
    }

    @Override
    public void configure() throws Exception {
        super.configure();

        onException(ValidationException.class)
                .handled(true)
                .log(LoggingLevel.INFO, correlation() + "Could not process file ${header." + FILE_HANDLE + "}")
                .process(e -> JobEvent.providerJobBuilder(e).timetableAction(JobEvent.TimetableAction.FILE_CLASSIFICATION).state(JobEvent.State.FAILED).build())
                .to("direct:updateStatus")
                .setBody(constant(""))
                .to("google-pubsub:{{marduk.pubsub.project.id}}:MardukDeadLetterQueue");

        from("google-pubsub:{{marduk.pubsub.project.id}}:ProcessFileQueue")
                .process(e -> JobEvent.providerJobBuilder(e).timetableAction(JobEvent.TimetableAction.FILE_TRANSFER).state(JobEvent.State.OK).build())
                .to("direct:updateStatus")
                .process(e -> JobEvent.providerJobBuilder(e).timetableAction(JobEvent.TimetableAction.FILE_CLASSIFICATION).state(JobEvent.State.STARTED).build()).to("direct:updateStatus")
                .to("direct:getInternalBlob")
                .convertBodyTo(byte[].class)
                .validate().method(FileTypeClassifierBean.class, "validateFile")
                .choice()

                .when(header(FILE_TYPE).isEqualTo(FileType.UNKNOWN_FILE_EXTENSION.name()))
                .log(LoggingLevel.WARN, correlation() + "The file ${header." + FILE_HANDLE + "} does not end with a .zip or .ZIP extension")
                .process(e -> e.getIn().setHeader(Constants.JOB_ERROR_CODE, JobEvent.JOB_ERROR_FILE_UNKNOWN_FILE_EXTENSION))
                .process(e -> JobEvent.providerJobBuilder(e).timetableAction(JobEvent.TimetableAction.FILE_CLASSIFICATION).state(JobEvent.State.FAILED).build())
                .to("direct:updateStatus")
                .stop()

                .when(header(FILE_TYPE).isEqualTo(FileType.UNKNOWN_FILE_TYPE.name()))
                .log(LoggingLevel.WARN, correlation() + "The file ${header." + FILE_HANDLE + "} cannot be processed: unknown file type")
                .process(e -> e.getIn().setHeader(Constants.JOB_ERROR_CODE, JobEvent.JOB_ERROR_UNKNOWN_FILE_TYPE))
                .process(e -> JobEvent.providerJobBuilder(e).timetableAction(JobEvent.TimetableAction.FILE_CLASSIFICATION).state(JobEvent.State.FAILED).build())
                .to("direct:updateStatus")
                .stop()

                .when(header(FILE_TYPE).isEqualTo(FileType.NOT_A_ZIP_FILE.name()))
                .log(LoggingLevel.WARN, correlation() + "The file ${header." + FILE_HANDLE + "} is not a valid zip archive")
                .process(e -> e.getIn().setHeader(Constants.JOB_ERROR_CODE, JobEvent.JOB_ERROR_FILE_NOT_A_ZIP_FILE))
                .process(e -> JobEvent.providerJobBuilder(e).timetableAction(JobEvent.TimetableAction.FILE_CLASSIFICATION).state(JobEvent.State.FAILED).build())
                .to("direct:updateStatus")
                .stop()

                .when(header(FILE_TYPE).isEqualTo(FileType.ZIP_CONTAINS_SUBDIRECTORIES.name()))
                .log(LoggingLevel.WARN, correlation() + "The file ${header." + FILE_HANDLE + "} contains one or more subdirectories")
                .process(e -> e.getIn().setHeader(Constants.JOB_ERROR_CODE, JobEvent.JOB_ERROR_FILE_ZIP_CONTAINS_SUB_DIRECTORIES))
                .process(e -> JobEvent.providerJobBuilder(e).timetableAction(JobEvent.TimetableAction.FILE_CLASSIFICATION).state(JobEvent.State.FAILED).build())
                .to("direct:updateStatus")
                .stop()

                .when(header(FILE_TYPE).isEqualTo(FileType.INVALID_ZIP_FILE_ENTRY_CONTENT_ENCODING.name()))
                .log(LoggingLevel.WARN, correlation() + "The file ${header." + FILE_HANDLE + "} contains one or more invalid XML files: invalid encoding")
                .process(e -> e.getIn().setHeader(Constants.JOB_ERROR_CODE, JobEvent.JOB_ERROR_INVALID_XML_ENCODING))
                .process(e -> JobEvent.providerJobBuilder(e).timetableAction(JobEvent.TimetableAction.FILE_CLASSIFICATION).state(JobEvent.State.FAILED).build())
                .to("direct:updateStatus")
                .stop()

                .when(header(FILE_TYPE).isEqualTo(FileType.INVALID_ZIP_FILE_ENTRY_XML_CONTENT.name()))
                .log(LoggingLevel.WARN, correlation() + "The file ${header." + FILE_HANDLE + "} contains one or more invalid XML files: unparseable XML")
                .process(e -> e.getIn().setHeader(Constants.JOB_ERROR_CODE, JobEvent.JOB_ERROR_INVALID_XML_CONTENT))
                .process(e -> JobEvent.providerJobBuilder(e).timetableAction(JobEvent.TimetableAction.FILE_CLASSIFICATION).state(JobEvent.State.FAILED).build())
                .to("direct:updateStatus")
                .stop()

                .when(header(FILE_TYPE).isEqualTo(FileType.INVALID_ZIP_FILE_ENTRY_NAME_ENCODING.name()))
                .log(LoggingLevel.WARN, correlation() + "The file ${header." + FILE_HANDLE + "} contains one or more invalid zip entry names: invalid encoding")
                .process(e -> e.getIn().setHeader(Constants.JOB_ERROR_CODE, JobEvent.JOB_ERROR_INVALID_ZIP_ENTRY_ENCODING))
                .process(e -> JobEvent.providerJobBuilder(e).timetableAction(JobEvent.TimetableAction.FILE_CLASSIFICATION).state(JobEvent.State.FAILED).build())
                .to("direct:updateStatus")
                .stop()

                .when(header(FILE_TYPE).isEqualTo(FileType.INVALID_FILE_NAME.name()))
                .log(LoggingLevel.WARN, correlation() + "File with invalid characters in file name ${header." + FILE_HANDLE + "}")
                .to("direct:sanitizeFileName")

                .otherwise()
                .to("direct:processValidFile")

                .routeId("file-classify");

        from("direct:sanitizeFileName")
                .process(e -> {
                    String originalFileName = e.getIn().getHeader(Constants.FILE_NAME, String.class);
                    String sanitizedFileName = MardukFileUtils.sanitizeFileName(originalFileName);
                    e.getIn().setHeader(FILE_HANDLE, Constants.BLOBSTORE_PATH_INBOUND
                            + getProviderRepository().getReferential(e.getIn().getHeader(PROVIDER_ID, Long.class))
                            + "/" + sanitizedFileName);
                    e.getIn().setHeader(FILE_NAME, sanitizedFileName);
                })
                .log(LoggingLevel.INFO, correlation() + "Uploading file with new file name ${header." + FILE_HANDLE + "}")
                .to("direct:uploadInternalBlob")
                .to("google-pubsub:{{marduk.pubsub.project.id}}:ProcessFileQueue")
                .routeId("file-sanitize-filename");

        from("direct:processValidFile")
                .setBody(constant(""))
                .process(e -> JobEvent.providerJobBuilder(e).timetableAction(JobEvent.TimetableAction.FILE_CLASSIFICATION).state(JobEvent.State.OK).build())
                .to("direct:updateStatus")
                .setBody(constant(""))
                .choice()
                .when(header(IMPORT_TYPE).isEqualTo(IMPORT_TYPE_NETEX_FLEX))
                .to("direct:flexibleLinesImport")
                .otherwise()
                .to("direct:antuNetexPreValidation")
                // launch the import process if this is a GTFS file or if the pre-validation is activated in chouette
                .filter(PredicateBuilder.or(simple("{{chouette.enablePreValidation:false}}"), header(FILE_TYPE).isEqualTo(FileType.GTFS)))
                .log(LoggingLevel.INFO, correlation() + "Posting " + FILE_HANDLE + " ${header." + FILE_HANDLE + "} and " + FILE_TYPE + " ${header." + FILE_TYPE + "} on chouette import queue.")
                .to("google-pubsub:{{marduk.pubsub.project.id}}:ChouetteImportQueue")
                .routeId("process-valid-file");

        from("direct:antuNetexPreValidation")
                .filter(header(FILE_TYPE).isEqualTo(FileType.NETEXPROFILE))
                .to("direct:copyInternalBlobToValidationBucket")
                .process(e -> {
                    Provider provider = getProviderRepository().getProvider(e.getIn().getHeader(PROVIDER_ID, Long.class));
                    e.getIn().setHeader(DATASET_REFERENTIAL, provider.getChouetteInfo().getReferential());
                })
                .setHeader(VALIDATION_DATASET_FILE_HANDLE_HEADER, header(FILE_HANDLE))
                .setHeader(VALIDATION_CORRELATION_ID_HEADER, header(CORRELATION_ID))
                .setHeader(VALIDATION_CLIENT_HEADER, constant(VALIDATION_CLIENT_MARDUK))
                .setHeader(VALIDATION_STAGE_HEADER, constant(VALIDATION_STAGE_PREVALIDATION))

                .to("direct:setNetexValidationProfile")
                .to("google-pubsub:{{antu.pubsub.project.id}}:AntuNetexValidationQueue")
                .process(e -> JobEvent.providerJobBuilder(e).timetableAction(JobEvent.TimetableAction.PREVALIDATION).state(JobEvent.State.PENDING).build())
                .to("direct:updateStatus")
                .routeId("antu-netex-pre-validation");

        from("direct:setNetexValidationProfile")
                .choice()
                .when(e -> isSwedishReferential(e.getIn().getHeader(DATASET_REFERENTIAL, String.class)))
                .log(LoggingLevel.INFO, correlation() + "Applying validation rules for Timetable data/Sweden")
                .setHeader(VALIDATION_PROFILE_HEADER, constant(VALIDATION_PROFILE_TIMETABLE_SWEDEN))
                .when(e -> isFinishReferential(e.getIn().getHeader(DATASET_REFERENTIAL, String.class)))
                .log(LoggingLevel.INFO, correlation() + "Applying validation rules for Timetable data/Finland")
                .setHeader(VALIDATION_PROFILE_HEADER, constant(VALIDATION_PROFILE_TIMETABLE_FINLAND))
                .otherwise()
                .log(LoggingLevel.INFO, correlation() + "Applying validation rules for Timetable data/Norway")
                .setHeader(VALIDATION_PROFILE_HEADER, constant(VALIDATION_PROFILE_TIMETABLE))
                .end()
                .routeId("set-netex-validation-profile");

    }

    private boolean isSwedishReferential(String referential) {
        String codespace = referential.replace("rb_", "").toUpperCase(Locale.ROOT);
        return swedishCodespaces.contains(codespace);
    }

    private boolean isFinishReferential(String referential) {
        String codespace = referential.replace("rb_", "").toUpperCase(Locale.ROOT);
        return finnishCodespaces.contains(codespace);
    }

}
