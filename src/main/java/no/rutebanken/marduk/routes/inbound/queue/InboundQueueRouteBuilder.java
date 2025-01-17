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

package no.rutebanken.marduk.routes.inbound.queue;

import no.rutebanken.marduk.Constants;
import no.rutebanken.marduk.routes.BaseRouteBuilder;
import no.rutebanken.marduk.routes.file.beans.FileTypeClassifierBean;
import no.rutebanken.marduk.routes.status.JobEvent;
import org.apache.camel.Exchange;
import org.apache.camel.LoggingLevel;
import org.apache.camel.ValidationException;
import org.springframework.stereotype.Component;

import static no.rutebanken.marduk.Constants.CHOUETTE_REFERENTIAL;
import static no.rutebanken.marduk.Constants.FILE_HANDLE;
import static no.rutebanken.marduk.Constants.PROVIDER_ID;

/**
 * Receives file notification from "external" queue and uses this to download the file from blob store.
 */
@Component
public class InboundQueueRouteBuilder extends BaseRouteBuilder {

    @Override
    public void configure() throws Exception {
        super.configure();

        onException(ValidationException.class)
                .handled(true)
                .log(LoggingLevel.INFO, correlation() + "Could not process file ${header." + FILE_HANDLE + "}")
                .process(e -> JobEvent.providerJobBuilder(e).timetableAction(JobEvent.TimetableAction.FILE_CLASSIFICATION).state(JobEvent.State.FAILED).build())
                .to("direct:updateStatus")
                .setBody(simple(""))      //remove file data from body
                .to("google-pubsub:{{marduk.pubsub.project.id}}:MardukDeadLetterQueue");


        from("google-pubsub:{{marduk.pubsub.project.id}}:MardukInboundQueue")
                .streamCache("true")
                .setHeader(Exchange.FILE_NAME, header(Constants.FILE_NAME))
                .log(LoggingLevel.INFO, correlation() + "Received notification about file '${header." + Constants.FILE_NAME + "}' on inbound queue. Fetching file ...")
                .log(LoggingLevel.INFO, correlation() + "Fetching blob ${header." + FILE_HANDLE + "}")
                .to("direct:fetchExternalBlob")
                .process(e -> e.getIn().setHeader(CHOUETTE_REFERENTIAL, getProviderRepository().getProvider(e.getIn().getHeader(PROVIDER_ID, Long.class)).getChouetteInfo().getReferential()))
                .convertBodyTo(byte[].class)
                .validate().method(FileTypeClassifierBean.class, "validateFile")
                .log(LoggingLevel.INFO, correlation() + "File handle is: ${header." + FILE_HANDLE + "}")
                .to(logDebugShowAll())
                .to("direct:uploadInternalBlob")
                .to(logDebugShowAll())
                .process(e -> JobEvent.providerJobBuilder(e).timetableAction(JobEvent.TimetableAction.FILE_TRANSFER).state(JobEvent.State.STARTED).build())
                .to("direct:updateStatus")
                .choice()
                .when(simple("{{blobstore.delete.external.blobs:true}}"))
                .to("direct:deleteExternalBlob")
                .end()
                .to("google-pubsub:{{marduk.pubsub.project.id}}:ProcessFileQueue")
                .routeId("inbound-queue");

    }

}
