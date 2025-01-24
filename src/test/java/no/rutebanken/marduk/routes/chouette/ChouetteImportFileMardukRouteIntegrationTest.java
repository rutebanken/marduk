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
import no.rutebanken.marduk.MardukRouteBuilderIntegrationTestBase;
import no.rutebanken.marduk.TestConstants;
import no.rutebanken.marduk.routes.status.JobEvent;
import org.apache.camel.EndpointInject;
import org.apache.camel.Exchange;
import org.apache.camel.Expression;
import org.apache.camel.Produce;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.builder.AdviceWith;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.model.language.SimpleExpression;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Value;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertTrue;


class ChouetteImportFileMardukRouteIntegrationTest extends MardukRouteBuilderIntegrationTestBase {

    @EndpointInject("mock:chouetteCreateImport")
    protected MockEndpoint chouetteCreateImport;

    @EndpointInject("mock:pollJobStatus")
    protected MockEndpoint pollJobStatus;

    @EndpointInject("mock:chouetteGetJobsForProvider")
    protected MockEndpoint chouetteGetJobs;

    @EndpointInject("mock:processImportResult")
    protected MockEndpoint processActionReportResult;

    @EndpointInject("mock:chouetteValidationQueue")
    protected MockEndpoint chouetteValidationQueue;

    @EndpointInject("mock:checkScheduledJobsBeforeTriggeringNextAction")
    protected MockEndpoint checkScheduledJobsBeforeTriggeringNextAction;

    @EndpointInject("mock:updateStatus")
    protected MockEndpoint updateStatus;

    @Produce("google-pubsub:{{marduk.pubsub.project.id}}:ProcessFileQueue")
    protected ProducerTemplate importTemplate;

    @Produce("direct:processImportResult")
    protected ProducerTemplate processImportResultTemplate;

    @Produce("direct:checkScheduledJobsBeforeTriggeringNextAction")
    protected ProducerTemplate triggerJobListTemplate;

    @Value("${chouette.url}")
    private String chouetteUrl;

    @Override
    @BeforeEach
    protected void setUp() throws IOException {
        super.setUp();
        chouetteCreateImport.reset();
        pollJobStatus.reset();
        chouetteGetJobs.reset();
        processActionReportResult.reset();
        chouetteValidationQueue.reset();
        checkScheduledJobsBeforeTriggeringNextAction.reset();
        updateStatus.reset();
    }

    @Test
    void testImportFileToDataspace() throws Exception {

        AdviceWith.adviceWith(context, "file-classify", a -> a.interceptSendToEndpoint("direct:updateStatus").skipSendToOriginalEndpoint()
                .to("mock:updateStatus"));

        String testFilename = "netex.zip";
        InputStream testFile = getTestNetexArchiveAsStream();

        //populate fake blob repo
        internalInMemoryBlobStoreRepository.uploadBlob("rut/" + testFilename, testFile);

        // Mock initial call to Chouette to import job
        AdviceWith.adviceWith(context, "chouette-send-import-job", a -> a.interceptSendToEndpoint(chouetteUrl + "/chouette_iev/referentials/rut/importer/netexprofile")
                .skipSendToOriginalEndpoint().to("mock:chouetteCreateImport"));

        // Mock job polling route - AFTER header validatio (to ensure that we send correct headers in test as well
        AdviceWith.adviceWith(context, "chouette-validate-job-status-parameters", a -> a.interceptSendToEndpoint("direct:checkJobStatus").skipSendToOriginalEndpoint()
                .to("mock:pollJobStatus"));

        // Mock update status calls
        AdviceWith.adviceWith(context, "chouette-process-import-status", a -> {
            a.weaveByToUri("direct:updateStatus").replace().to("mock:updateStatus");
            a.weaveByToUri("direct:checkScheduledJobsBeforeTriggeringNextAction").replace().to("mock:checkScheduledJobsBeforeTriggeringNextAction");
            a.interceptSendToEndpoint("direct:copyOriginalDataset")
                    .skipSendToOriginalEndpoint()
                    .to("mock:sink");
        });

        pollJobStatus.expectedMessageCount(1);
        updateStatus.expectedMessageCount(6);
        checkScheduledJobsBeforeTriggeringNextAction.expectedMessageCount(1);
        // 1 initial import call
        chouetteCreateImport.expectedMessageCount(1);
        chouetteCreateImport.returnReplyHeader("Location", new SimpleExpression(
                chouetteUrl.replace("http:", "http://") + "/chouette_iev/referentials/rut/scheduled_jobs/1"));

        // we must manually start when we are done with all the advice with
        context.start();

        Map<String, String> headers = new HashMap<>();
        headers.put(Constants.PROVIDER_ID, TestConstants.PROVIDER_ID_AS_STRING_RUT);
        headers.put(Constants.FILE_NAME, testFilename);
        headers.put(Constants.CORRELATION_ID, "corr_id");
        headers.put(Constants.FILE_HANDLE, "rut/" + testFilename);
        sendBodyAndHeadersToPubSub(importTemplate, "", headers);

        chouetteCreateImport.assertIsSatisfied();
        pollJobStatus.assertIsSatisfied();

        Exchange exchange = pollJobStatus.getReceivedExchanges().getFirst();
        exchange.getIn().setHeader("action_report_result", "OK");
        exchange.getIn().setHeader("validation_report_result", "OK");
        processImportResultTemplate.send(exchange);

        checkScheduledJobsBeforeTriggeringNextAction.assertIsSatisfied();

        updateStatus.assertIsSatisfied();
        List<JobEvent> events = updateStatus.getExchanges().stream().map(e -> JobEvent.fromString(e.getIn().getBody().toString())).toList();
        assertTrue(events.stream().anyMatch(je -> JobEvent.JobDomain.TIMETABLE.equals(je.getDomain())
                && JobEvent.TimetableAction.FILE_TRANSFER.name().equals(je.getAction())
                && JobEvent.State.OK.equals(je.getState())));

        assertTrue(events.stream().anyMatch(je -> JobEvent.JobDomain.TIMETABLE.equals(je.getDomain())
                && JobEvent.TimetableAction.FILE_CLASSIFICATION.name().equals(je.getAction())
                && JobEvent.State.STARTED.equals(je.getState())));

        assertTrue(events.stream().anyMatch(je -> JobEvent.JobDomain.TIMETABLE.equals(je.getDomain())
                && JobEvent.TimetableAction.FILE_CLASSIFICATION.name().equals(je.getAction())
                && JobEvent.State.OK.equals(je.getState())));

        assertTrue(events.stream().anyMatch(je -> JobEvent.JobDomain.TIMETABLE.equals(je.getDomain())
                && JobEvent.TimetableAction.IMPORT.name().equals(je.getAction())
                && JobEvent.State.PENDING.equals(je.getState())));

        assertTrue(events.stream().anyMatch(je -> JobEvent.JobDomain.TIMETABLE.equals(je.getDomain())
                && JobEvent.TimetableAction.IMPORT.name().equals(je.getAction())
                && JobEvent.State.OK.equals(je.getState())));


    }


    @Test
    void testJobListResponseTerminated() throws Exception {
        testJobListResponse("/no/rutebanken/marduk/chouette/getJobListResponseAllTerminated.json", true);
    }

    @Test
    void testJobListResponseScheduled() throws Exception {
        testJobListResponse("/no/rutebanken/marduk/chouette/getJobListResponseScheduled.json", false);
    }

    void testJobListResponse(String jobListResponseClasspathReference, boolean expectExport) throws Exception {

        AdviceWith.adviceWith(context, "chouette-process-job-list-after-import", a -> {
            a.interceptSendToEndpoint(chouetteUrl + "/*")
                    .skipSendToOriginalEndpoint()
                    .to("mock:chouetteGetJobsForProvider");

            a.weaveByToUri("google-pubsub:(.*):ChouetteValidationQueue").replace().to("mock:chouetteValidationQueue");

        });

        context.start();

        // 1 call to list other import jobs in referential
        chouetteGetJobs.expectedMessageCount(1);
        chouetteGetJobs.returnReplyBody(new Expression() {

            @SuppressWarnings("unchecked")
            @Override
            public <T> T evaluate(Exchange ex, Class<T> arg1) {
                try {
                    return (T) IOUtils.toString(getClass().getResourceAsStream(jobListResponseClasspathReference), StandardCharsets.UTF_8);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        Map<String, Object> headers = new HashMap<>();
        headers.put(Constants.PROVIDER_ID, TestConstants.PROVIDER_ID_AS_STRING_RUT);
        headers.put(Constants.CHOUETTE_REFERENTIAL, TestConstants.CHOUETTE_REFERENTIAL_RUT);

        triggerJobListTemplate.sendBodyAndHeaders(null, headers);

        chouetteGetJobs.assertIsSatisfied();

        if (expectExport) {
            chouetteValidationQueue.expectedMessageCount(1);
        }
        chouetteValidationQueue.assertIsSatisfied();

    }

}