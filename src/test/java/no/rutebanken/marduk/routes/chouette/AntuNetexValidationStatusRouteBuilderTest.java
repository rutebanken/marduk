package no.rutebanken.marduk.routes.chouette;

import no.rutebanken.marduk.MardukRouteBuilderIntegrationTestBase;
import no.rutebanken.marduk.routes.status.JobEvent;
import org.apache.camel.*;
import org.apache.camel.builder.AdviceWith;
import org.apache.camel.component.mock.MockEndpoint;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static no.rutebanken.marduk.Constants.*;
import static no.rutebanken.marduk.routes.chouette.AntuNetexValidationStatusRouteBuilder.STATUS_VALIDATION_FAILED;
import static no.rutebanken.marduk.routes.chouette.AntuNetexValidationStatusRouteBuilder.STATUS_VALIDATION_OK;
import static org.junit.jupiter.api.Assertions.*;

class AntuNetexValidationStatusRouteBuilderTest extends MardukRouteBuilderIntegrationTestBase {

    @Produce("google-pubsub:{{marduk.pubsub.project.id}}:AntuNetexValidationStatusQueue")
    protected ProducerTemplate importTemplate;

    @EndpointInject("mock:updateStatus")
    protected MockEndpoint updateStatus;

    @EndpointInject("mock:copyInternalBlobToAnotherBucket")
    protected MockEndpoint copyInternalBlobToAnotherBucket;

    @EndpointInject("mock:copyExternalBlobInBucket")
    protected MockEndpoint copyExternalBlobInBucket;

    @EndpointInject("mock:copyBlobInBucket")
    protected MockEndpoint copyBlobInBucketMock;

    @EndpointInject("mock:chouetteMergeWithFlexibleLinesQueue")
    protected MockEndpoint chouetteMergeWithFlexibleLinesQueueMock;

    @EndpointInject("mock:publishMergedNetexQueue")
    protected MockEndpoint publishMergedNetexQueueMock;

    @Override
    @BeforeEach
    protected void setUp() throws IOException {
        super.setUp();
        updateStatus.reset();
        copyInternalBlobToAnotherBucket.reset();
        copyExternalBlobInBucket.reset();
        chouetteMergeWithFlexibleLinesQueueMock.reset();
    }

    @Test
    void testAntuStatusValidationOk() throws Exception {

        AdviceWith.adviceWith(context, "antu-netex-validation-complete", a -> {
            a.interceptSendToEndpoint("direct:updateStatus")
                    .skipSendToOriginalEndpoint()
                    .to("mock:updateStatus");
            a.interceptSendToEndpoint("direct:copyExternalBlobInBucket")
                    .skipSendToOriginalEndpoint()
                    .to("mock:copyExternalBlobInBucket");

            a.weaveByToUri("google-pubsub:(.*):ChouetteMergeWithFlexibleLinesQueue")
                    .replace()
                    .to("mock:chouetteMergeWithFlexibleLinesQueue");
        });


        // we must manually start when we are done with all the advice with
        context.start();

        updateStatus.expectedMessageCount(1);
        copyExternalBlobInBucket.expectedMessageCount(1);
        chouetteMergeWithFlexibleLinesQueueMock.expectedMessageCount(1);


        Map<String, String> headers = new HashMap<>();
        headers.put(VALIDATION_STAGE_HEADER, VALIDATION_STAGE_FLEX_POSTVALIDATION);
        headers.put(VALIDATION_DATASET_FILE_HANDLE_HEADER, "testFileName");
        headers.put(VALIDATION_CORRELATION_ID_HEADER, "testCorrelationId");
        sendBodyAndHeadersToPubSub(importTemplate, STATUS_VALIDATION_OK, headers);

        updateStatus.assertIsSatisfied();
        copyExternalBlobInBucket.assertIsSatisfied();
        chouetteMergeWithFlexibleLinesQueueMock.assertIsSatisfied();

        List<JobEvent> events = updateStatus.getExchanges().stream().map(e -> JobEvent.fromString(e.getIn().getBody().toString())).toList();
        assertTrue(events.stream().anyMatch(je -> JobEvent.JobDomain.TIMETABLE.equals(je.getDomain())
                && JobEvent.TimetableAction.EXPORT_NETEX_POSTVALIDATION.name().equals(je.getAction())
                && JobEvent.State.OK.equals(je.getState())));
    }

    @Test
    void testAntuStatusValidationFailedShouldStopMergeWithFlexibleLine() throws Exception {

        AdviceWith.adviceWith(context, "antu-netex-validation-failed", a -> {
            a.interceptSendToEndpoint("direct:updateStatus")
                    .skipSendToOriginalEndpoint()
                    .to("mock:updateStatus");
            a.interceptSendToEndpoint("direct:copyBlobToAnotherBucket")
                    .skipSendToOriginalEndpoint()
                    .to("mock:copyBlobToAnotherBucket");
            a.interceptSendToEndpoint("google-pubsub:(.*):ChouetteMergeWithFlexibleLinesQueue")
                    .skipSendToOriginalEndpoint()
                    .to("mock:chouetteMergeWithFlexibleLinesQueue");
        });

        // we must manually start when we are done with all the advice with
        context.start();

        updateStatus.expectedMessageCount(1);
        copyInternalBlobToAnotherBucket.expectedMessageCount(0);
        chouetteMergeWithFlexibleLinesQueueMock.expectedMessageCount(0);

        Map<String, String> headers = new HashMap<>();
        headers.put(VALIDATION_STAGE_HEADER, VALIDATION_STAGE_FLEX_POSTVALIDATION);
        headers.put(VALIDATION_DATASET_FILE_HANDLE_HEADER, "testFileName");
        headers.put(VALIDATION_CORRELATION_ID_HEADER, "testCorrelationId");
        sendBodyAndHeadersToPubSub(importTemplate, STATUS_VALIDATION_FAILED, headers);

        updateStatus.assertIsSatisfied();
        copyInternalBlobToAnotherBucket.assertIsSatisfied();
        chouetteMergeWithFlexibleLinesQueueMock.assertIsSatisfied();

        List<JobEvent> events = updateStatus.getExchanges().stream().map(e -> JobEvent.fromString(e.getIn().getBody().toString())).toList();
        assertTrue(events.stream().anyMatch(je -> JobEvent.JobDomain.TIMETABLE.equals(je.getDomain())
                && JobEvent.TimetableAction.EXPORT_NETEX_POSTVALIDATION.name().equals(je.getAction())
                && JobEvent.State.FAILED.equals(je.getState())));
    }

    @Test
    void testAntuFailedValidationShouldStopPublishMergedNetexQueue() throws Exception {

        // Mock update status calls
        AdviceWith.adviceWith(context, "antu-netex-validation-failed", a -> {
            a.interceptSendToEndpoint("direct:updateStatus")
                    .skipSendToOriginalEndpoint()
                    .to("mock:updateStatus");
            a.interceptSendToEndpoint("direct:copyBlobInBucket")
                    .skipSendToOriginalEndpoint()
                    .to("mock:copyBlobInBucket");
            a.interceptSendToEndpoint("google-pubsub:(.*):PublishMergedNetexQueue")
                    .skipSendToOriginalEndpoint()
                    .to("mock:publishMergedNetexQueue");
        });

        // we must manually start when we are done with all the advice with
        context.start();

        updateStatus.expectedMessageCount(1);
        copyBlobInBucketMock.expectedMessageCount(0);
        publishMergedNetexQueueMock.expectedMessageCount(0);

        Map<String, String> headers = new HashMap<>();
        headers.put(VALIDATION_STAGE_HEADER, VALIDATION_STAGE_EXPORT_MERGED_POSTVALIDATION);
        headers.put(VALIDATION_DATASET_FILE_HANDLE_HEADER, "testFileName");
        headers.put(VALIDATION_CORRELATION_ID_HEADER, "testCorrelationId");
        sendBodyAndHeadersToPubSub(importTemplate, STATUS_VALIDATION_FAILED, headers);

        updateStatus.assertIsSatisfied();
        copyBlobInBucketMock.assertIsSatisfied();
        publishMergedNetexQueueMock.assertIsSatisfied();

        List<JobEvent> events = updateStatus.getExchanges().stream().map(e -> JobEvent.fromString(e.getIn().getBody().toString())).toList();
        assertTrue(events.stream().anyMatch(je -> JobEvent.JobDomain.TIMETABLE.equals(je.getDomain())
                && JobEvent.TimetableAction.EXPORT_NETEX_MERGED_POSTVALIDATION.name().equals(je.getAction())
                && JobEvent.State.FAILED.equals(je.getState())));
    }
}