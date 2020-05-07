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

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import org.apache.camel.EndpointInject;
import org.apache.camel.Exchange;
import org.apache.camel.Expression;
import org.apache.camel.Produce;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.builder.AdviceWithRouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.model.language.SimpleExpression;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;

import no.rutebanken.marduk.Constants;
import no.rutebanken.marduk.MardukRouteBuilderIntegrationTestBase;
import no.rutebanken.marduk.repository.InMemoryBlobStoreRepository;
import no.rutebanken.marduk.routes.file.ZipFileUtils;
import no.rutebanken.marduk.TestApp;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, classes = TestApp.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
public class ChouetteImportFileMardukRouteIntegrationTest extends MardukRouteBuilderIntegrationTestBase {

    @Autowired
    private InMemoryBlobStoreRepository inMemoryBlobStoreRepository;

    @EndpointInject(uri = "mock:chouetteCreateImport")
    protected MockEndpoint chouetteCreateImport;

    @EndpointInject(uri = "mock:pollJobStatus")
    protected MockEndpoint pollJobStatus;

    @EndpointInject(uri = "mock:chouetteGetJobsForProvider")
    protected MockEndpoint chouetteGetJobs;

    @EndpointInject(uri = "mock:processImportResult")
    protected MockEndpoint processActionReportResult;

    @EndpointInject(uri = "mock:chouetteValidationQueue")
    protected MockEndpoint chouetteValidationQueue;

    @EndpointInject(uri = "mock:checkScheduledJobsBeforeTriggeringNextAction")
    protected MockEndpoint checkScheduledJobsBeforeTriggeringNextAction;

    @EndpointInject(uri = "mock:updateStatus")
    protected MockEndpoint updateStatus;

    @Produce(uri = "entur-google-pubsub:ProcessFileQueue")
    protected ProducerTemplate importTemplate;

    @Produce(uri = "direct:processImportResult")
    protected ProducerTemplate processImportResultTemplate;

    @Produce(uri = "direct:checkScheduledJobsBeforeTriggeringNextAction")
    protected ProducerTemplate triggerJobListTemplate;

    @Value("${chouette.url}")
    private String chouetteUrl;

    @BeforeEach
    public void setUp() throws IOException {
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
    public void testImportFileToDataspace() throws Exception {

        String testFilename = "netex.zip";
        InputStream testFile = getTestNetexArchiveAsStream();

        //populate fake blob repo
        inMemoryBlobStoreRepository.uploadBlob("rut/" + testFilename, testFile, false);

        // Mock initial call to Chouette to import job
        context.getRouteDefinition("chouette-send-import-job").adviceWith(context, new AdviceWithRouteBuilder() {
            @Override
            public void configure() {
                interceptSendToEndpoint(chouetteUrl + "/chouette_iev/referentials/rut/importer/netexprofile")
                        .skipSendToOriginalEndpoint().to("mock:chouetteCreateImport");
            }
        });

        // Mock job polling route - AFTER header validatio (to ensure that we send correct headers in test as well
        context.getRouteDefinition("chouette-validate-job-status-parameters").adviceWith(context, new AdviceWithRouteBuilder() {
            @Override
            public void configure() {
                interceptSendToEndpoint("direct:checkJobStatus").skipSendToOriginalEndpoint()
                        .to("mock:pollJobStatus");
            }
        });

        // Mock update status calls
        context.getRouteDefinition("chouette-process-import-status").adviceWith(context, new AdviceWithRouteBuilder() {
            @Override
            public void configure() {
                weaveByToUri("direct:updateStatus").replace().to("mock:updateStatus");
                weaveByToUri("direct:checkScheduledJobsBeforeTriggeringNextAction").replace().to("mock:checkScheduledJobsBeforeTriggeringNextAction");
            }
        });

        // we must manually start when we are done with all the advice with
        context.start();

        // 1 initial import call
        chouetteCreateImport.expectedMessageCount(1);
        chouetteCreateImport.returnReplyHeader("Location", new SimpleExpression(
                                                                                       chouetteUrl.replace("http4:", "http://") + "/chouette_iev/referentials/rut/scheduled_jobs/1"));


        pollJobStatus.expectedMessageCount(1);


        updateStatus.expectedMessageCount(1);
        checkScheduledJobsBeforeTriggeringNextAction.expectedMessageCount(1);


        Map<String, Object> headers = new HashMap<>();
        headers.put(Constants.PROVIDER_ID, "2");
        headers.put(Constants.FILE_NAME, testFilename);
        headers.put(Constants.CORRELATION_ID, "corr_id");
        headers.put(Constants.FILE_HANDLE, "rut/" + testFilename);
        importTemplate.sendBodyAndHeaders(null, headers);

        chouetteCreateImport.assertIsSatisfied();
        pollJobStatus.assertIsSatisfied();

        Exchange exchange = pollJobStatus.getReceivedExchanges().get(0);
        exchange.getIn().setHeader("action_report_result", "OK");
        exchange.getIn().setHeader("validation_report_result", "OK");
        processImportResultTemplate.send(exchange);

        checkScheduledJobsBeforeTriggeringNextAction.assertIsSatisfied();
        updateStatus.assertIsSatisfied();


    }


    @Test
    public void testJobListResponseTerminated() throws Exception {
        testJobListResponse("/no/rutebanken/marduk/chouette/getJobListResponseAllTerminated.json", true);
    }

    @Test
    public void testJobListResponseScheduled() throws Exception {
        testJobListResponse("/no/rutebanken/marduk/chouette/getJobListResponseScheduled.json", false);
    }

    public void testJobListResponse(String jobListResponseClasspathReference, boolean expectExport) throws Exception {

        context.getRouteDefinition("chouette-process-job-list-after-import").adviceWith(context, new AdviceWithRouteBuilder() {
            @Override
            public void configure() {
                interceptSendToEndpoint(chouetteUrl + "/*")
                        .skipSendToOriginalEndpoint()
                        .to("mock:chouetteGetJobsForProvider");
                interceptSendToEndpoint("entur-google-pubsub:ChouetteValidationQueue")
                        .skipSendToOriginalEndpoint()
                        .to("mock:chouetteValidationQueue");
            }
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
        headers.put(Constants.PROVIDER_ID, "2");
        headers.put(Constants.CHOUETTE_REFERENTIAL, "rut");
        headers.put(Constants.ENABLE_VALIDATION, true);

        triggerJobListTemplate.sendBodyAndHeaders(null, headers);

        chouetteGetJobs.assertIsSatisfied();

        if (expectExport) {
            chouetteValidationQueue.expectedMessageCount(1);
        }
        chouetteValidationQueue.assertIsSatisfied();

    }

}