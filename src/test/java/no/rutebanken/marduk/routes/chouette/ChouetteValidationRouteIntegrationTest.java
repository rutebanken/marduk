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
import no.rutebanken.marduk.TestApp;
import no.rutebanken.marduk.routes.status.JobEvent;
import org.apache.camel.*;
import org.apache.camel.builder.AdviceWithRouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.model.ModelCamelContext;
import org.apache.camel.model.language.SimpleExpression;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,classes = TestApp.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class ChouetteValidationRouteIntegrationTest extends MardukRouteBuilderIntegrationTestBase {

	@Autowired
	private ModelCamelContext context;

	@EndpointInject(uri = "mock:chouetteCreateValidation")
	protected MockEndpoint chouetteCreateValidation;

	@EndpointInject(uri = "mock:pollJobStatus")
	protected MockEndpoint pollJobStatus;

	@EndpointInject(uri = "mock:chouetteGetJobsForProvider")
	protected MockEndpoint chouetteGetJobs;

	@EndpointInject(uri = "mock:processValidationResult")
	protected MockEndpoint processValidationResult;

	@EndpointInject(uri = "mock:chouetteTransferExportQueue")
	protected MockEndpoint chouetteTransferExportQueue;

	@EndpointInject(uri = "mock:checkScheduledJobsBeforeTriggeringExport")
	protected MockEndpoint chouetteCheckScheduledJobs;

	@EndpointInject(uri = "mock:updateStatus")
	protected MockEndpoint updateStatus;

	@Produce(uri = "entur-google-pubsub:ChouetteValidationQueue")
	protected ProducerTemplate validationTemplate;

	@Produce(uri = "direct:processValidationResult")
	protected ProducerTemplate processValidationResultTemplate;

	@Produce(uri = "direct:checkScheduledJobsBeforeTriggeringExport")
	protected ProducerTemplate triggerJobListTemplate;

	@Value("${chouette.url}")
	private String chouetteUrl;

	@BeforeEach
	protected void setUp() throws IOException {
		super.setUp();
		chouetteCreateValidation.reset();
		pollJobStatus.reset();
		chouetteGetJobs.reset();
		processValidationResult.reset();
		chouetteTransferExportQueue.reset();
		chouetteCheckScheduledJobs.reset();
		updateStatus.reset();
	}
	
	@Test
	void testRunChouetteValidation() throws Exception {

		// Mock initial call to Chouette to validation job
		AdviceWithRouteBuilder.adviceWith(context, "chouette-send-validation-job", a -> {
			a.interceptSendToEndpoint(chouetteUrl + "/chouette_iev/referentials/rut/validator")
					.skipSendToOriginalEndpoint().to("mock:chouetteCreateValidation");
			a.interceptSendToEndpoint("direct:updateStatus").skipSendToOriginalEndpoint()
					.to("mock:updateStatus");
		});

		// Mock job polling route - AFTER header validatio (to ensure that we send correct headers in test as well)
		AdviceWithRouteBuilder.adviceWith(context, "chouette-validate-job-status-parameters", a -> {
			a.interceptSendToEndpoint("direct:checkJobStatus").skipSendToOriginalEndpoint()
					.to("mock:pollJobStatus");
		});

		// Mock update status calls
		AdviceWithRouteBuilder.adviceWith(context, "chouette-process-validation-status", a -> {
			a.interceptSendToEndpoint("direct:updateStatus").skipSendToOriginalEndpoint()
					.to("mock:updateStatus");
			a.interceptSendToEndpoint("direct:checkScheduledJobsBeforeTriggeringExport").skipSendToOriginalEndpoint()
					.to("mock:checkScheduledJobsBeforeTriggeringExport");
		});

		// we must manually start when we are done with all the advice with
		context.start();

		// 1 initial import call
		chouetteCreateValidation.expectedMessageCount(1);
		chouetteCreateValidation.returnReplyHeader("Location", new SimpleExpression(
				chouetteUrl.replace("http4:", "http://") + "/chouette_iev/referentials/rut/scheduled_jobs/1"));

	
		pollJobStatus.expectedMessageCount(1);
		
		
		updateStatus.expectedMessageCount(2);
		chouetteCheckScheduledJobs.expectedMessageCount(1);
		
		
		Map<String, Object> headers = new HashMap<>();
		headers.put(Constants.PROVIDER_ID, "2");
		headers.put(Constants.CHOUETTE_JOB_STATUS_JOB_VALIDATION_LEVEL, JobEvent.TimetableAction.VALIDATION_LEVEL_2.toString());
		validationTemplate.sendBodyAndHeaders(null, headers);

		chouetteCreateValidation.assertIsSatisfied();
		pollJobStatus.assertIsSatisfied();
		
		Exchange exchange = pollJobStatus.getReceivedExchanges().get(0);
		exchange.getIn().setHeader("action_report_result", "OK");
		exchange.getIn().setHeader("validation_report_result", "OK");
		processValidationResultTemplate.send(exchange );
		
		chouetteCheckScheduledJobs.assertIsSatisfied();
		updateStatus.assertIsSatisfied();
		
		
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

		AdviceWithRouteBuilder.adviceWith(context, "chouette-process-job-list-after-validation", a -> {
			a.interceptSendToEndpoint(chouetteUrl + "/*")
					.skipSendToOriginalEndpoint()
					.to("mock:chouetteGetJobsForProvider");
			a.interceptSendToEndpoint("entur-google-pubsub:ChouetteTransferExportQueue")
					.skipSendToOriginalEndpoint()
					.to("mock:chouetteTransferExportQueue");
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
		headers.put(Constants.CHOUETTE_REFERENTIAL, "rut");
		headers.put(Constants.PROVIDER_ID,2);
		
		triggerJobListTemplate.sendBodyAndHeaders(null,headers);
		
		chouetteGetJobs.assertIsSatisfied();

		if (expectExport) {
			chouetteTransferExportQueue.expectedMessageCount(1);
		}
		chouetteTransferExportQueue.assertIsSatisfied();

	}

}