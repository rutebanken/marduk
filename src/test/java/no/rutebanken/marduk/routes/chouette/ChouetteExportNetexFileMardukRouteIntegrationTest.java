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
import org.apache.camel.*;
import org.apache.camel.builder.AdviceWithRouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.model.ModelCamelContext;
import org.apache.camel.model.language.SimpleExpression;
import org.apache.commons.io.IOUtils;
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
class ChouetteExportNetexFileMardukRouteIntegrationTest extends MardukRouteBuilderIntegrationTestBase {

	@Autowired
	private ModelCamelContext context;

	@EndpointInject("mock:chouetteCreateExport")
	protected MockEndpoint chouetteCreateExport;

	@EndpointInject("mock:pollJobStatus")
	protected MockEndpoint pollJobStatus;

	@EndpointInject("mock:updateStatus")
	protected MockEndpoint updateStatus;

	@EndpointInject("mock:chouetteGetData")
	protected MockEndpoint chouetteGetData;


	@EndpointInject("mock:ChouetteMergeWithFlexibleLinesQueue")
	protected MockEndpoint mergeWithFlexibleLinesMock;

	@EndpointInject("mock:ExportGtfsQueue")
	protected MockEndpoint exportGtfsQueue;

	@Produce("entur-google-pubsub:ChouetteExportNetexQueue")
	protected ProducerTemplate importTemplate;

	@Produce("direct:processNetexExportResult")
	protected ProducerTemplate processExportResultTemplate;

	@Value("${chouette.url}")
	private String chouetteUrl;



	@Test
	void testExportDataspace() throws Exception {

		// Mock initial call to Chouette to import job
		AdviceWithRouteBuilder.adviceWith(context, "chouette-start-export-netex", a -> {
			a.interceptSendToEndpoint(chouetteUrl + "/chouette_iev/referentials/rut/exporter/netexprofile")
					.skipSendToOriginalEndpoint().to("mock:chouetteCreateExport");
			a.interceptSendToEndpoint("direct:updateStatus").skipSendToOriginalEndpoint()
					.to("mock:updateStatus");
		});

		// Mock job polling route - AFTER header validatio (to ensure that we send correct headers in test as well
		AdviceWithRouteBuilder.adviceWith(context, "chouette-validate-job-status-parameters", a -> a.interceptSendToEndpoint("direct:checkJobStatus").skipSendToOriginalEndpoint()
				.to("mock:pollJobStatus"));

		// Mock update status calls
		AdviceWithRouteBuilder.adviceWith(context, "chouette-process-export-netex-status", a -> {
			a.interceptSendToEndpoint("direct:updateStatus").skipSendToOriginalEndpoint()
					.to("mock:updateStatus");
			a.interceptSendToEndpoint("entur-google-pubsub:ChouetteMergeWithFlexibleLinesQueue").skipSendToOriginalEndpoint().to("mock:ChouetteMergeWithFlexibleLinesQueue");
			a.interceptSendToEndpoint("entur-google-pubsub:ChouetteExportGtfsQueue").skipSendToOriginalEndpoint().to("mock:ExportGtfsQueue");
		});

		AdviceWithRouteBuilder.adviceWith(context, "chouette-get-job-status", a -> a.interceptSendToEndpoint(chouetteUrl+ "/chouette_iev/referentials/rut/jobs/1/data")
				.skipSendToOriginalEndpoint().to("mock:chouetteGetData"));

		chouetteGetData.expectedMessageCount(1);
		chouetteGetData.returnReplyBody(new Expression() {

			@SuppressWarnings("unchecked")
			@Override
			public <T> T evaluate(Exchange ex, Class<T> arg1) {
				try {
					// Should be GTFS contnet
					return (T) IOUtils.toString(getClass()
							                            .getResourceAsStream("/no/rutebanken/marduk/chouette/getActionReportResponseOK.json"), StandardCharsets.UTF_8);
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			}
		});

		// we must manually start when we are done with all the advice with
		context.start();

		// 1 initial import call
		chouetteCreateExport.expectedMessageCount(1);
		chouetteCreateExport.returnReplyHeader("Location", new SimpleExpression(
				                                                                       chouetteUrl.replace("http:", "http://") + "/chouette_iev/referentials/rut/scheduled_jobs/1"));


		pollJobStatus.expectedMessageCount(1);
		updateStatus.expectedMessageCount(2);


		mergeWithFlexibleLinesMock.expectedMessageCount(1);
		exportGtfsQueue.expectedMessageCount(1);

		Map<String, Object> headers = new HashMap<>();
		headers.put(Constants.PROVIDER_ID, "2");

		importTemplate.sendBodyAndHeaders(null, headers);

		chouetteCreateExport.assertIsSatisfied();
		pollJobStatus.assertIsSatisfied();

		Exchange exchange = pollJobStatus.getReceivedExchanges().get(0);
		exchange.getIn().setHeader("action_report_result", "OK");
		exchange.getIn().setHeader("data_url", chouetteUrl+ "/chouette_iev/referentials/rut/jobs/1/data");
		processExportResultTemplate.send(exchange );

		chouetteGetData.assertIsSatisfied();
		updateStatus.assertIsSatisfied();
		mergeWithFlexibleLinesMock.assertIsSatisfied();
		exportGtfsQueue.assertIsSatisfied();

	}
}
