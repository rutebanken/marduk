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

import no.rutebanken.marduk.routes.status.JobEvent;
import org.apache.camel.LoggingLevel;
import org.springframework.stereotype.Component;

import static no.rutebanken.marduk.Constants.DATASET_REFERENTIAL;
import static no.rutebanken.marduk.Constants.PROVIDER_ID;

@Component
public class AntuNetexValidationStatusRouteBuilder extends AbstractChouetteRouteBuilder {

    private static final String STATUS_VALIDATION_STARTED = "started";
    private static final String STATUS_VALIDATION_OK = "ok";
    private static final String STATUS_VALIDATION_FAILED = "failed";

    @Override
    public void configure() throws Exception {
        super.configure();

        from("entur-google-pubsub:AntuNetexValidationStatusQueue")
                .process(e -> e.getIn().setHeader(PROVIDER_ID, getProviderRepository().getProviderId(e.getIn().getHeader(DATASET_REFERENTIAL, String.class))))
                .process(this::setCorrelationIdIfMissing)
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
                .when(header(DATASET_REFERENTIAL).startsWith("rb_"))
                .process(e -> JobEvent.providerJobBuilder(e).timetableAction(JobEvent.TimetableAction.EXPORT_NETEX_POSTVALIDATION).state(JobEvent.State.STARTED).build())
                .otherwise()
                .process(e -> JobEvent.providerJobBuilder(e).timetableAction(JobEvent.TimetableAction.PREVALIDATION).state(JobEvent.State.STARTED).build())
                //end otherwise
                .end()
                // end choice
                .end()
                .to("direct:updateStatus")
                .routeId("antu-netex-validation-started");

        from("direct:antuNetexValidationComplete")
                .log(LoggingLevel.INFO, getClass().getName(), correlation() + "Antu NeTEx validation complete for referential ${header." + DATASET_REFERENTIAL + "}")
                .choice()
                .when(header(DATASET_REFERENTIAL).startsWith("rb_"))
                .process(e -> JobEvent.providerJobBuilder(e).timetableAction(JobEvent.TimetableAction.EXPORT_NETEX_POSTVALIDATION).state(JobEvent.State.OK).build())
                .otherwise()
                .process(e -> JobEvent.providerJobBuilder(e).timetableAction(JobEvent.TimetableAction.PREVALIDATION).state(JobEvent.State.OK).build())
                //end otherwise
                .end()
                // end choice
                .end()
                .to("direct:updateStatus")
                .routeId("antu-netex-validation-complete");

        from("direct:antuNetexValidationFailed")
                .log(LoggingLevel.INFO, getClass().getName(), correlation() + "Antu NeTEx validation failed for referential ${header." + DATASET_REFERENTIAL + "}")
                .choice()
                .when(header(DATASET_REFERENTIAL).startsWith("rb_"))
                .process(e -> JobEvent.providerJobBuilder(e).timetableAction(JobEvent.TimetableAction.EXPORT_NETEX_POSTVALIDATION).state(JobEvent.State.FAILED).build())
                .otherwise()
                .process(e -> JobEvent.providerJobBuilder(e).timetableAction(JobEvent.TimetableAction.PREVALIDATION).state(JobEvent.State.FAILED).build())
                //end otherwise
                .end()
                // end choice
                .end()
                .to("direct:updateStatus")
                .routeId("antu-netex-validation-failed");

    }
}
