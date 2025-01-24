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
import org.apache.camel.LoggingLevel;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import static no.rutebanken.marduk.Constants.*;

/**
 * Upload a dated version of an exported file with a unique name to the marduk-exchange blobstore.
 */
@Component
public class UploadDatedExportRouteBuilder extends BaseRouteBuilder {

    @Value("${netex.export.dated.version.path:outbound/dated}")
    private String blobStorePath;

    @Override
    public void configure() throws Exception {
        super.configure();

        from("direct:copyDatedExport")
                .streamCache("true")
                .setProperty("datedVersionFileName", simple("${header." + CHOUETTE_REFERENTIAL + "}-${date:now:yyyyMMddHHmmssSSS}.zip"))
                .log(LoggingLevel.INFO, getClass().getName(), correlation() + "Start copying dated version of ${exchangeProperty.datedVersionFileName} to marduk-exchange")
                .setHeader(FILE_HANDLE, simple(Constants.BLOBSTORE_PATH_OUTBOUND + "netex/" + "${header." + CHOUETTE_REFERENTIAL + "}-" + Constants.CURRENT_AGGREGATED_NETEX_FILENAME))
                .setHeader(TARGET_FILE_HANDLE, simple(blobStorePath + "/${exchangeProperty.datedVersionFileName}"))
                .setHeader(TARGET_CONTAINER, simple("${properties:blobstore.gcs.exchange.container.name}"))
                .to("direct:copyBlobToAnotherBucket")
                .routeId("copy-dated-export");

    }
}
