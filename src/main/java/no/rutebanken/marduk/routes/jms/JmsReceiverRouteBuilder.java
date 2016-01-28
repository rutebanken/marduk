package no.rutebanken.marduk.routes.jms;

import no.rutebanken.marduk.routes.BaseRouteBuilder;
import org.apache.camel.LoggingLevel;
import org.jboss.poc.camelblob.BlobMessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import static no.rutebanken.marduk.Constants.FILE_HANDLE;
import static no.rutebanken.marduk.Constants.PROVIDER_ID;

/**
 * Receives file from "external" queue, using ActiveMQ blob message. Then putting it in jcouds blob store and posting handle on queue.
 */
@Component
public class JmsReceiverRouteBuilder extends BaseRouteBuilder {


    @Bean
    public BlobMessageConverter blobMessageConverter() {
        return new BlobMessageConverter();
    }

    @Override
    public void configure() throws Exception {
        super.configure();

        from("activemq:queue:ExternalFileUploadQueue?disableReplyTo=true&messageConverter=#blobMessageConverter")
            .log(LoggingLevel.INFO, getClass().getName(), "Received file ${header.CamelFileName} on jms receiver route for ${header.RutebankenProviderId}. Storing file ...")
            .setProperty("providerId", header("RutebankenProviderId"))
            .setProperty("fileName", header("CamelFileName"))
            .setHeader(FILE_HANDLE, simple("inbound/${property.providerId}-${date:now:yyyyMMddHHmmss}-${header.CamelFileName}"))
            .log(LoggingLevel.INFO, getClass().getName(), "File handle is: ${header." + FILE_HANDLE + "}")
            .to("log:" + getClass().getName() + "?level=DEBUG&showAll=true&multiline=true")
            .to("direct:uploadBlob")
            .setHeader(PROVIDER_ID, simple("${property.providerId}"))
            .log(LoggingLevel.INFO, getClass().getName(), "Putting handle ${header." + FILE_HANDLE + "} and provider ${header." + PROVIDER_ID + "} on queue...")
            .to("activemq:queue:ProcessFileQueue")
            .setBody(simple("Received file ${header.CamelFileName} on jms receiver route for ${header.RutebankenProviderId}."))
            .to("activemq:queue:ExternalProviderStatus");    //TODO switch to topic?
    }

}
