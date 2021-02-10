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

package no.rutebanken.marduk.config;

import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.security.oauth2.client.OAuth2ClientProperties;
import org.springframework.boot.autoconfigure.security.oauth2.client.OAuth2ClientPropertiesRegistrationAdapter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.oauth2.client.AuthorizedClientServiceReactiveOAuth2AuthorizedClientManager;
import org.springframework.security.oauth2.client.ClientCredentialsReactiveOAuth2AuthorizedClientProvider;
import org.springframework.security.oauth2.client.InMemoryReactiveOAuth2AuthorizedClientService;
import org.springframework.security.oauth2.client.ReactiveOAuth2AuthorizedClientManager;
import org.springframework.security.oauth2.client.ReactiveOAuth2AuthorizedClientService;
import org.springframework.security.oauth2.client.endpoint.WebClientReactiveClientCredentialsTokenResponseClient;
import org.springframework.security.oauth2.client.registration.ClientRegistration;
import org.springframework.security.oauth2.client.registration.InMemoryReactiveClientRegistrationRepository;
import org.springframework.security.oauth2.client.registration.ReactiveClientRegistrationRepository;
import org.springframework.security.oauth2.client.web.reactive.function.client.ServerOAuth2AuthorizedClientExchangeFilterFunction;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.ClientRequest;
import org.springframework.web.reactive.function.client.ExchangeFilterFunction;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;

@Configuration
public class WebClientConfig {

    /**
     * Return a WebClient for authorized API calls.
     * The WebClient inserts a JWT bearer token in the Authorization HTTP header.
     * The JWT token is obtained from the configured Authorization Server.
     * @param properties
     * @param audience
     * @return
     */
    @Bean
    WebClient webClient(OAuth2ClientProperties properties, @Value("${marduk.oauth2.client.audience}") String audience) {
        ServerOAuth2AuthorizedClientExchangeFilterFunction oauth = new ServerOAuth2AuthorizedClientExchangeFilterFunction(reactiveOAuth2AuthorizedClientManager(properties, audience));
        oauth.setDefaultClientRegistrationId("marduk");
        //oauth.setAuthorizationFailureHandler(new RemoveAuthorizedClientReactiveOAuth2AuthorizationFailureHandler());

        return WebClient.builder()
                .filters(exchangeFilterFunctions -> {
                            exchangeFilterFunctions.add(oauth);
                        }
                )
                .build();
    }


    /**
     * Return the repository of OAuth2 clients.
     * In a reactive Spring Boot application this bean would be auto-configured.
     * Since Marduk is servlet-based (not reactive), the bean must be created manually.
     * @param properties
     * @return
     */
    private ReactiveClientRegistrationRepository clientRegistrationRepository(OAuth2ClientProperties properties) {
        List<ClientRegistration> registrations = new ArrayList<>(
                OAuth2ClientPropertiesRegistrationAdapter.getClientRegistrations(properties).values());
        return new InMemoryReactiveClientRegistrationRepository(registrations);
    }


    /**
     * Return an Authorized Client Manager.
     * This must be manually configured in order to inject a WebClient compatible with Auth0.
     * See {@link #webClientForTokenRequest(String)}
     * @param  properties
     * @param audience
     * @return
     */
    private ReactiveOAuth2AuthorizedClientManager reactiveOAuth2AuthorizedClientManager(OAuth2ClientProperties properties, String audience) {

        ReactiveClientRegistrationRepository clientRegistrations = clientRegistrationRepository(properties);


        ReactiveOAuth2AuthorizedClientService reactiveOAuth2AuthorizedClientService = new InMemoryReactiveOAuth2AuthorizedClientService(clientRegistrations);

        WebClientReactiveClientCredentialsTokenResponseClient webClientReactiveClientCredentialsTokenResponseClient = new WebClientReactiveClientCredentialsTokenResponseClient();
        webClientReactiveClientCredentialsTokenResponseClient.setWebClient(webClientForTokenRequest(audience));


        ClientCredentialsReactiveOAuth2AuthorizedClientProvider reactiveOAuth2AuthorizedClientProvider = new ClientCredentialsReactiveOAuth2AuthorizedClientProvider();
        reactiveOAuth2AuthorizedClientProvider.setAccessTokenResponseClient(webClientReactiveClientCredentialsTokenResponseClient);

        AuthorizedClientServiceReactiveOAuth2AuthorizedClientManager authorizedClientServiceReactiveOAuth2AuthorizedClientManager = new AuthorizedClientServiceReactiveOAuth2AuthorizedClientManager(clientRegistrations, reactiveOAuth2AuthorizedClientService);
        authorizedClientServiceReactiveOAuth2AuthorizedClientManager.setAuthorizedClientProvider(reactiveOAuth2AuthorizedClientProvider);
        return authorizedClientServiceReactiveOAuth2AuthorizedClientManager;
    }

    /**
     * Return a WebClient for requesting a token to the Authorization Server.
     * Auth0 requires that the form data in the body include an "audience" parameter in addition to the standard
     * "grant_type" parameter.
     *
     * @param audience the audience to be inserted in the request body for compatibility with Auth0.
     * @return a WebClient instance that can be used for requesting a token to the Authorization Server.
     */
    @NotNull
    private WebClient webClientForTokenRequest(String audience) {

        // The exchange filter adds the 2 required parameters in the request body.
        ExchangeFilterFunction tokenRequestFilter = ExchangeFilterFunction.ofRequestProcessor(clientRequest -> {
            ClientRequest.Builder builder = ClientRequest.from(clientRequest);
            LinkedMultiValueMap<String, String> formData = new LinkedMultiValueMap<>();
            formData.add("grant_type", "client_credentials");
            formData.add("audience", audience);
            builder.body(BodyInserters.fromFormData(formData));
            return Mono.just(builder.build());
        });

        return WebClient.builder()
                .filters(exchangeFilterFunctions -> {
                            exchangeFilterFunctions.add(tokenRequestFilter);
                        }
                )
                .build();
    }


}


