package com.integration.monday.platform.service;

import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;

@Service
public class MondayWebClientService {

    private final WebClient webClient;

    private final String MONDAY_API_URL = "https://api.monday.com/v2";
    // https://developer.monday.com/api-reference/docs/authentication
    private final String API_TOKEN = "your-api-token";


    public MondayWebClientService(WebClient.Builder webClientBuilder) {
        this.webClient = webClientBuilder.baseUrl("").build();
    }

    public String fetchData() {
        String graphqlQuery = "{ \"query\": \"query { boards { id name items { id name } } }\" }";

        return this.webClient.post()
                .uri(MONDAY_API_URL)
                .header("Authorization", "Bearer "+API_TOKEN)
                .header("Content-Type", "application/json")
                .bodyValue(graphqlQuery)
                .retrieve()
                .bodyToMono(String.class)
                .block(); // for sync
    }
}
