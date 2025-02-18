package com.integration.monday.platform.service;

import org.springframework.boot.web.reactive.function.client.WebClientCustomizer;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;

@Service
public class MondayWebClientService {

    private final WebClient webClient;

    public MondayWebClientService(WebClient.Builder webClientBuilder) {
        this.webClient = webClientBuilder.baseUrl("").build();
    }

    public String fetchData() {
        return this.webClient.get()
                .uri("/")
                .retrieve()
                .bodyToMono(String.class)
                .block(); // for sync
    }
}
