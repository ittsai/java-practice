package com.integration.monday.platform.controller;


import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

import static org.springframework.util.MimeTypeUtils.APPLICATION_JSON_VALUE;

@RestController
@RequestMapping("/monday/api")
public class WebhookController {

    @PostMapping(value = "/notification", consumes = APPLICATION_JSON_VALUE)
    public Map<String, Object>  postNotification(@RequestBody Map<String, Object> payload) {
        System.out.print("Received Webhook: " + payload);

        return payload;
    }
}
