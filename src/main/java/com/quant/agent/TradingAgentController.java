package com.quant.agent;

import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

@CrossOrigin(origins = "*", allowedHeaders = "*")
@RestController
public class TradingAgentController {

    private final TradingAgentService tradingAgentService;

    public TradingAgentController(TradingAgentService tradingAgentService) {
        this.tradingAgentService = tradingAgentService;
    }

    public record ChatRequest(String input) {}

    @PostMapping(value = "/api/chat/stream", produces = MediaType.TEXT_PLAIN_VALUE)
    public Flux<String> streamChat(@RequestBody ChatRequest request) {
        return this.tradingAgentService.streamAgentResponse(request.input());
    }
}