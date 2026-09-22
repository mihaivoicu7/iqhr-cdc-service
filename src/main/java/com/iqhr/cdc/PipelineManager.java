package com.iqhr.cdc;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.*;
import org.springframework.boot.actuate.health.*;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

@Component("cdc")
public class PipelineManager implements SmartLifecycle,HealthIndicator {
    private final List<TenantPipeline> pipelines;
    private volatile boolean running;
    public PipelineManager(CdcProperties properties,ObjectMapper mapper) {
        pipelines=properties.tenants().stream().filter(CdcProperties.Tenant::enabled)
                .map(tenant -> new TenantPipeline(properties,tenant,mapper)).toList();
    }
    @Override public void start() { running=true;pipelines.forEach(TenantPipeline::start); }
    @Override public void stop() { pipelines.parallelStream().forEach(TenantPipeline::close);running=false; }
    @Override public boolean isRunning() { return running; }
    @Override public Health health() {
        // No unauthenticated source names, diagnostics, row values or credentials are exposed.
        return running&&!pipelines.isEmpty()&&pipelines.stream().allMatch(TenantPipeline::healthy)?Health.up().build():Health.down().build();
    }
}
