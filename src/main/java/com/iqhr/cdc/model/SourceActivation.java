package com.iqhr.cdc.model;

import jakarta.persistence.*;
import java.time.Instant;

@Entity @Table(name="IQHR_CdcActivation", schema="dbo")
public class SourceActivation {
    @Id @Column(name="source_id", length=100) public String sourceId;
    @Column(name="tenant_id", nullable=false, length=64) public String tenantId;
    @Column(name="incarnation", nullable=false, length=36) public String incarnation;
    @Column(name="configuration_hash", nullable=false, length=64) public String configurationHash;
    @Column(name="created_at", nullable=false) public Instant createdAt;
    @Column(name="fence", nullable=false) public long fence;
    @Column(name="table_registry_initialized",nullable=false) public boolean tableRegistryInitialized;
}
