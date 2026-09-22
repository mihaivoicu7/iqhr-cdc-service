package com.iqhr.cdc.model;

import jakarta.persistence.*;
import java.time.Instant;

@Entity @Table(name="IQHR_CdcStatus", schema="dbo")
public class CdcStatus {
    @Id @Column(name="source_id", length=100) public String sourceId;
    @Column(name="tenant_id", nullable=false, length=64) public String tenantId;
    @Column(name="state", nullable=false, length=32) public String state;
    @Column(name="message", length=1000) public String message;
    @Column(name="updated_at", nullable=false) public Instant updatedAt;
    @Column(name="last_event_at") public Instant lastEventAt;
    @Column(name="last_commit_lsn", length=32) public String lastCommitLsn;
    @Column(name="retention_minutes") public Integer retentionMinutes;
    @Column(name="retention_headroom_seconds") public Long retentionHeadroomSeconds;
    @Column(name="capture_state", nullable=false, length=32) public String captureState;
    @Column(name="consumer_state", nullable=false, length=32) public String consumerState;
}
