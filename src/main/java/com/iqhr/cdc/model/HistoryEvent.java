package com.iqhr.cdc.model;

import jakarta.persistence.*;
import java.time.Instant;

@Entity @Table(name = "IQHR_CdcEvent", schema = "dbo")
public class HistoryEvent {
    @Id @Column(name="event_id", length=64) public String eventId;
    @Column(name="tenant_id", nullable=false, length=64) public String tenantId;
    @Column(name="source_id", nullable=false, length=100) public String sourceId;
    @Column(name="schema_name", nullable=false, length=128) public String schemaName;
    @Column(name="table_name", nullable=false, length=128) public String tableName;
    @Column(name="operation", nullable=false, length=16) public String operation;
    @Column(name="occurred_at", nullable=false) public Instant occurredAt;
    @Column(name="captured_at", nullable=false) public Instant capturedAt;
    @Column(name="record_key", nullable=false, columnDefinition="nvarchar(max)") public String recordKey;
    @Column(name="before_json", columnDefinition="nvarchar(max)") public String beforeJson;
    @Column(name="after_json", columnDefinition="nvarchar(max)") public String afterJson;
    @Column(name="changed_properties", nullable=false, columnDefinition="nvarchar(max)") public String changedProperties;
    @Column(name="commit_lsn", length=32) public String commitLsn;
    @Column(name="change_lsn", length=32) public String changeLsn;
    @Column(name="event_serial_no") public Long eventSerialNo;
}
