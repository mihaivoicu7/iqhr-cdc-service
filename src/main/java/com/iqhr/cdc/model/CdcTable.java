package com.iqhr.cdc.model;

import jakarta.persistence.*;
import java.time.Instant;

@Entity @Table(name="IQHR_CdcTable",schema="dbo")
public class CdcTable {
    @Id @Column(name="table_id",length=64) public String tableId;
    @Column(name="tenant_id",nullable=false,length=64) public String tenantId;
    @Column(name="source_id",nullable=false,length=100) public String sourceId;
    @Column(name="schema_name",nullable=false,length=128) public String schemaName;
    @Column(name="table_name",nullable=false,length=128) public String tableName;
    @Column(name="sql_cdc_enabled",nullable=false) public boolean sqlCdcEnabled;
    @Column(name="capture_fingerprint",length=64) public String captureFingerprint;
    @Column(name="desired_enabled",nullable=false) public boolean desiredEnabled;
    @Column(name="active",nullable=false) public boolean active;
    @Column(name="state",nullable=false,length=32) public String state;
    @Column(name="diagnostic",length=1000) public String diagnostic;
    @Column(name="observed_at",nullable=false) public Instant observedAt;
    @Column(name="requested_at") public Instant requestedAt;
    @Column(name="enabled_at") public Instant enabledAt;
    @Column(name="requested_by",length=256) public String requestedBy;
    @Column(name="activation_lsn",length=32) public String activationLsn;
    @Version @Column(name="version",nullable=false) public long version;
    public String qualifiedName() {return schemaName+"."+tableName;}
}
