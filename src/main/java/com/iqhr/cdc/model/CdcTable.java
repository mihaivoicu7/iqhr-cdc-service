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
    /** Service-owned schema evolution state: CURRENT, SOURCE_CHANGED, READER_STALE, REFRESHING or REFRESH_FAILED. */
    @Column(name="schema_state",length=32) public String schemaState;
    @Column(name="schema_detail",length=2000) public String schemaDetail;
    @Column(name="capture_instances",length=1000) public String captureInstances;
    /** Admin-owned refresh request; the service consumes it when it creates the replacement capture instance. */
    @Column(name="refresh_requested_at") public Instant refreshRequestedAt;
    @Column(name="refresh_requested_by",length=256) public String refreshRequestedBy;
    @Column(name="refresh_started_at") public Instant refreshStartedAt;
    @Column(name="refresh_detail",length=2000) public String refreshDetail;
    @Column(name="refreshed_at") public Instant refreshedAt;
    @Column(name="refresh_error",length=200) public String refreshError;
    /** Set when only this table stopped (lost source history or CDC disabled) while the others continued. */
    @Column(name="suspended_at") public Instant suspendedAt;
    @Column(name="suspension_reason",length=64) public String suspensionReason;
    @Column(name="suspended_lsn",length=32) public String suspendedLsn;
    @Version @Column(name="version",nullable=false) public long version;
    public String qualifiedName() {return schemaName+"."+tableName;}
}
