package com.iqhr.cdc.model;

import jakarta.persistence.*;
import org.hibernate.annotations.Immutable;

/** Minimal read-only mapping; all schema-history writes belong to the Debezium JDBC SPI. */
@Entity @Immutable @Table(name="IQHR_CdcSchemaHistory",schema="dbo")
public class SchemaHistoryRow {
    @Id @Column(name="storage_sequence") public long sequence;
    /** Debezium's serialized schema change, possibly split into ordered parts of one record id. */
    @Column(name="id",length=36) public String recordId;
    @Column(name="history_data_seq") public int part;
    @Column(name="history_data",columnDefinition="nvarchar(max)") public String data;
}
