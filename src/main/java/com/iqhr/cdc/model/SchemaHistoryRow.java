package com.iqhr.cdc.model;

import jakarta.persistence.*;
import org.hibernate.annotations.Immutable;

/** Minimal read-only mapping; all schema-history writes belong to the Debezium JDBC SPI. */
@Entity @Immutable @Table(name="IQHR_CdcSchemaHistory",schema="dbo")
public class SchemaHistoryRow {
    @Id @Column(name="storage_sequence") public long sequence;
}
