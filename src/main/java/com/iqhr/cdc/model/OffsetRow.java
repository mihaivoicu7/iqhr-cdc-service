package com.iqhr.cdc.model;

import jakarta.persistence.*;

/** Read-only mapping of the library-owned JDBC protocol table. Writes belong to Debezium. */
@Entity @Table(name="IQHR_CdcOffset", schema="dbo")
public class OffsetRow {
    @Id @Column(name="id", length=36) public String id;
    @Column(name="offset_key", columnDefinition="nvarchar(max)") public String key;
    @Column(name="offset_val", columnDefinition="nvarchar(max)") public String value;
}
