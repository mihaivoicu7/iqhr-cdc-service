package com.iqhr.cdc.model;

import jakarta.persistence.*;
import java.time.Instant;

/** The sole explicitly authorized service-owned CDC source; never a public history event. */
@Entity @Table(name="IQHR_CdcHeartbeat",schema="dbo")
public class SourceHeartbeat {
    @Id @Column(name="singleton_id") public Integer singletonId;
    @Column(name="tick_sequence",nullable=false) public long tickSequence;
    @Column(name="ticked_at",nullable=false) public Instant tickedAt;
}
