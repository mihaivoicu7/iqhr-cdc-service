package com.iqhr.cdc;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.iqhr.cdc.source.EventCodec;
import com.iqhr.cdc.model.HistoryEvent;
import java.util.List;

final class Fixtures {
    static final ObjectMapper MAPPER=new ObjectMapper().findAndRegisterModules();
    static final CdcProperties.Tenant TENANT=tenant("TECHNO");
    static final String INCARNATION="01b67815-3af8-491a-b3bb-ed64ea4b67f8";
    static CdcProperties.Tenant tenant(String id) {
        return new CdcProperties.Tenant(id,"employee-contract","localhost",1433,"tdev_technophar","user","test-password",false,true,
                List.of("dbo.HR_EmployeeContractInfo"),true,20160,1024,128);
    }
    static EventCodec codec() { return new EventCodec(MAPPER,TENANT,INCARNATION); }
    static String json(String op,String before,String after) {
        return """
            {"op":"%s","before":%s,"after":%s,"source":{"db":"tdev_technophar","schema":"dbo","table":"HR_EmployeeContractInfo",
             "commit_lsn":"00000027:00000ac0:0003","change_lsn":"00000027:00000ab8:0002","event_serial_no":1,"ts_ms":1758500000000}}
            """.formatted(op,before,after);
    }
    static HistoryEvent event() { return codec().parse("{\"EmployeeID\":42,\"StartDate\":\"2026-01-01\"}",json("c","null","{\"name\":\"A\"}"),"topic"); }
}
