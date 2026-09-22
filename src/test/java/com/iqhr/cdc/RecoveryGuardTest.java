package com.iqhr.cdc;

import static org.assertj.core.api.Assertions.*;
import org.junit.jupiter.api.Test;
import com.iqhr.cdc.source.*;
import com.iqhr.cdc.store.TenantStore;
import com.iqhr.cdc.model.*;
import java.util.*;

class RecoveryGuardTest {
    SourceActivation activation() { var row=new SourceActivation();row.tenantId="TECHNO";row.configurationHash=TenantStore.configurationHash(Fixtures.TENANT);return row; }
    OffsetRow offset() {
        var row=new OffsetRow();row.key="[\"iqhr-cdc-TECHNO-employee-contract\",{\"server\":\"iqhr-cdc-TECHNO-employee-contract\",\"database\":\"tdev_technophar\"}]";
        row.value="{\"commit_lsn\":\"00000027:00000ac0:0003\",\"change_lsn\":\"00000027:00000ab8:0002\",\"event_serial_no\":1}";return row;
    }
    @Test void onlyPristineActivationMayStartWithoutOffsets() {
        assertThat(RecoveryGuard.savedLsn(null,List.of(),Fixtures.TENANT,Fixtures.MAPPER)).isNull();
        assertThatThrownBy(() -> RecoveryGuard.savedLsn(activation(),List.of(),Fixtures.TENANT,Fixtures.MAPPER)).hasMessage("SOURCE_OFFSETS_MISSING_OR_AMBIGUOUS");
        assertThatThrownBy(() -> RecoveryGuard.savedLsn(null,List.of(offset()),Fixtures.TENANT,Fixtures.MAPPER)).hasMessage("ACTIVATION_MISSING_FOR_EXISTING_OFFSETS");
    }
    @Test void corruptForeignAndIncompletePositionsFailClosed() {
        var row=offset();assertThat(RecoveryGuard.savedLsn(activation(),List.of(row),Fixtures.TENANT,Fixtures.MAPPER)).isEqualTo("00000027:00000ac0:0003");
        row.value="{}";assertThatThrownBy(() -> RecoveryGuard.savedLsn(activation(),List.of(row),Fixtures.TENANT,Fixtures.MAPPER)).hasMessage("SOURCE_REPLAY_COORDINATES_INVALID");
        row.value="{\"snapshot\":\"INITIAL\",\"snapshot_completed\":false}";
        assertThatThrownBy(() -> RecoveryGuard.savedLsn(activation(),List.of(row),Fixtures.TENANT,Fixtures.MAPPER)).hasMessage("SOURCE_INITIALIZATION_INCOMPLETE");
        row.key=row.key.replace("tdev_technophar","another");
        assertThatThrownBy(() -> RecoveryGuard.savedLsn(activation(),List.of(row),Fixtures.TENANT,Fixtures.MAPPER)).hasMessage("SOURCE_OFFSET_PARTITION_CHANGED");
    }
    @Test void nativeStorageIsFencedAndUsesSqlServerSchemaHistoryOrder() {
        var properties=DebeziumConfiguration.create(Fixtures.TENANT,19);
        assertThat(properties.getProperty("offset.storage.jdbc.table.delete")).contains("UPDLOCK,HOLDLOCK","fence=19","DELETE FROM dbo.%s");
        assertThat(properties.getProperty("schema.history.internal.jdbc.table.exists")).contains("TOP(1)").doesNotContain("LIMIT");
        assertThat(properties.getProperty("schema.history.internal.jdbc.table.select")).contains("storage_sequence");
        assertThat(properties.getProperty("snapshot.mode")).isEqualTo("no_data");
    }
    @Test void overlappingCaptureInstanceRolloverPreservesAvailableSavedPosition() {
        byte[] saved=SqlServerControl.parseLsn("00000027:00000ac0:0003");
        byte[] oldMin=SqlServerControl.parseLsn("00000027:00000a00:0001");
        byte[] newMin=SqlServerControl.parseLsn("00000027:00000b00:0001");
        byte[] maximum=SqlServerControl.parseLsn("00000027:00000c00:0001");
        assertThat(SqlServerControl.covers(saved,oldMin,newMin,maximum)).isTrue();
        assertThat(SqlServerControl.covers(saved,newMin,null,maximum)).isFalse();
        assertThat(SqlServerControl.covers(saved,oldMin,oldMin,maximum)).isFalse();
    }
    @Test void missingReplayFieldsCannotFallBackToConnectorDefaults() {
        var row=offset();row.value=row.value.replace("\"event_serial_no\":1","\"event_serial_no\":-1");
        assertThatThrownBy(() -> RecoveryGuard.savedLsn(activation(),List.of(row),Fixtures.TENANT,Fixtures.MAPPER)).hasMessage("SOURCE_REPLAY_COORDINATES_INVALID");
        row.value="{\"commit_lsn\":\"00000027:00000ac0:0003\",\"change_lsn\":null,\"event_serial_no\":1}";
        assertThatThrownBy(() -> RecoveryGuard.savedLsn(activation(),List.of(row),Fixtures.TENANT,Fixtures.MAPPER)).hasMessage("SOURCE_REPLAY_COORDINATES_INVALID");
        row.value=row.value.replace("{","{\"snapshot\":\"INITIAL\",\"snapshot_completed\":true,");
        assertThat(RecoveryGuard.savedLsn(activation(),List.of(row),Fixtures.TENANT,Fixtures.MAPPER)).isEqualTo("00000027:00000ac0:0003");
    }
    @Test void safeDiagnosticsRetainSqlErrorCodesAndNeverMessages() {
        String code=TenantPipeline.code(new IllegalStateException("secret-value",new java.sql.SQLException("password=secret-row", "28000",18456)));
        assertThat(code).contains("IllegalStateException","SQLException","28000","18456").doesNotContain("secret","password");
    }
    @Test void liveNoDataInitialOffsetRetainsItsConcreteCommitBoundary() {
        var row=offset();
        row.value="{\"event_serial_no\":1,\"commit_lsn\":\"0006d9d3:000000d8:001b\",\"change_lsn\":\"NULL\"}";
        assertThat(RecoveryGuard.savedLsn(activation(),List.of(row),Fixtures.TENANT,Fixtures.MAPPER))
                .isEqualTo("0006d9d3:000000d8:001b");
        row.value=row.value.replace("\"event_serial_no\":1","\"event_serial_no\":2");
        assertThatThrownBy(() -> RecoveryGuard.savedLsn(activation(),List.of(row),Fixtures.TENANT,Fixtures.MAPPER))
                .hasMessage("SOURCE_REPLAY_COORDINATES_INVALID");
        row.value="{\"event_serial_no\":1,\"commit_lsn\":\"NULL\",\"change_lsn\":\"NULL\"}";
        assertThatThrownBy(() -> RecoveryGuard.savedLsn(activation(),List.of(row),Fixtures.TENANT,Fixtures.MAPPER))
                .hasMessage("INVALID_SOURCE_POSITION");
    }
}
