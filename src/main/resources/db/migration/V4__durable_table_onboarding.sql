IF COL_LENGTH(N'dbo.IQHR_CdcActivation',N'table_registry_initialized') IS NULL
    ALTER TABLE dbo.IQHR_CdcActivation ADD table_registry_initialized bit NOT NULL
        CONSTRAINT DF_IQHR_CdcActivation_table_registry_initialized DEFAULT 0 WITH VALUES;
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id=OBJECT_ID(N'dbo.IQHR_CdcActivation')
    AND name=N'table_registry_initialized' AND TYPE_NAME(system_type_id)=N'bit' AND is_nullable=0)
    THROW 51000,'Incompatible CDC registry initialization marker; explicit reviewed migration required',1;

IF OBJECT_ID(N'dbo.IQHR_CdcTable',N'U') IS NULL
BEGIN
    CREATE TABLE dbo.IQHR_CdcTable (
        table_id nvarchar(64) NOT NULL CONSTRAINT PK_IQHR_CdcTable PRIMARY KEY,
        tenant_id nvarchar(64) NOT NULL,
        source_id nvarchar(100) NOT NULL,
        schema_name nvarchar(128) NOT NULL,
        table_name nvarchar(128) NOT NULL,
        sql_cdc_enabled bit NOT NULL,
        capture_fingerprint nvarchar(64) NULL,
        desired_enabled bit NOT NULL,
        active bit NOT NULL,
        state nvarchar(32) NOT NULL,
        diagnostic nvarchar(1000) NULL,
        observed_at datetime2(7) NOT NULL,
        requested_at datetime2(7) NULL,
        enabled_at datetime2(7) NULL,
        requested_by nvarchar(256) NULL,
        activation_lsn nvarchar(32) NULL,
        version bigint NOT NULL CONSTRAINT DF_IQHR_CdcTable_version DEFAULT 0,
        CONSTRAINT UQ_IQHR_CdcTable_name UNIQUE(tenant_id,source_id,schema_name,table_name)
    );
END;
IF COALESCE(COL_LENGTH(N'dbo.IQHR_CdcTable',N'table_id'),-1)<>128
 OR COALESCE(COL_LENGTH(N'dbo.IQHR_CdcTable',N'tenant_id'),-1)<>128
 OR COALESCE(COL_LENGTH(N'dbo.IQHR_CdcTable',N'source_id'),-1)<>200
 OR COALESCE(COL_LENGTH(N'dbo.IQHR_CdcTable',N'schema_name'),-1)<>256
 OR COALESCE(COL_LENGTH(N'dbo.IQHR_CdcTable',N'table_name'),-1)<>256
 OR COALESCE(COL_LENGTH(N'dbo.IQHR_CdcTable',N'capture_fingerprint'),-1)<>128
 OR COALESCE(COL_LENGTH(N'dbo.IQHR_CdcTable',N'diagnostic'),-1)<>2000
 OR COALESCE(COL_LENGTH(N'dbo.IQHR_CdcTable',N'activation_lsn'),-1)<>64
 OR COALESCE(COL_LENGTH(N'dbo.IQHR_CdcTable',N'version'),-1)<>8
 OR COALESCE(COL_LENGTH(N'dbo.IQHR_CdcTable',N'sql_cdc_enabled'),-1)<>1
 OR COALESCE(COL_LENGTH(N'dbo.IQHR_CdcTable',N'desired_enabled'),-1)<>1
 OR COALESCE(COL_LENGTH(N'dbo.IQHR_CdcTable',N'active'),-1)<>1
 OR COALESCE(COL_LENGTH(N'dbo.IQHR_CdcTable',N'state'),-1)<>64
 OR COALESCE(COL_LENGTH(N'dbo.IQHR_CdcTable',N'observed_at'),-1)<>8
 OR COALESCE(COL_LENGTH(N'dbo.IQHR_CdcTable',N'requested_at'),-1)<>8
 OR COALESCE(COL_LENGTH(N'dbo.IQHR_CdcTable',N'enabled_at'),-1)<>8
 OR COALESCE(COL_LENGTH(N'dbo.IQHR_CdcTable',N'requested_by'),-1)<>512
    THROW 51000,'Incompatible IQHR_CdcTable; explicit reviewed migration required',1;
IF EXISTS (
    SELECT 1 FROM (VALUES
        ('table_id','nvarchar',0),('tenant_id','nvarchar',0),('source_id','nvarchar',0),
        ('schema_name','nvarchar',0),('table_name','nvarchar',0),('sql_cdc_enabled','bit',0),
        ('capture_fingerprint','nvarchar',1),('desired_enabled','bit',0),('active','bit',0),
        ('state','nvarchar',0),('diagnostic','nvarchar',1),('observed_at','datetime2',0),
        ('requested_at','datetime2',1),('enabled_at','datetime2',1),('requested_by','nvarchar',1),
        ('activation_lsn','nvarchar',1),('version','bigint',0)
    ) expected(column_name,type_name,is_nullable)
    LEFT JOIN sys.columns c ON c.object_id=OBJECT_ID(N'dbo.IQHR_CdcTable') AND c.name=expected.column_name
    LEFT JOIN sys.types t ON t.user_type_id=c.user_type_id
    WHERE c.column_id IS NULL OR t.name<>expected.type_name OR c.is_nullable<>expected.is_nullable
        OR (t.name='datetime2' AND c.scale<>7)
)
    THROW 51000,'Incompatible IQHR_CdcTable column types; explicit reviewed migration required',1;
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes i JOIN sys.index_columns ic ON ic.object_id=i.object_id AND ic.index_id=i.index_id
    JOIN sys.columns c ON c.object_id=ic.object_id AND c.column_id=ic.column_id
    WHERE i.object_id=OBJECT_ID(N'dbo.IQHR_CdcTable') AND i.is_primary_key=1 AND ic.key_ordinal=1 AND c.name=N'table_id'
      AND NOT EXISTS(SELECT 1 FROM sys.index_columns extra WHERE extra.object_id=i.object_id AND extra.index_id=i.index_id AND extra.key_ordinal>1)
)
    THROW 51000,'Incompatible IQHR_CdcTable primary key; explicit reviewed migration required',1;
