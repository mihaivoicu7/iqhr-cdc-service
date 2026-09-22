-- Fail clearly on incompatible pre-existing objects; never drop/recreate or rewrite their data.
IF EXISTS (
    SELECT 1 FROM (VALUES
        ('dbo.IQHR_CdcEvent','event_id','varchar',64,0),
        ('dbo.IQHR_CdcEvent','tenant_id','nvarchar',128,0),
        ('dbo.IQHR_CdcEvent','source_id','nvarchar',200,0),
        ('dbo.IQHR_CdcEvent','schema_name','nvarchar',256,0),
        ('dbo.IQHR_CdcEvent','table_name','nvarchar',256,0),
        ('dbo.IQHR_CdcEvent','operation','varchar',16,0),
        ('dbo.IQHR_CdcEvent','occurred_at','datetime2',8,0),
        ('dbo.IQHR_CdcEvent','captured_at','datetime2',8,0),
        ('dbo.IQHR_CdcEvent','record_key','nvarchar',-1,0),
        ('dbo.IQHR_CdcEvent','before_json','nvarchar',-1,1),
        ('dbo.IQHR_CdcEvent','after_json','nvarchar',-1,1),
        ('dbo.IQHR_CdcEvent','changed_properties','nvarchar',-1,0),
        ('dbo.IQHR_CdcEvent','commit_lsn','varchar',32,1),
        ('dbo.IQHR_CdcEvent','change_lsn','varchar',32,1),
        ('dbo.IQHR_CdcEvent','event_serial_no','bigint',8,1),
        ('dbo.IQHR_CdcStatus','source_id','nvarchar',200,0),
        ('dbo.IQHR_CdcStatus','tenant_id','nvarchar',128,0),
        ('dbo.IQHR_CdcStatus','state','varchar',32,0),
        ('dbo.IQHR_CdcStatus','message','nvarchar',2000,1),
        ('dbo.IQHR_CdcStatus','updated_at','datetime2',8,0),
        ('dbo.IQHR_CdcStatus','last_event_at','datetime2',8,1),
        ('dbo.IQHR_CdcStatus','last_commit_lsn','varchar',32,1),
        ('dbo.IQHR_CdcStatus','retention_minutes','int',4,1),
        ('dbo.IQHR_CdcStatus','retention_headroom_seconds','bigint',8,1),
        ('dbo.IQHR_CdcStatus','capture_state','varchar',32,0),
        ('dbo.IQHR_CdcStatus','consumer_state','varchar',32,0),
        ('dbo.IQHR_CdcActivation','source_id','nvarchar',200,0),
        ('dbo.IQHR_CdcActivation','tenant_id','nvarchar',128,0),
        ('dbo.IQHR_CdcActivation','incarnation','varchar',36,0),
        ('dbo.IQHR_CdcActivation','configuration_hash','varchar',64,0),
        ('dbo.IQHR_CdcActivation','created_at','datetime2',8,0),
        ('dbo.IQHR_CdcActivation','fence','bigint',8,0),
        ('dbo.IQHR_CdcOffset','id','varchar',36,0),
        ('dbo.IQHR_CdcOffset','offset_key','nvarchar',-1,1),
        ('dbo.IQHR_CdcOffset','offset_val','nvarchar',-1,1),
        ('dbo.IQHR_CdcOffset','record_insert_ts','datetime2',8,0),
        ('dbo.IQHR_CdcOffset','record_insert_seq','int',4,0),
        ('dbo.IQHR_CdcSchemaHistory','id','varchar',36,0),
        ('dbo.IQHR_CdcSchemaHistory','history_data','nvarchar',-1,0),
        ('dbo.IQHR_CdcSchemaHistory','history_data_seq','int',4,0),
        ('dbo.IQHR_CdcSchemaHistory','record_insert_ts','datetime2',8,0),
        ('dbo.IQHR_CdcSchemaHistory','record_insert_seq','int',4,0),
        ('dbo.IQHR_CdcSchemaHistory','storage_sequence','bigint',8,0),
        ('dbo.IQHR_CdcHeartbeat','singleton_id','int',4,0),
        ('dbo.IQHR_CdcHeartbeat','tick_sequence','bigint',8,0),
        ('dbo.IQHR_CdcHeartbeat','ticked_at','datetime2',8,0)
    ) AS expected(table_name,column_name,type_name,max_length,is_nullable)
    LEFT JOIN sys.columns c ON c.object_id=OBJECT_ID(expected.table_name,N'U') AND c.name=expected.column_name
    LEFT JOIN sys.types t ON t.user_type_id=c.user_type_id
    WHERE c.column_id IS NULL OR t.name<>expected.type_name OR c.max_length<>expected.max_length
        OR c.is_nullable<>expected.is_nullable OR (t.name='datetime2' AND c.scale<>7)
)
    THROW 51000, 'Incompatible CDC table columns: reviewed additive migration required', 1;
IF EXISTS (
    SELECT 1 FROM (VALUES ('dbo.IQHR_CdcEvent'),('dbo.IQHR_CdcStatus'),('dbo.IQHR_CdcActivation'),
        ('dbo.IQHR_CdcOffset'),('dbo.IQHR_CdcSchemaHistory'),('dbo.IQHR_CdcHeartbeat')) AS expected(table_name)
    WHERE NOT EXISTS(SELECT 1 FROM sys.indexes i WHERE i.object_id=OBJECT_ID(expected.table_name) AND i.is_primary_key=1)
)
    THROW 51000, 'Missing CDC primary key: reviewed additive migration required', 1;
IF COLUMNPROPERTY(OBJECT_ID(N'dbo.IQHR_CdcSchemaHistory'),'storage_sequence','IsIdentity')<>1
    THROW 51000, 'Missing CDC schema ordering identity: reviewed additive migration required', 1;
