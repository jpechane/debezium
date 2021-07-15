-- Create Table
create table debezium.products (id NUMBER(4) GENERATED BY DEFAULT ON NULL AS IDENTITY (START WITH 101) NOT NULL PRIMARY KEY, name VARCHAR2(255) NOT NULL, description VARCHAR2(512), weight FLOAT);
create table debezium.products (id NUMBER(4) GENERATED BY DEFAULT ON NULL AS IDENTITY (START WITH 101 INCREMENT BY 1 CYCLE CACHE 200) NOT NULL PRIMARY KEY, name VARCHAR2(255) NOT NULL, description VARCHAR2(512), weight FLOAT);
create global temporary table sys.ora_temp_1_ds_1550399 sharing=none on commit preserve rows cache noparallel as select /*+ no_parallel(t) no_parallel_index(t) dbms_stats cursor_sharing_exact use_weak_name_resl dynamic_sampling(0) no_monitoring xmlindex_sel_idx_tbl opt_param('optimizer_inmemory_aware' 'false') no_substrb_pad */"ENTRYUUID", rowid SYS_DS_ALIAS_0 from "IDENTITYDB"."OAUTH2_CLIENT_CHANGE_LOGS" sample ( 10.0000000000) t WHERE 1 = 2;
CREATE TABLE "ABCD_SHARD_1_3"."D_PLAN_SCHEDULE_LOT_ENTRY"("ID" NUMBER(38,0) NOT NULL ENABLE, "LOT_ID" NUMBER(38,0) NOT NULL ENABLE, "PLAN_SCHEDULE_ID" NUMBER(38,0) NOT NULL ENABLE, "IS_ACTUAL" NUMBER(1,0) NOT NULL ENABLE, "OOS_POSITION_NUMBER" NVARCHAR2(50) DEFAULT 0, CONSTRAINT "PK_PSLE_ENTRY_ID" PRIMARY KEY ("ID") USING INDEX  ENABLE, SUPPLEMENTAL LOG DATA (PRIMARY KEY) COLUMNS, SUPPLEMENTAL LOG DATA (UNIQUE INDEX) COLUMNS, SUPPLEMENTAL LOG DATA (FOREIGN KEY) COLUMNS, CONSTRAINT "FK_PSLE_LOT_VERSION" FOREIGN KEY ("LOT_ID") REFERENCES "ABCD_SHARD_1_3"."D_LOT_VERSION" ("ID") DISABLE, CONSTRAINT "FK_PSLE_PLAN_SCHEDULE_VERSION" FOREIGN KEY ("PLAN_SCHEDULE_ID") REFERENCES "ABCD_SHARD_1_3"."D_PLAN_SCHEDULE_VERSION" ("ID") ENABLE );
CREATE TABLE IDATA_BAIIR.CUST_INFO (ID NUMBER(20) NOT NULL ENABLE, CUST_NAME VARCHAR2(200 CHAR) NOT NULL ENABLE, CUST_NAME_PY VARCHAR2(500 CHAR), CUST_NAME_PY_SZ VARCHAR2(50 CHAR), CUST_NAME_ABBR VARCHAR2(100 CHAR), CUST_NAME_ABBR_PY VARCHAR2(300 CHAR), CUST_NAME_ABBR_PY_SZ VARCHAR2(50 CHAR), CUST_NAME_EN VARCHAR2(100 CHAR), CUST_TYPE NUMBER(10), CUST_STATUS NUMBER(10), CUST_LEVEL NUMBER(10), MEMO VARCHAR2(500 CHAR), TEL_NUM VARCHAR2(100 CHAR), FAX_NUM VARCHAR2(100 CHAR), INTERNET_ADDRESS VARCHAR2(200 CHAR), COMPANY_ADDRESS VARCHAR2(200 CHAR), POST_CODE VARCHAR2(50 CHAR), EMAIL_ADDRESS VARCHAR2(200 CHAR), CUST_VALID_FLAG NUMBER(1), EXP_DATE DATE, CERT_TYPE NUMBER(10), CERT_NUM VARCHAR2(100 CHAR), CERT_CUST_NAME VARCHAR2(200 CHAR), RISK_LEVEL NUMBER(10), FXPP_CONFIRM_PROCESS NUMBER(10), FXPP_CONFIRM_PROCESS_INFO NUMBER(10), SDX_INFO_AUDITOR VARCHAR2(100 CHAR), SDX_NEW_COM NUMBER(1), ECIF_OTC_FLAG NUMBER(1), AUDIT_STATUS NUMBER(1), IS_VALID NUMBER(1) DEFAULT 1 NOT NULL ENABLE, CREATE_TIME DATE, CREATOR NUMBER(20), CREATOR_NAME VARCHAR2(30 CHAR), MODIFY_TIME DATE, MODIFIER NUMBER(20), MODIFIER_NAME VARCHAR2(30 CHAR)) tablespace BIGDATADBT pctfree 10 initrans 1 maxtrans 255 storage ( initial 192K next 1M minextents 1 maxextents unlimited );
create table dbz1211 (id number(38) not null, data varchar2(50), constraint name primary key (id) using index tablespace ts1) tablespace ts2;
CREATE TABLE "HR"."COUNTRIES"( "COUNTRY_ID" CHAR(2) CONSTRAINT "COUNTRY_ID_NN" NOT NULL ENABLE,"COUNTRY_NAME" VARCHAR2(40),"REGION_ID" NUMBER,CONSTRAINT "COUNTRY_C_ID_PK" PRIMARY KEY ("COUNTRY_ID") ENABLE,SUPPLEMENTAL LOG DATA (ALL) COLUMNS,CONSTRAINT "COUNTR_REG_FK" FOREIGN KEY ("REGION_ID")REFERENCES "HR"."REGIONS" ("REGION_ID") ENABLE) ORGANIZATION INDEX NOCOMPRESS;
CREATE TABLE "VELEBIT_ZAVAR_PROD"."PON_PRIJENOS" ("SIF_AGENCIJE" NUMBER DEFAULT 0 NOT NULL ENABLE, "DAT_PRIJENOSA" DATE NOT NULL ENABLE, "VRS_POLICE" NUMBER DEFAULT 0 NOT NULL ENABLE, "BR_ZAPRIMLJENIH" NUMBER DEFAULT 0, "BR_POLICIRANIH" NUMBER DEFAULT 0) SEGMENT CREATION IMMEDIATE PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 ROW STORE COMPRESS ADVANCED LOGGING STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645 PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT) TABLESPACE "TS_VELEBIT" PARALLEL 8 ;
-- Create index
create index hr.name on hr.table (id,data) tablespace ts;