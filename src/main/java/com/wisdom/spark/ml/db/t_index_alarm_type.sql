use wsd;
DROP TABLE IF EXISTS t_index_alarm_type;
CREATE TABLE IF NOT EXISTS t_index_alarm_type
(
tgtvar VARCHAR(40) not null ,
hostnode VARCHAR(20) not null ,
period VARCHAR(2) not null ,
model_typ VARCHAR(20) not null,
flag CHAR(2) not null,
recordTime TIMESTAMP not null DEFAULT CURRENT_TIMESTAMP,
reserveCol1 VARCHAR(50),
reserveCol2 VARCHAR(50)
--,primary key(tgtvar,hostnode,period,recordTime)
)DEFAULT CHARSET=utf8;

alter table wsd.t_index_alarm_type modify tgtvar VARCHAR(40) COMMENT '指标名';
alter table wsd.t_index_alarm_type modify hostnode VARCHAR(20) COMMENT '主机节点';
alter table wsd.t_index_alarm_type modify period VARCHAR(2) COMMENT '预测周期';
alter table wsd.t_index_alarm_type modify model_typ VARCHAR(20) COMMENT '模型类型简称';
alter table wsd.t_index_alarm_type modify flag CHAR(2) COMMENT '00-无效，01有效';
alter table wsd.t_index_alarm_type modify recordTime TIMESTAMP COMMENT '插入记录的时间戳';
alter table wsd.t_index_alarm_type modify reserveCol1 VARCHAR(50) COMMENT '备用字段';
alter table wsd.t_index_alarm_type modify reserveCol2 VARCHAR(50) COMMENT '备用字段';

--test data
insert into wsd.t_index_alarm_type values ('IDLE_CPU','ASGAAC01','0','WINSTATS','01','2017-05-17 16:42:50','NULL','NULL');
insert into wsd.t_index_alarm_type values ('GAAC_AVG_TRANS_TIME','ASGAAC01','0','WINSTATS','01','2017-05-17 16:42:50','NULL','NULL');
