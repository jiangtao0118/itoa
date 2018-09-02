use wsd;
DROP TABLE IF EXISTS T_MODEL_TRAIN_TEST_PARAS;
CREATE TABLE T_MODEL_TRAIN_TEST_PARAS(
 tgtvar               VARCHAR  (40 )        COMMENT 'PK,指标，IDLE_CPU等                            '
,hostnode             VARCHAR  (20 )        COMMENT 'PK,主机节点                                    '
,period               VARCHAR  (20 )        COMMENT 'PK,预测周期，5、15、30、1H                     '
,corrThreshold        FLOAT                 COMMENT '相关系数阈值,用于筛选变量时的阈值              '
,clusters             Int                   COMMENT '独立高斯个数                                   '
,maxIter              Int                   COMMENT '最大迭代次数                                   '
,tolerance            FLOAT                 COMMENT '收敛阈值                                       '
,seed                 Long                  COMMENT '随机种子                                       '
,betaMin              FLOAT                 COMMENT 'DAEM参数，退回因子下界，取值0-1，默认为1       '
,betaStep             FLOAT                 COMMENT 'DAEM退火因子步长，取值0-1，默认为0             '
,betaMax              FLOAT                 COMMENT 'DAEM参数，退回因子上界，取值1-2，默认为1       '
,trainFileURI         VARCHAR  (256)        COMMENT '训练文件全路径                                 '
,trainRecordCnt       Long                  COMMENT '训练数据记录数                                 '
,trainCorrCost        Long                  COMMENT '训练数据降维耗时，毫秒                         '
,traindmCnt           Int                   COMMENT '训练数据降维后维度                             '
,trainCost            Long                  COMMENT '训练GMM模型耗时，毫秒                          '
,testFileURI          VARCHAR  (256)        COMMENT '测试文件全路径                                 '
,testOutputFileURI    VARCHAR  (256)        COMMENT '测试结果输出文件全路径                         '
,testRecordCnt        Long                  COMMENT '测试数据记录数                                 '
,testCost             Long                  COMMENT '测试模型耗时，毫秒                             '
,testCorr             FLOAT                 COMMENT '测试输出实际值与预测值相关系数                 '
,testMAE              FLOAT                 COMMENT '测试输出平均绝对误差                           '
,testMAER             FLOAT                 COMMENT '测试输出平均绝对误差率                         '
,testSSE              FLOAT                 COMMENT '测试输出误差平方和                             '
,recordTime           TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6)            COMMENT 'PK,插入此记录条目的时间戳，建议由数据库自更新  '
,reserveCol1          VARCHAR  (3000 )      COMMENT '备用字段                                       '
,reserveCol2          VARCHAR  (50 )        COMMENT '备用字段                                       '
,reserveCol3          VARCHAR  (50 )        COMMENT '备用字段                                       '
,reserveCol4          VARCHAR  (50 )        COMMENT '备用字段                                       '
,reserveCol5          VARCHAR  (50 )        COMMENT '备用字段                                       '
);

--存储筛选出的字段
alter table T_MODEL_TRAIN_TEST_PARAS modify column reserveCol1 varchar(3000);
