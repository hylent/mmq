-- 客户端
CREATE TABLE `mmq_session` (
    `session`       BIGINT UNSIGNED NOT NULL                COMMENT '客户端',
    `tube`          VARCHAR(200) NOT NULL                   COMMENT '使用的管道',
    `ts_created`    INT UNSIGNED NOT NULL DEFAULT 0         COMMENT '创建时间',
    `ts_updated`    INT UNSIGNED NOT NULL DEFAULT 0         COMMENT '更新时间',
    primary key (`session`)
)
COMMENT='客户端'
COLLATE='utf8_general_ci'
ENGINE=InnoDB
;

-- 客户端关注的管道
CREATE TABLE `mmq_session_tube` (
    `session`       BIGINT UNSIGNED NOT NULL                COMMENT '客户端',
    `tube`          VARCHAR(200) NOT NULL                   COMMENT '关注的管道',
    primary key (`session`, `tube`)
)
COMMENT='客户端关注的管道'
COLLATE='utf8_general_ci'
ENGINE=InnoDB
;

-- 任务消息
CREATE TABLE `mmq_job` (
    `id`            BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'ID',
    `data`          VARCHAR(2000) NOT NULL                  COMMENT '内容',
    `ttr`           INT UNSIGNED NOT NULL DEFAULT 0         COMMENT '最大运行时间',
    `pri`           INT UNSIGNED NOT NULL DEFAULT 0         COMMENT '优先级',
    `tube`          VARCHAR(200) NOT NULL                   COMMENT '管道',
    `state`         TINYINT UNSIGNED NOT NULL DEFAULT 0     COMMENT '状态：0就绪，1搁置，2删除',
    `session`       BIGINT UNSIGNED NOT NULL DEFAULT 0      COMMENT '客户端',
    `ts_available`  INT UNSIGNED NOT NULL DEFAULT 0         COMMENT '可用时间',
    `ts_created`    INT UNSIGNED NOT NULL DEFAULT 0         COMMENT '创建时间',
    `ts_updated`    INT UNSIGNED NOT NULL DEFAULT 0         COMMENT '更新时间',
    `cnt_reserves`  INT UNSIGNED NOT NULL DEFAULT 0         COMMENT '消费次数',
    `cnt_timeouts`  INT UNSIGNED NOT NULL DEFAULT 0         COMMENT '超时次数',
    `cnt_releases`  INT UNSIGNED NOT NULL DEFAULT 0         COMMENT '重放次数',
    `cnt_buries`    INT UNSIGNED NOT NULL DEFAULT 0         COMMENT '搁置次数',
    `cnt_kicks`     INT UNSIGNED NOT NULL DEFAULT 0         COMMENT '激活次数',
    primary key (`id`)
)
COMMENT='任务消息'
COLLATE='utf8_general_ci'
ENGINE=InnoDB
;
