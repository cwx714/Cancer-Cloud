# 分析平台接口

## 报警规则

报警代码 |  类型  | 名称          | 规则                    | 参数                   | 优先级
-------- | ------ | ------------- | ----------------------- | ---------------------- | -----
11000    | 车      | 超速          | 连续N毫秒超过速度X     | `1`: 提醒间隔(秒)      | 2
11001    | 车      | 疲劳驾驶      | 在启动状态超过N毫秒    | `1`: 提醒间隔(秒) `2`: 连续休息时长 `3`: 累计休息时长 `4`: 最小有效休息时长 `5`: 预疲劳提前量 `6`: 最小行驶距离      | 2
11004    | 车      | 进入区域      |                        |                        | 2
11005    | 车      | 驶出区域      |                        |                        | 2
12001    | 电池    | 低电量        |                        |                        | 2
12002    | 电池    | 单体欠压      |                        |                        | 2
12003    | 电池    | 单体过压      |                        |                        | 2
12004    | 电池    | 电池压差      |                        |                        | 2
12005    | 电池    | 相邻两串压差  |                        |                        | 1
12006    | 电池    | 充高放低      |                        |                        | 2
12007    | 电池    | 温度过高      |                        |                        | 2
12008    | 电池    | 温感线异常    |                        |                        | 2
12009    | 电池    | 绝缘过低      |                        |                        | 2
12010    | 电池    | SOC过低       |                        |                        | 2
12011    | 电池    | 温差报警      |                        |                        | 2

超速报警RULE:

早上8:00 - 20:00 超过120KM或低于30KM报警
其他时间，超过100KM或低于30KM报警

提醒间隔120秒

```json
{
  "rule_id": 1,
  "alarm_code": 11000,
  "alarm_source": 20000,
  "device_type": "DEVICE_VEHICLE_GB_V1",
  "device_ids": [],
  "configs": {
    "1": 120
  },
  "conditions": [
    {
      "alarm_level": 1,
      "start_time": 480,
      "stop_time": 1200,
      "duration": 1,
      "logical": "OR",
      "expressions": [
        {
          "operator": "LT",
          "value": {
            "l": 30
          }
        },
        {
          "operator": "GT",
          "value": {
            "l": 120
          }
        }
      ]
    },
    {
      "alarm_level": 1,
      "start_time": 0,
      "stop_time": 480,
      "duration": 1,
      "logical": "OR",
      "expressions": [
        {
          "operator": "LT",
          "value": {
            "l": 30
          }
        },
        {
          "operator": "GT",
          "value": {
            "l": 100
          }
        }
      ]
    },
    {
      "alarm_level": 1,
      "start_time": 1200,
      "stop_time": 1440,
      "duration": 1,
      "logical": "OR",
      "expressions": [
        {
          "operator": "LT",
          "value": {
            "l": 30
          }
        },
        {
          "operator": "GT",
          "value": {
            "l": 100
          }
        }
      ]
    }
  ]
}
```

疲劳驾驶 RULE:

```json
{
  "rule_id": 1,
  "alarm_code": 11001,
  "alarm_source": 20000,
  "device_type": "DEVICE_VEHICLE_GB_V1",
  "device_ids": [],
  "configs": {
    "1": 120,
    "2": 7200,
    "3": 7200,
    "4": 3600,
    "5": 1800,
    "6": 5
  },
  "conditions": [
    {
      "alarm_level": 1,
      "start_time": 480,
      "stop_time": 1200,
      "duration": 1,
      "logical": "AND",
      "expressions": [
        {
          "operator": "GT",
          "value": {
            "l": 14400
          }
        }
      ]
    },
    {
      "alarm_level": 1,
      "start_time": 0,
      "stop_time": 480,
      "duration": 1,
      "logical": "AND",
      "expressions": [
        {
          "operator": "GT",
          "value": {
            "l": 10800
          }
        }
      ]
    },
    {
      "alarm_level": 1,
      "start_time": 1200,
      "stop_time": 1440,
      "duration": 1,
      "logical": "AND",
      "expressions": [
        {
          "operator": "GT",
          "value": {
            "l": 10800
          }
        }
      ]
    }
  ]
}
```

## 报表

* 周期定义:
    * `A`: 所有, 从统计开始到当前的累计值
    * `R`: 实时状态

* 代码定义:
    * 第一位:
        * `1`: 车
        * `2`: 桩
    * 第二位:
        * `0`: 综合
        * `1`: 实时状态数据
        * `2`, 报警
        * `3`: 事件
        * `4`: 基础数据
    * 第三位:
        * `0`: 综合
        * `1`: HTA
        * `2`: RTA

* `access_id`: `device_type`:`device_id`

报表代码  | 名称  | 类型  | 周期  | 分组项  | 统计项  | 优先级
-------- | ---- | ----- | ---- | ------ | ------ | -------
102001   | 车辆性质               | RTA      | R      | `Base.natureId`           | `数量`                                                                          | 1
111001   | 行驶记录               | RTA,HTA  | 次     | `access_id:S`             | `开始里程:L`,  `结束里程:L`, `开始时间:L`, `结束时间:L`                         | 1
111002   | 行驶统计(运营商、品牌)   | HTA      | 日     | `品牌:I`, `运营企业:I`     | `次均行驶里程:L`,  `日均行驶里程:L`, `次均行驶时长:L`, `日均行驶时长:L`, `日均出行次数:I` | 1
111003   | 超速                   | RTA,HTA  | 次     | `access_id:S`             | `开始时间:L`,  `结束时间:L`,  `平均速度:L`                                      | 1
111004   | 疲劳驾驶               | RTA,HTA  | 次     | `access_id:S`             | `开始时间:L`,  `结束时间:L`, `总里程:L`, `平均速度:L`                           | 1
112001   | 投运车辆               | RTA      | 日,A   | `device_id`               | `数量`                                                                          | 1
112002   | 行驶里程               | RTA      | 时,A   |                           | `里程`                                                                          | 1
112003   | 车辆状态               | RTA      | R      | `power_status`            | `数量`                                                                          | 1
112004   | 车辆状态开始时间        | RTA      | R      | `power_status`            | `时间`                                                                          | 1
122001   | 车辆告警数             | RTA      | 时,A   | `alarm_code`              | `数量`                                                                          | 1
211001   | 充电运营统计表          | HTA      | 日     |                           | `已投运充电设施数量:I`, `充电中数量:I`, `故障数量:I`, `累计充电量:L`, `累计充电次数:I`, `当日充电量:L`, `当日累计充电次数:I` | 1
212001   | 投运电桩               | RTA      | 日,A   | `device_id`               | `数量`                                                                          | 1
212002   | 充电量及次数           | RTA,HTA  | 时,日,A | `access_id:S`             | `充电量:I`, `次数:I`                                                            | 1
212003   | 充电桩状态             | RTA      | R      | `evse_status`             | `数量`                                                                          | 1
212004   | 充电桩状态开始时间      | RTA      | R      | `evse_status`             | `数量`                                                                          | 1
212005   | 充电枪状态             | RTA      | R      | `plug_status`             | `时间`                                                                          | 1
222001   | 充电桩告警数           | RTA      | 时,A   | `alarm_code`              | `数量`                                                                          | 1

