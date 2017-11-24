## metric 发布方式
我们基于http的方式发布\管理监控项目

### 接口描述
```
http://localhost:8009/mesa/execute?type=${type}&id=${mAlarmId}&group=${alarmMetric}

```
#### 参数描述
 - type 取值为`start` , `suspend`, `resume` , `kill` , `query`,分别代表启动, 暂停, 恢复, 删除, 查询操作
 - id 取值为json串中的 `mAlarmId`
 - group 取值为json串中的 `alarmMetric`
                                "
## 接口方式
我们提供`get` , `post` 两种接口方式.

### post 接口
- JSON 方式
```
#-- 方式一
curl -i -X POST -H "'Content-type':'application/x-www-form-urlencoded', 'charset':'utf-8', 'Accept': 'text/plain'" -d 'json_data={"a":"aaa","b":"bbb","data":[{"c":"ccc",
"d":"ddd","keywords":[{"e": "eee", "f":"fff", "g":"ggg"}]}]}' http://localhost:8009/mesa/execute

#-- 方式二
curl -H "Content-Type: application/json" -X POST  --data '{"data":"1"}' http://127.0.0.1/

```

- file方式
```
curl -H "Content-Type: application/json" -X POST  --data  @docs/metrics/di_mesa_bhv_online.json  http://localhost:8009/mesa/execute

```

- json参数说明

### get 接口
```
http://localhost:8009/mesa/execute?type=${type}&id=${mAlarmId}&group=${alarmMetric
```
其中`type=query`时,可以不用 `id` 和 `group` 参数

### 批量发布脚本
项目根目录中 ` bin/submit-metrics.sh  `
