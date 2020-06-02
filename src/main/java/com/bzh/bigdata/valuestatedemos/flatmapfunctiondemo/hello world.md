# Typora介绍

## 代码块

```python
def helloworld():
    print("hello, world!")                                                                                                                                                                                    
```



| 1    | 2    | 3    |
| ---- | ---- | ---- |
|      |      |      |
|      |      |      |
|      |      |      |

[https://www.cnblogs.com/hider/p/11614688.html]: 

------

------

```sql
with now_day_data as
(
select    sd.uid uid
          ,split(sd.channel,'\\\\?')[0] channel
          ,sd.spm_value spm_value
          ,sd.action action
          ,sd.spm_time spm_time
          ,sd.other other
          ,'${before_day}' statis_day
from      dx_intelli_recharge_ods.spm_data sd
where     from_unixtime(cast(cast(sd.spm_time as bigint)/1000 as bigint),'yyyy-MM-dd') = '${before_day}'
)
```

