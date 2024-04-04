# 参会数据统计小工具

## 环境准备

1. 安装python3。

2. 安装``openpyxl``库，执行命令``py -m pip install openpyxl``。

3. 使用Windows操作系统。

## 操作步骤

1. 新建节气目录，如``1.冬至立志``。

2. ``生活修行考勤表.xlsx``，``考勤数据.xlsx``，放入节气目录中。

3. 1. 统计参会时长：执行命令``py .\meeting_main.py stat_time .\1.冬至立志\``。

   2. 统计缺勤人数：执行命令``py .\meeting_main.py stat_absent .\1.冬至立志\``。

4. 填充后的表格``生活修行考勤表（生成）.xlsx``将生成在节气目录中。

## 开发说明

1. 安装``pytest``库，执行命令``py -m pip install pytest``。

2. 运行测试，执行命令``py -m pytest``。
