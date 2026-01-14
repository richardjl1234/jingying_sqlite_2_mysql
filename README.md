# SQLite 到 MySQL 数据迁移工具

本项目是一个用于将工资单数据从 SQLite 数据库迁移到 MySQL 数据库的工具集。

## 功能概述

该工具集主要用于工资单系统的数据迁移，包括定额数据、员工信息、代码映射等，支持从 SQLite 读取数据并导入到 MySQL 数据库。

## 项目结构

```
sqlite_2_mysql/
├── .gitignore                    # Git 忽略配置
├── README.md                     # 项目说明文档
├── sql_util.py                   # 数据库工具类（SQLite/MySQL 连接管理）
├── load_quota_to_mysql.py        # 定额数据迁移主程序
├── load_cat1_code.py             # 类别1代码导入
├── cat2_code.py                  # 类别2代码生成（拼音首字母）
├── load_motor_models.py          # 电机型号导入
├── process_code.py               # 加工工序代码生成（拼音首字母）
├── load_worker_code.py           # 员工代码迁移
├── demo.ipynb                    # Jupyter 演示文件
└── plans/                        # 计划文档目录
    ├── load_quota_to_mysql_plan.md
    └── load_quota_to_mysql_mysql_plan.md
```

## 核心功能模块

### 1. 数据库工具 ([`sql_util.py`](sql_util.py))

提供 SQLite 和 MySQL 数据库的连接和执行功能：

- `sqlite_sql()` - 执行 SQLite 数据库查询
- `mysql_sql()` - 执行 MySQL 数据库查询（带重试机制）
- 支持环境变量配置数据库连接 URL
- 默认 SQLite 路径：`/home/richard/shared/jianglei/payroll/payroll_database.db`
- 默认 MySQL：腾讯云数据库（需配置环境变量）

### 2. 定额数据迁移 ([`load_quota_to_mysql.py`](load_quota_to_mysql.py))

将 SQLite 中的定额数据迁移到 MySQL：

- 从 SQLite 读取定额数据（包含类别1、类别2、型号、加工工序、定额、生效日期）
- 计算每条记录的失效日期（obsolete_date）
  - 同一组内下一条记录的生效日期减1天
  - 组内最后一条记录的失效日期为 99991231
  - 单条记录的失效日期为 99991231
- 使用代码字典将数据映射到 MySQL 表结构
- 批量导入到 MySQL 的 quotas 表

#### Excel 导出功能 ([`export_quota_to_excel()`](load_quota_to_mysql.py:440))

将定额数据导出到 Excel 文件 (`定额.xlsx`)：

- **工作表命名**：格式为 `{类别1名称} {类别1代码} {生效日期}`
  - 示例：`绕嵌排 C06 2020-03-01`
  - 按类别1代码和生效日期排序
  - 自动处理无效字符和长度限制（31字符）

- **表格结构**：
  - 类别2标题：显示类别2名称和代码，格式为 `{名称} ({代码})`，**粗体蓝色字体**
  - 列名：格式为 `{加工工序名称}\n({加工工序代码})`，**浅黄色背景**
  - 行索引：显示型号名称和代码，格式为 `{名称} ({代码})`，**浅粉色背景**
  - 列宽：自动调整以适应内容
  - 行高：列标题行高度为默认的2倍（30）

- **数据排序**：
  - 型号按前缀数字排序
  - 排序键：`int(model_code.split('-')[0])`
  - 示例：`63-1` → 63, `100-2` → 100, `999` → 999
  - 排序结果：`63-1`, `100-2`, `999`

- **输出文件**：定额.xlsx

### 3. 代码生成工具

#### 类别1代码导入 ([`load_cat1_code.py`](load_cat1_code.py))

从 Excel 文件 (`cat1_code.xlsx`) 导入类别1代码到 MySQL 的 `process_cat1` 表。

#### 类别2代码生成 ([`cat2_code.py`](cat2_code.py))

根据中文名称生成类别2代码（使用拼音首字母）：

- 忽略标点符号
- 保留英文字符和数字
- 中文转换为拼音首字母大写
- 处理重复代码（添加后缀 `_1`, `_2` 等）
- 导入到 MySQL 的 `process_cat2` 表

#### 加工工序代码生成 ([`process_code.py`](process_code.py))

根据中文名称生成加工工序代码（使用拼音首字母）：

- 处理逻辑与类别2代码相同
- 导入到 MySQL 的 `processes` 表

#### 电机型号导入 ([`load_motor_models.py`](load_motor_models.py))

从 Excel 文件 (`model_code.xlsx`) 导入电机型号到 MySQL 的 `motor_models` 表。

### 4. 员工代码迁移 ([`load_worker_code.py`](load_worker_code.py))

将员工信息从 SQLite 迁移到 MySQL：

- 从 SQLite 的 `payroll_details` 表查询所有不同的员工姓名
- 生成员工代码（格式：`W001`, `W002`, ...）
- 导入到 MySQL 的 `workers` 表

## 安装依赖

```bash
pip install pandas sqlalchemy pymysql pypinyin openpyxl
```

## 环境配置

需要配置以下环境变量：

| 环境变量 | 说明 | 默认值 |
|---------|------|--------|
| `SQLITE_DB_URL` | SQLite 数据库连接 URL | `sqlite:////home/richard/shared/jianglei/payroll/payroll_database.db` |
| `MYSQL_DB_URL` | MySQL 数据库连接 URL | 腾讯云 MySQL 实例 |

示例：
```bash
export MYSQL_DB_URL="mysql+pymysql://用户名:密码@主机:端口/数据库名"
```

## 使用方法

### 迁移定额数据

```bash
python load_quota_to_mysql.py
```

该脚本会：
1. 从 SQLite 读取定额数据并计算失效日期
2. 从 MySQL 获取代码字典
3. 将数据映射并导入 MySQL

### 导入类别1代码

```bash
python load_cat1_code.py
```

### 生成并导入类别2代码

```bash
python cat2_code.py
```

### 生成并导入加工工序代码

```bash
python process_code.py
```

### 导入电机型号

```bash
python load_motor_models.py
```

### 迁移员工代码

```bash
python load_worker_code.py
```

## MySQL 表结构

### quotas 表

| 字段 | 类型 | 说明 |
|-----|------|------|
| cat1_code | VARCHAR | 类别1代码 |
| cat2_code | VARCHAR | 类别2代码 |
| model_code | VARCHAR | 型号代码 |
| process_code | VARCHAR | 加工工序代码 |
| unit_price | DECIMAL | 定额单价 |
| effective_date | DATE | 生效日期 |
| obsolete_date | DATE | 失效日期 |
| created_by | INT | 创建人 |
| created_at | DATETIME | 创建时间 |

### 代码表

- `process_cat1` - 类别1代码表
- `process_cat2` - 类别2代码表
- `processes` - 加工工序代码表
- `motor_models` - 电机型号表
- `workers` - 员工表

## 代码生成规则

代码生成采用拼音首字母方案：

| 输入 | 输出 | 说明 |
|-----|------|------|
| "2人校正" | "2RXZ" | 数字保留，中文取首字母 |
| "Y2后装" | "Y2HZ" | 英文和数字保留 |
| "精车" | "JC" | 纯中文取首字母 |

## 注意事项

1. 运行前请确保 MySQL 数据库连接配置正确
2. 建议先备份目标数据库再执行导入操作
3. 代码生成时遇到重复代码会自动添加数字后缀
4. 批量导入使用 100 条/批次
5. MySQL 查询具有重试机制（最多5次重试）

## 技术栈

- **Python 3.x**
- **pandas** - 数据处理
- **SQLAlchemy** - 数据库连接
- **PyMySQL** - MySQL 驱动
- **pypinyin** - 中文拼音转换
- **openpyxl** - Excel 文件读写
