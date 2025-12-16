# MySQL 到 PostgreSQL 迁移工具

一个用 Python 开发的命令行工具，用于将数据库模式和数据从 MySQL 迁移到 PostgreSQL。

## 功能

- **模式比较**: 比较源（MySQL）和目标（PostgreSQL）数据库，并列出目标数据库中存在或缺失的表。
- **表格创建**: 根据 MySQL 表结构自动生成 PostgreSQL `CREATE TABLE` 语句，并映射数据类型。
- **灵活的迁移控制**:
    - `--recreate`: 即使目标数据库中的表已存在，也会删除并重新创建。
    - `--truncate`: 在迁移数据之前清空（截断）目标数据库中的现有表。
- **基于块的数据迁移**: 将数据以可配置的块大小（`--chunk-size`）传输，以高效处理大表。
- **实时进度显示**: 显示每个表的数据迁移实时进度，包括已传输的记录数。
- **配置文件**: 数据库凭据和连接详细信息在外部 `config.ini` 文件中管理，而不是硬编码。
- **Python `uv` 环境**: 使用 `uv` 进行快速、简单的 Python 环境和包管理。

## 先决条件

- Python 3.8+
- [uv](https://github.com/astral-sh/uv) (用于环境管理，也可以使用 `pip`)

## 设置和安装

1.  **克隆仓库** (或使用已创建的文件):
    ```bash
    # git clone <repository_url>
    cd mysql2postgres
    ```

2.  **创建虚拟环境**:
    使用 `uv` 创建隔离的 Python 环境。
    ```bash
    uv venv
    ```

3.  **激活虚拟环境**:
    - 在 macOS/Linux 上:
      ```bash
      source .venv/bin/activate
      ```
    - 在 Windows 上:
      ```bash
      .venv\Scripts\activate
      ```

4.  **安装依赖项**:
    从 `requirements.txt` 安装所需的 Python 包。
    ```bash
    uv pip install -r requirements.txt
    ```

## 配置

运行工具之前，您必须提供数据库连接详细信息。

1.  将 `config.ini.template` 重命名或复制为 `config.ini`。
2.  用您特定的数据库凭据编辑 `config.ini`:

    ```ini
    [mysql]
    host = localhost
    user = your_mysql_user
    password = your_mysql_password
    database = source_database_name

    [postgresql]
    host = localhost
    user = your_postgres_user
    password = your_postgres_password
    database = target_database_name
    port = 5432
    ```

## 用法

在运行任何命令之前，请确保您的虚拟环境已激活。所有命令都应从 `mysql2postgres` 目录运行。

### 1. 显示帮助
查看所有可用命令和选项：
```bash
python main.py --help
```

### 2. 模式检查（模拟运行）
比较数据库而不执行任何迁移。这是推荐的第一步，以了解当前状态。
```bash
python main.py
```
这将输出在源中找到的表列表，并指示它们是否存在于目标中。

### 3. 开始基本迁移
此命令仅迁移目标数据库中缺失的表。它不会影响现有表。
```bash
python main.py
```

### 4. 清空并迁移
这将在目标数据库中清空任何现有表，然后将新数据迁入其中。
```bash
python main.py --truncate
```

### 5. 重新创建所有表
这将**删除**目标数据库中的所有相应表，然后根据 MySQL 模式重新创建它们。

**警告**: 请谨慎使用此命令，因为它会销毁目标表中的任何现有数据和模式。
```bash
python main.py --recreate
```

### 6. 调整块大小
要控制内存使用量和迁移速度，您可以设置每次批处理中要处理的记录数。
```bash
python main.py --chunk-size 5000
```

## 数据类型映射

脚本包含一个简化函数（`map_mysql_to_postgres_type`），用于将 MySQL 数据类型转换为其 PostgreSQL 等效项。这种映射涵盖了常见类型，但可能无法完美处理所有边缘情况或自定义数据类型。如果您有复杂的模式，您可能需要在 `main.py` 中调整此函数。

## 许可证

该项目根据 MIT 许可证授权。