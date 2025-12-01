# 使用官方 Python 镜像
FROM python:3.10-slim

# 设置工作目录
WORKDIR /app

# 把当前目录的所有文件复制进容器
COPY . .

# 安装 openpyxl，用于写 Excel
RUN pip install --no-cache-dir openpyxl pandas

# 执行脚本
CMD ["python", "test.py"]
