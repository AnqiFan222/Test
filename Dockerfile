# 使用官方 Python 精简镜像
FROM python:3.10-slim

# 设置容器中的工作目录
WORKDIR /app

# 将你所有代码文件复制到容器中
COPY . .

# 安装必要的依赖包（你 test.py 实际用到的）
RUN pip install --no-cache-dir pandas numpy scipy openpyxl requests google-cloud-storage

# 设置容器启动时运行的脚本
CMD ["python", "test.py"]
