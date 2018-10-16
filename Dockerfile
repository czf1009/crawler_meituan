# FROM python:3
FROM python3:pkg_installed
# FROM ubuntu-upstart

WORKDIR /home/project

# COPY requirements.txt ./
# RUN pip install --no-cache-dir -r requirements.txt

# COPY . .

# CMD [ "python", "./get_tenant_id.py" ]

EXPOSE 80