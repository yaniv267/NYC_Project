FROM apache/airflow:2.0.0

USER root

# תיקון שרתי הארכיון של דביאן (הכרחי לגרסה 2.0.0)
RUN sed -i 's/deb.debian.org/archive.debian.org/g' /etc/apt/sources.list && \
    sed -i 's|security.debian.org/debian-security|archive.debian.org/debian-security|g' /etc/apt/sources.list && \
    sed -i '/buster-updates/d' /etc/apt/sources.list

# התקנת Docker CLI בגרסה 27.0.3 (תומכת ב-API 1.45 ומעלה)
RUN apt-get update && apt-get install -y curl && \
    curl -L https://download.docker.com/linux/static/stable/x86_64/docker-27.0.3.tgz | tar -xz -C /tmp && \
    mv /tmp/docker/docker /usr/bin/docker && \
    chmod +x /usr/bin/docker && \
    rm -rf /tmp/docker

# מתן הרשאות למשתמש airflow להריץ פקודות דוקר
RUN usermod -aG root airflow

USER airflow

# התקנת ספריות פייתון עבור הפרודוסר
RUN pip install kafka-python requests