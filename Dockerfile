# Используем официальный образ Python
FROM python:3.12-bullseye

RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    gcc \
    curl \
    unzip \
    default-jdk \
    libglib2.0-0 \
    libnss3 \
    libx11-6 \
    xvfb

RUN apt update && apt install -y poppler-utils \
    tesseract-ocr

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Install Google Chrome
RUN wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub \
    | gpg --dearmor -o /usr/share/keyrings/google-chrome-archive-keyring.gpg && \
    echo "deb [arch=amd64 signed-by=/usr/share/keyrings/google-chrome-archive-keyring.gpg] http://dl.google.com/linux/chrome/deb/ stable main" \
    > /etc/apt/sources.list.d/google-chrome.list && \
    apt-get update && \
    apt-get install -y google-chrome-stable && \
    apt-get install tesseract-ocr-rus &&\
    apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN chmod +x /app/start.sh
ENTRYPOINT ["/app/start.sh"]