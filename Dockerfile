FROM python:3.11-slim

WORKDIR /app

# fdk — requirements.txt pulls in fdk_requirements.txt via -r; fyers-apiv3 needs --no-deps
# (its metadata pins an older requests). Revert these two lines to the plain
# `COPY requirements.txt .` / `RUN pip install ... -r requirements.txt` to remove fdk.
COPY requirements.txt fdk_requirements.txt fdk_requirements_nodeps.txt ./
RUN pip install --no-cache-dir -r requirements.txt \
 && pip install --no-cache-dir --no-deps -r fdk_requirements_nodeps.txt

# fdk — headless Chromium for the Shoonya browser login (FDK_SHOONYA_AUTO_LOGIN). Build with
# --build-arg FDK_BROWSER=0 to skip it (the Telegram-link login still works). Remove to drop fdk.
ARG FDK_BROWSER=1
RUN if [ "$FDK_BROWSER" = "1" ]; then \n      apt-get update && apt-get install -y --no-install-recommends chromium chromium-driver \n      && rm -rf /var/lib/apt/lists/*; \n    fi

COPY . .

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
