# استفاده از یک ایمیج رسمی و سبک پایتون به عنوان پایه
FROM python:3.10-slim

# تعیین پوشه کاری داخل کانتینر
WORKDIR /app

# کپی کردن فایل نیازمندی‌ها به داخل کانتینر
COPY requirements.txt .

# نصب کتابخانه‌های مورد نیاز
RUN pip install --no-cache-dir -r requirements.txt

COPY *.py .
COPY DejaVuSansMono.ttf .

# دستوری که موقع اجرای کانتینر اجرا خواهد شد
# (این دستور توسط docker-compose بازنویسی می‌شود)
CMD ["python", "collector.py"]