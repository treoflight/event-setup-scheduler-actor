# Use Apify’s Python base image
FROM apify/actor-python:3.11

# Copy files into container
COPY . /app
WORKDIR /app

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Run your Python script
CMD ["python", "main.py"]
