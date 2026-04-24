FROM public.ecr.aws/docker/library/python:3.12-slim

WORKDIR /app

COPY . .

CMD ["python", "app/generate.py"]
