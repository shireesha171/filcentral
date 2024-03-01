FROM public.ecr.aws/lambda/python:3.10
LABEL authors="dataplatform-team"

# Source Code
COPY ./functions/fileops-FileConverter/ ${LAMBDA_TASK_ROOT}

RUN pip install -r requirements.txt -t .

# Layers
COPY ./layers/fileops-essentials-layer/python/dbconnection.py /opt/python/dbconnection.py

CMD [ "code.app.lambda_handler" ]