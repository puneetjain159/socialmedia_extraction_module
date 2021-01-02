FROM public.ecr.aws/lambda/python:3.8

# Copy function code
COPY googleanalytics/main.py ${LAMBDA_TASK_ROOT}
COPY . .

RUN pip install -r requirement.txt

# EXPOSE PORT 
EXPOSE 8080

# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
CMD [ "main.handler" ]


