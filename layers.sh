cd layers/fileops-essentials-layer/python/
pip install -r requirements.txt -t .
cd ../../
cd fileops-jinja2-layer/python/
pip install -r requirements.txt -t .
cd ../../..
sam validate
sam deploy --template-file template.yaml --stack-name fileops-stack-dev --no-confirm-changeset --resolve-image-repos  --parameter-overrides Environment=dev corsDomain=* --capabilities CAPABILITY_NAMED_IAM
