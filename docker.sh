aws ecr get-login-password --region us-east-2 | docker login --username AWS --password-stdin 478313837588.dkr.ecr.us-east-2.amazonaws.com
docker build --platform linux/amd64 -t fileops-fileconverter -f functions/fileops-FileConverter/Dockerfile . 
docker tag fileops-fileconverter:latest 478313837588.dkr.ecr.us-east-2.amazonaws.com/fileops-fileconverter:latest
docker push 478313837588.dkr.ecr.us-east-2.amazonaws.com/fileops-fileconverter:latest
