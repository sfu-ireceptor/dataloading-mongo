FROM mongo:3

# add this folder to the Docker image
COPY . /app

# download mapping file
ADD https://raw.githubusercontent.com/sfu-ireceptor/config/turnkey/AIRR-iReceptorMapping.txt /app/config/

# set working directory
WORKDIR /root
