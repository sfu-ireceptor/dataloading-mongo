FROM python:3.6

# install python modules
COPY requirements.txt /
RUN pip install -r /requirements.txt

# add PHP requirements (PHP 7.4 specific). If the version changes, the
# reference to /etc/php/7.4/cli/php.ini will need to be changed.
RUN apt-get update
RUN apt-get install php -y
RUN apt-get install php-dev -y
RUN pecl install mongodb
RUN php -v
RUN echo "extension=mongodb.so" >> /etc/php/7.4/cli/php.ini
RUN curl -sS https://getcomposer.org/installer |php
RUN mv composer.phar /usr/local/bin/composer
RUN composer require mongodb/mongodb

# add this folder to the Docker image
COPY . /app

RUN mkdir /config /app/config
RUN ln -s /config/AIRR-iReceptorMapping.txt /app/config/AIRR-iReceptorMapping.txt


# set working directory
WORKDIR /root
