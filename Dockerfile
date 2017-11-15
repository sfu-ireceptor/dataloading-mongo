# Base Jupyter Image
# See https://hub.docker.com/r/jupyter/scipy-notebook/
FROM jupyter/scipy-notebook

LABEL maintainer="iReceptor <ireceptor@sfu.ca>"

# PROXY: uncomment these and define if building behind a proxy
# These are UTSW proxy settings
#ENV http_proxy 'http://proxy.swmed.edu:3128/'
#ENV https_proxy 'https://proxy.swmed.edu:3128/'
#ENV HTTP_PROXY 'http://proxy.swmed.edu:3128/'
#ENV HTTPS_PROXY 'https://proxy.swmed.edu:3128/'