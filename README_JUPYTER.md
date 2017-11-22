#Jupyter Notebook Docker (deprecated?)#

When the turnkey node is running, one of the Docker containers which is fired up is a Jupyter Notebook which is a sandbox of script development to facilitate data loading into the turnkey node's MongoDb database. This Notebook may be accessed using a web browser; however, the default is for the notebook to be protected by an authentication token. To see the full local link for this token, you need to look inside the log file of the Jupyter container. This is easily done as follows:

```
$ sudo docker logs irdn-notebook  # where 'irdn-notebook' is the default name of the Jupyter Notebook
```
Clicking on this link should give you the notebook home page with README and a *work* directory with data loading scripts under development.

Note, however, that the Python loading scripts in the Notebook may be out-of-date and rather, the script documented in the main README of this project are likely the preferred channel for data loading.