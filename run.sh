#!/bin/bash

# Activar el entorno virtual
source /root/MercaBot/venv/bin/activate
pip install -r /root/MercaBot/requirements.txt

log_file="/root/MercaBot/logs/$(date +'%Y-%m-%d_%H-%M-%S').log"

python3 /root/MercaBot/code/main.py >> "$log_file" 2>&1

cd MercaBot
git add . 
git commit -m "Automated commit on $(date +'%Y-%m-%d')"
git push 

deactivate

