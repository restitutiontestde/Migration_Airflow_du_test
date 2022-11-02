# Migration_Airflow_du_test

Migration de l'ETL codé dans le projet : 
## https://github.com/restitutiontestde/Partie-python-restitution.git
vers une version orchestré par Airflow

# Documentation
Nous avons choisi Docker comme solution pour lancer notre pipeline en local.

Nous avons copier le répertoir "etl_src" depuis le projet 
https://github.com/restitutiontestde/Partie-python-restitution.git dans le dossier dags du présent projet.

Ensuite, nous avons ajouté un script "etl_pipeline_dags.py" pour lancer notre ETL sur airflow.


# Etapes d'installation du projet
    1. cloner ce repository : 
        git clone https://github.com/restitutiontestde/Migration_Airflow_du_test.git
    2. Installer Docker
    3. Installer Docker-compose
    4. lancer la commande suivante en root du projet : 
        docker-compos up -d
        
