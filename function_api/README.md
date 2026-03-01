# Azure Function: GetFeedback

Ce dossier contient une Azure Function Python simple qui permet de faire une requête HTTP vers une API externe et de renvoyer le résultat.
L'objectif est de déployer rapidement un service qui récupère les données depuis votre API (par exemple celle fournie par `feedback_api`).

## Structure

```
function_api/
├── GetFeedback/
│   ├── __init__.py       # code de la fonction
│   └── function.json     # bindings HTTP
├── host.json             # configuration de l'hôte Functions
├── local.settings.json   # paramètres locaux (ne pas pousser en prod)
├── requirements.txt      # dépendances Python
└── .funcignore           # fichiers à ignorer lors du déploiement
```

## Utilisation locale

1. Installer l'[Azure Functions Core Tools](https://learn.microsoft.com/azure/azure-functions/functions-run-local) et Python 3.11 (ou version supportée).
2. Positionnez-vous dans ce dossier :
   ```powershell
   cd function_api
   ```
3. Créer un environnement virtuel et installer les dépendances :
   ```powershell
   python -m venv .venv
   .\.venv\Scripts\activate
   pip install -r requirements.txt
   ```
4. Démarrer l'extension :
   ```powershell
   func start
   ```
5. Appeler la fonction via HTTP :
   ```powershell
   curl "http://localhost:7071/api/GetFeedback?api_url=http://localhost:8000/feedbacks"
   ```
   ou en POST avec un corps JSON.

## Déploiement rapide sur Azure

1. Se connecter :
   ```powershell
   az login
   ```
2. Créer un groupe de ressources et un plan de consommation si nécessaire.
3. Déployer avec les Functions Core Tools :
   ```powershell
   func azure functionapp publish <nom-de-votre-app>
   ```
4. L'URL finale sera `https://<nom-de-votre-app>.azurewebsites.net/api/GetFeedback`.

## Adapter à votre API

- Remplacez la logique `requests.get(api_url)` par l'appel à votre service (ou utilisez `requests.post`, etc.).
- Si vous avez déjà un schéma Pydantic (`Feedback`), vous pouvez le copier pour valider la réponse.

---

Ce projet est un point de départ pour héberger votre code de récupération d'informations via une API dans Azure Functions de manière simple et rapide.