import requests
import json
from datetime import date

from airflow.decorators import task
from airflow.models import Variable

"""
Nota:

Este script requiere habilitar el servicio de Youtube en algún proyecto de Google Console (o crear uno nuevo) y
generar una API Key.
"""

# Obtenemos variables de Airflow.
# Estas variables pueden ser creadas a través de la UI de Airflow(Seccion "Admin" -> "Variables") o creando
# variables de entorno en el host o máquina de Airflow que tengan el prefijo "AIRFLOW_VAR".
# En nuestro caso, se han creado las variables de entorno "AIRFLOW_VAR_API_KEY" y "AIRFLOW_VAR_CHANNEL_HANDLE" en el docker-compose.yaml.
API_KEY = Variable.get("API_KEY")
CHANNEL_HANDLE = Variable.get("CHANNEL_HANDLE")
MAX_RESULTS = 50

@task
def get_playlist_id():
    try:
        url = f"https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={CHANNEL_HANDLE}&key={API_KEY}"

        response = requests.get(url)

        # Lanzará una excepción si la petición http retorna una respuesta con un código de estado erróneo.
        response.raise_for_status()

        data = response.json()

        channel_items = data['items'][0]

        channnel_playlist_id = channel_items['contentDetails']['relatedPlaylists']['uploads']

        return channnel_playlist_id
    
    # Todas las excepciones que pueda lanzar el módulo Requests heredan de la clase "requests.exceptions.RequestException".
    except requests.exceptions.RequestException as e:
        raise e


@task
def get_video_ids(playlist_id):
    base_url = f"https://youtube.googleapis.com/youtube/v3/playlistItems?part=contentDetails&maxResults={MAX_RESULTS}&playlistId={playlist_id}&key={API_KEY}"
    video_ids = []
    url = base_url

    try:
        while True:
            response = requests.get(url)

            # Lanzará una excepción si la petición http retorna una respuesta con un código de estado erróneo.
            response.raise_for_status()

            data = response.json()

            for item in data.get('items', []):
                video_id = item['contentDetails']['videoId']
                video_ids.append(video_id)

            # Si en la respuesta viene el campo "nextPageToken", significa que no estamos en la última página y hay más vídeos. 
            next_page_token = data.get('nextPageToken')

            if not next_page_token:
                break
            
            url = f"{base_url}&pageToken={next_page_token}"

        return video_ids

    # Todas las excepciones que pueda lanzar el módulo Requests heredan de la clase "requests.exceptions.RequestException".
    except requests.exceptions.RequestException as e:
        raise e
    

@task
def get_videos_data(video_ids):
    videos_data = []

    # Separa la lista de ids en trozos y crea un generador que proporciona cada trozo de ids.
    def batch_video_ids_list(video_id_list, batch_size):
        for i in range(0, len(video_id_list), batch_size):
            yield video_id_list[i: i + batch_size]

    try:

        for batch_ids in batch_video_ids_list(video_ids, MAX_RESULTS):
            video_ids_str = ",".join(batch_ids)

            url = f"https://youtube.googleapis.com/youtube/v3/videos?part=contentDetails&part=snippet&part=statistics&id={video_ids_str}&key={API_KEY}"

            response = requests.get(url)

            # Lanzará una excepción si la petición http retorna una respuesta con un código de estado erróneo.
            response.raise_for_status()

            data = response.json()

            for item in data.get('items', []):
                video_id = item['id']
                snippet = item['snippet']
                content_details = item['contentDetails']
                statistics = item['statistics']

                video_data = {
                    "video_id": video_id,
                    "title": snippet['title'],
                    "publishedAt": snippet['publishedAt'],
                    "duration": content_details['duration'],
                    "viewCount": statistics.get('viewCount', None),
                    "likeCount": statistics.get('likeCount', None),
                    "commentCount": statistics.get('commentCount', None)
                }

                videos_data.append(video_data)
        
        return videos_data

    except requests.exceptions.RequestException as e:
        raise e


@task
def save_to_json(videos_data):
    file_path = "./data/YT_data_{}.json".format(date.today())

    with open(file_path, "w", encoding="utf-8") as json_outfile:
        json.dump(videos_data, json_outfile, indent=4, ensure_ascii=False)


if __name__ == "__main__":
    playlist_id = get_playlist_id()
    video_ids = get_video_ids(playlist_id)
    videos_data = get_videos_data(video_ids)
    save_to_json(videos_data)