import json
import requests
import sys
import os
import tqdm
from multiprocessing.pool import ThreadPool

SATELLITE = "LANDSAT_8"
DATA = os.path.join(os.pardir, "data")
url = "https://m2m.cr.usgs.gov/api/api/json/stable/"


def send_post_request(API_url, endpoint, data, key=None):

    request_data = json.dumps(data)

    if key == None:
        header = {'User-Agent': 'myapplication'}
        request = requests.post(
            API_url + endpoint, request_data, headers=header)
    else:
        AUTH = {'X-Auth-Token': key}
        request = requests.post(API_url + endpoint, request_data, headers=AUTH)

    status = request.status_code
    response = json.loads(request.text)

    if status >= 400:
        print(f"Error {status} | {response['errorMessage']}")
        sys.exit(0)

    request.close()

    return response


def download_product(products, path=DATA):

    r = requests.get(products["url"], stream=True)
    if r.status_code == 200:
        with open(os.path.join(path, str(products["downloadId"])+".tar"), 'wb') as f:
            for part in r:
                f.write(part)
        print(f"\nProduct {products['downloadId']} downloaded.")
        f.close()
    else:
        print(f"Error {r.status_code}")


if __name__ == '__main__':

    credentials = {
        'username': 'xxxxx',
        'password': 'xxxxx'
    }

    request_login = send_post_request(url, "login", credentials)
    key = request_login["data"]

    print(f"\n... Looking for {SATELLITE} datasets ...\n")

    request_params = {'datasetName': SATELLITE}
    request_search = send_post_request(
        url, "dataset-search", request_params, key)

    print("\nCollections found are the following ones:\n")

    for i in range(len(request_search["data"])):
        print(
            f"\t{request_search['data'][i]['collectionName']} | Alias: {request_search['data'][i]['datasetAlias']}")

    print("\nWhich alias dataset do you choose? ", end="")
    dataset = input()

    print("\nWhich is the WRS? [WRS1, WRS2]: ", end="")
    WRS = input()
    print("\nROW: ", end="")
    row = input()
    print("\nPATH: ", end="")
    path = input()

    request_params = {
        "row": row,
        "path": path,
        "gridType": WRS,
        "responseShape": "point"
    }

    grid_response = send_post_request(url, "grid2ll", request_params, key)
    coordinates = grid_response["data"]["coordinates"]

    print(
        f"\nThe center point of the given WRS2 grid has coordinates: {coordinates}")
    print()

    request_params = {
        "datasetName": dataset,
        "sceneFilter": {
            "spatialFilter": {
                "filterType": "mbr",
                "lowerLeft": {
                    "latitude": coordinates[0]["latitude"],
                    "longitude": coordinates[0]["longitude"]
                },
                "upperRight": {
                    "latitude": coordinates[0]["latitude"],
                    "longitude": coordinates[0]["longitude"]
                }
            },
            "seasonalFilter": [5, 6],
            "acquisitionFilter": {
                "start": "2013-1-1",
                "end": "2020-12-31"
            },
            "cloudCoverFilter": {
                "max": 5,
                "min": 0,
                "includeUnknown": True
            },
            "startingNumber": 1
        }
    }

    scenes = send_post_request(url, "scene-search", request_params, key)

    scenes_list = []

    for scene_collection in scenes["data"]["results"]:
        scenes_list.append(scene_collection["entityId"])

    print(f"There are {len(scenes_list)} scenes in the list\n")

    request_params = {
        "datasetName": dataset,
        "entityIds": scenes_list
    }

    download_options = send_post_request(
        url, "download-options", request_params, key)

    unique_options = set()
    for product in range(len(download_options["data"])):
        unique_options.add(download_options["data"][product]["productName"])

    print("Download Options are: \n")
    for item in unique_options:
        print(f"\t{item}")

    product_list = []
    for i in range(len(download_options["data"])):
        product = download_options["data"][i]

        if str(product["displayId"]).endswith("T1") and product["productName"] == "Landsat Collection 2 Level-1 Product Bundle" and product["available"] == True:
            product_list.append({'entityId': product['entityId'],
                                 'productId': product['id']})

    print(f"\nThe are {len(product_list)} products available with Landsat Collection 2 Level-1 Product Bundle option in T1 collection category:\n")

    request_params = {"downloads": product_list}
    products_response = send_post_request(
        url, "download-request", request_params, key)

    download_list = []
    product_urls = []

    for i in range(len(products_response["data"]["availableDownloads"])):
        download_list.append(
            products_response["data"]["availableDownloads"][i])
        print(
            f"Product {i} with Id {download_list[i]['downloadId']}: {download_list[i]['url']}")
        product_urls.append(download_list[i]["url"])

    print("\n... Starting to download products ...\n")

    threadPool = ThreadPool(4)
    for _ in tqdm.tqdm(threadPool.imap_unordered(download_product, download_list), total=len(download_list)):
        pass

    print("\nDownload completed")

    send_post_request(url, "logout", None, key)
    print("\n-- Logged Out from USGS API --\n\n")