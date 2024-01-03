import requests


def get_header():
    return {
        'sec-ch-ua': '"Google Chrome";v="119", "Chromium";v="119", "Not?A_Brand";v="24"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
        'x-api-source': 'pc',
        'x-requested-with': 'XMLHttpRequest',
        'x-shopee-language': 'vi',
    }


def send_request(url: str):
    return requests.get(url, headers=get_header())
