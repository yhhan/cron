import requests

print("하루에 한 번 실행되는 스크립트 시작")
# 여기에 원하는 작업(크롤링, API 호출 등)을 작성하세요.
response = requests.get("https://github.com")
print(f"GitHub API 상태: {response.status_code}")
