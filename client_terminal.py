import requests

API_URL = "http://localhost:8000/submit_task"


def main():
    print("TEST")
    print("Bereiche: IT Security, Renewable Energy und Elektrotechnik")

    while True:
        try:
            user_input = input("\nPM > ")
            if not user_input or user_input.lower() in ["exit", "quit"]: break

            print("KI analysiert")
            resp = requests.post(API_URL, json={"text": user_input})

            if resp.status_code == 200:
                print(f"{resp.json()['msg']}")
            else:
                print(f"Error: {resp.text}")

        except Exception as e:
            print(f"Connection Error: {e}")


if __name__ == "__main__":
    main()
