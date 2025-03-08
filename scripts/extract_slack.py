import fire
import datetime
import re
import time
import json
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

def remove_slack_links(text):
    text = re.sub(r"<[^|>]*\|([^>]+)>", r"\1", text)
    text = re.sub(r"<[^>]*>", "", text)
    return text

def get_combined_text(msg):
    pattern = r"\*<[^|>]+\|([^>]+)>\*"
    texts = []
    for block in msg.get("blocks", []):
        if block.get("type") == "rich_text":
            for element in block.get("elements", []):
                if element.get("type") == "rich_text_section":
                    for sub in element.get("elements", []):
                        if sub.get("type") in ["text", "link"]:
                            t = sub.get("text", "")
                            t = re.sub(pattern, r"\1", t)
                            t = remove_slack_links(t)
                            texts.append(t)
    for attach in msg.get("attachments", []):
        t = attach.get("text", "")
        if t:
            t = re.sub(pattern, r"\1", t)
            t = remove_slack_links(t)
            texts.append(t)
    return "\n".join(texts).strip()

def filter_messages(
    token, 
    channel, 
    workspace, 
    date_from=None, 
    date_to=None, 
    min_length=400, 
    debug=False
):
    client = WebClient(token=token)
    try:
        team_info = client.team_info()
        team_name = team_info.get("team", {}).get("name", "")
        if workspace.lower() not in team_name.lower():
            print(f"Warning: Provided workspace '{workspace}' does not match token's workspace '{team_name}'.")
    except SlackApiError as e:
        print(e.response)
        return

    today = datetime.date.today()
    if not date_to:
        date_to = today
    else:
        try:
            date_to = datetime.datetime.strptime(date_to, "%Y-%m-%d").date()
        except ValueError:
            print("Invalid 'date_to'")
            return

    if not date_from:
        date_from = today - datetime.timedelta(days=30)
    else:
        try:
            date_from = datetime.datetime.strptime(date_from, "%Y-%m-%d").date()
        except ValueError:
            print("Invalid 'date_from'")
            return

    if date_from > date_to:
        print("Error: 'date_from' cannot be later than 'date_to'.")
        return

    messages = []
    cursor = None
    while True:
        try:
            resp = client.conversations_history(channel=channel, cursor=cursor, limit=1000)
        except SlackApiError as e:
            if e.response.get("error") == "ratelimited":
                retry_after = int(e.response.headers.get("Retry-After", 1))
                print(f"Rate limited. Retrying after {retry_after} seconds...")
                time.sleep(retry_after)
                continue
            else:
                print(e.response)
                return
        messages.extend(resp.get("messages", []))
        cursor = resp.get("response_metadata", {}).get("next_cursor")
        if not cursor:
            break

    print(f"Total messages fetched: {len(messages)}")

    filtered = []
    for msg in messages:
        ts = float(msg.get("ts", 0))
        date_obj = datetime.datetime.fromtimestamp(ts).date()
        if date_from <= date_obj <= date_to:
            combined_text = get_combined_text(msg)
            if len(combined_text) >= min_length:
                msg["combined_text"] = combined_text
                msg["date_str"] = datetime.datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")
                filtered.append(msg)

    if not filtered:
        print("No messages found")
    else:
        for f in filtered:
            print(f"[{f['date_str']}]")
            print(f["combined_text"])
            print("-----")

    print(f"Total filtered messages: {len(filtered)}")

    text_list = [f["combined_text"] for f in filtered]
    with open("filtered_texts.json", "w", encoding="utf-8") as f:
        json.dump(text_list, f, ensure_ascii=False, indent=2)
    print("Filtered messages saved to filtered_texts.json")

    if debug:
        with open("blocks.json", "w", encoding="utf-8") as f:
            json.dump(messages, f, ensure_ascii=False, indent=2)
        print("Debug mode: All fetched messages saved to blocks.json")

if __name__ == "__main__":
    fire.Fire(filter_messages)
