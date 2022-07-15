import requests
from discord import Webhook, RequestsWebhookAdapter

def send_discord_msg(msg):
    webhook = Webhook.from_url("https://discord.com/api/webhooks/994100386925379634/fEvxgJzUu5wN8hypowqUoyh2flfQJKl6Ml7eoGoCir6HKuhbd7h2bHKqzdgeUeHUIJBA",
                               adapter=RequestsWebhookAdapter())
    webhook.send(msg)
