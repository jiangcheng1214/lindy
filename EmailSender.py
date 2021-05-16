import json
import os
import sendgrid
# from sendgrid import SendGridAPIClient
# from sendgrid.helpers.mail import Mail

with open('credentials/sendgrid_credentials.json', 'r') as f:
    credentials = json.load(f)


message = sendgrid.Mail(
    from_email='jiangcheng1214@gmail.com',
    to_emails=['jayceejiang1214@gmail.com','haotianwu3@gmail.com'])

sg = sendgrid.SendGridAPIClient(credentials['apiKey'])

message.template_id = 'd-ca859f1969174471b23884bde7d3de09'
message.dynamic_template_data = {
    'subject': 'Testing Templates',
    'href': 'https://www.hermes.com/us/en/',
    'updates': [1,1,1],
}

response = sg.send(message)
print(response.status_code)
print(response.body)
print(response.headers)
