import json

import sendgrid
from sendgrid import To

from Utils import get_current_pst_format_date

with open('credentials/sendgrid_credentials.json', 'r') as f:
    credentials = json.load(f)

sg = sendgrid.SendGridAPIClient(credentials['apiKey'])
TO_EMAILS = [
    To(
        email='jayceejiang1214@gmail.com',
        subject="Hermes product daily update",
    ),
]

added_list = [
    {
        "assets": [{
            "url": "//assets.hermes.com/is/image/hermesproduct/0005471%2093_front_1?a=a&size=3000,3000&extend=300,300,300,300&align=0,1"
        }, {
            "url": "//assets.hermes.com/is/image/hermesproduct/0005471%2093_front_2?a=a&size=3000,3000&extend=300,300,300,300&align=0,1"
        }, {
            "url": "//assets.hermes.com/is/image/hermesproduct/0005471%2093_front_3?a=a&size=3000,3000&extend=300,300,300,300&align=0,1"
        }],
        "price": 299,
        "time_added": "20210520_14_09_15",
        "time_available_hours": 6.335,
        "time_removed": "20210520_20_29_21",
        "title": "Apple AirTag Hermes bag charm",
        "url": "/product/apple-airtag-hermes-bag-charm-H0005471v9300"
    },
    {
        "assets": [{
            "url": "//assets.hermes.com/is/image/hermesproduct/0005501%2034_front_1?a=a&size=3000,3000&extend=300,300,300,300&align=0,1"
        }, {
            "url": "//assets.hermes.com/is/image/hermesproduct/0005501%2034_front_2?a=a&size=3000,3000&extend=300,300,300,300&align=0,1"
        }, {
            "url": "//assets.hermes.com/is/image/hermesproduct/0005501%2034_front_3?a=a&size=3000,3000&extend=300,300,300,300&align=0,1"
        }],
        "price": 349,
        "time_added": "20210519_12_03_50",
        "time_available_hours": 3.5841666666666665,
        "time_removed": "20210519_15_38_53",
        "title": "Apple AirTag Hermes key ring",
        "url": "/product/apple-airtag-hermes-key-ring-H0005501v3400"
    }
]

product_template = '''
<p><img src="{}" alt="interactive connection" width="210" height="196" /><img src="{}" height="196" /></p>
<p><a href="{}">{}</a></p>
<p>price:{}</p>
'''
added_item_html = ''
for item in added_list:
    item_html = product_template.format('https:' + item['assets'][0]['url'], 'https:' + item['assets'][1]['url'], 'https://www.hermes.com/us/en' + item['url'], item['title'], item['price'])
    added_item_html += item_html


html_content = '''
<h1>Update on {}</h1>
<h2>New Items:</h2>
{}
<h2>Removed Items:</h2>
<p><img src="https://assets.hermes.com/is/image/hermesproduct/0005471%2093_front_1?a=a&amp;size=3000,3000&amp;extend=300,300,300,300&amp;align=0,1" alt="interactive connection" width="210" height="196" /><img src="https://assets.hermes.com/is/image/hermesproduct/0005471%2093_front_2?a=a&amp;size=3000,3000&amp;extend=300,300,300,300&amp;align=0,1" alt="interactive connection" width="210" height="196" /></p>
<p><a href="https://www.hermes.com/us/en/product/apple-airtag-hermes-bag-charm-H0005471v9300/">title 1</a></p>
<p>price:</p>
<p><strong><br />Save this link into your bookmarks and share it with your friends. It is all FREE! </strong><br /><strong>Enjoy!</strong></p>
'''.format(get_current_pst_format_date(), added_item_html)

message = sendgrid.Mail(
    from_email='jiangcheng1214@gmail.com',
    to_emails=[
        'chengjiang1214@gmail.com',
        'haotianwu3@gmail.com',
        'limeihui816@hotmail.com'
    ],
    subject='Hermes product daily update ({})'.format(get_current_pst_format_date()),
    html_content=html_content
)

response = sg.send(message)
print(response.status_code)
print(response.body)
print(response.headers)
