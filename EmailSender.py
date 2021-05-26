import json

import sendgrid

from Utils import get_current_pst_format_date, supported_categories, get_current_pst_format_year_month
import pyrebase


class EmailSender:
    def __init__(self):
        with open('credentials/firebase_credentials.json', 'r') as f:
            firebase_credentials = json.load(f)
        self.firebase = pyrebase.initialize_app(firebase_credentials)
        self.database = self.firebase.database()

        with open('credentials/sendgrid_credentials.json', 'r') as f:
            sendgrid_credentials = json.load(f)
        self.sg = sendgrid.SendGridAPIClient(sendgrid_credentials['apiKey'])

    def send_daily_update(self, month_string, day_string):
        def image_html(item):
            template = '''<img src="{}" height="150" />'''
            html = ''
            for url in item['assets']:
                html += template.format('https:' + url['url'])
            return html
        daily_delta_db_path = "delta_daily/{}/{}".format(month_string, day_string)
        print('sending email for {} update'.format(daily_delta_db_path))
        daily_delta_data = self.database.child(daily_delta_db_path).get().val()
        if not daily_delta_data:
            print("daily delta not exists: {}".format(daily_delta_db_path))
            return
        product_template = '''
            <hr />
            <p>{}</p>
            <p>{} - <a href="{}">{}</a></p>
            <p>Price: ${}</p>
            <p>Category: {}</p>
            '''
        added_item_html = ''
        count = 0
        for category_code in supported_categories():
            if category_code not in daily_delta_data:
                print('{} not in daily_delta_data'.format(category_code))
                continue
            if "ADDED" not in daily_delta_data[category_code]:
                print('ADDED not in daily_delta_data[{}]'.format(category_code))
                continue
            for sku in daily_delta_data[category_code]['ADDED']:
                count += 1
                item = daily_delta_data[category_code]['ADDED'][sku]
                item_html = product_template.format(image_html(item), count,
                                                    'https://www.hermes.com/us/en' + item['url'], item['title'],
                                                    item['price'], category_code)
                added_item_html += item_html

        if count == 0:
            print('NO NEW ITEMS on {}'.format(daily_delta_db_path))
            return
        html_content = '''
        <h1>Update on {}</h1>
        <h2>New Items ({} in total):</h2>
        {}
        <p><strong><br />Thanks for staying updated with us!</strong></p>
        '''.format(day_string, count - 1, added_item_html)
        print(daily_delta_data)

        message = sendgrid.Mail(
            from_email='jiangcheng1214@gmail.com',
            to_emails=[
                'chengjiang1214@gmail.com',
                'haotianwu3@gmail.com',
                'limeihui816@hotmail.com'
            ],
            subject='Hermes product daily update ({})'.format(day_string),
            html_content=html_content
        )
        response = self.sg.send(message)
        print(response.status_code)
        print(response.body)
        print(response.headers)


# sender = EmailSender()
# sender.send_daily_update(get_current_pst_format_year_month(), get_current_pst_format_date())
# sender.send_daily_up
# date("202105", "20210524")
# response = sg.send(message)
# print(response.status_code)
# print(response.body)
# print(response.headers)
