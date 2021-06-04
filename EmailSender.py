import json

import pyrebase
import sendgrid

from Utils import supported_categories, get_current_pst_format_timestamp


class EmailSender:
    def __init__(self):
        with open('credentials/firebase_credentials.json', 'r') as f:
            firebase_credentials = json.load(f)
        self.firebase = pyrebase.initialize_app(firebase_credentials)
        self.database = self.firebase.database()

        with open('credentials/sendgrid_credentials.json', 'r') as f:
            sendgrid_credentials = json.load(f)
        self.sg = sendgrid.SendGridAPIClient(sendgrid_credentials['apiKey'])

    def item_added_html_string(self, category_code, sku):
        try:
            added_time = self.database.child(
                'product_updates/{}/ADDED/{}/{}'.format(category_code, sku, 'time_added')).get().val()
            return '<p>Available from: {}</p>'.format(added_time[:8] + " " + added_time[9:14].replace("_", ":"))
        except:
            return ""

    def item_added_time_html_string(self, category_code, sku):
        try:
            added_time = self.database.child(
                'product_updates/{}/ADDED/{}/{}'.format(category_code, sku, 'time_added')).get().val()
            formatted_added_time = added_time[:8] + " " + added_time[9:14].replace("_", ":")
            return '<p>Available from: {}</p>'.format(formatted_added_time)
        except:
            return '<p>Available from: N/A</p>'

    def item_removed_time_html_string(self, category_code, sku):
        ret = ""
        try:
            added_time = self.database.child(
                'product_updates/{}/REMOVED/{}/{}'.format(category_code, sku, 'time_added')).get().val()
            formatted_added_time = added_time[:8] + " " + added_time[9:14].replace("_", ":")
            ret += '<p>Available from: {}</p>'.format(formatted_added_time)
        except:
            ret += '<p>Available from: N/A</p>'
        try:
            removed_time = self.database.child(
                'product_updates/{}/REMOVED/{}/{}'.format(category_code, sku, 'time_removed')).get().val()
            available_time_hours = self.database.child(
                'product_updates/{}/REMOVED/{}/{}'.format(category_code, sku, 'time_available_hours')).get().val()
            formatted_removed_time = removed_time[:8] + " " + removed_time[9:14].replace("_", ":")
            ret += '<p>Sold out at: {} ({} hours after release)</p>'.format(formatted_removed_time,
                                                                            "{:.2f}".format(available_time_hours))
        except:
            ret += '<p>Sold out at: N/A (N/A hours after release)</p>'

        return ret

    def item_added_html_string(self, category_code, sku):
        try:
            added_time = self.database.child(
                'product_updates/{}/ADDED/{}/{}'.format(category_code, sku, 'time_added')).get().val()
            return '<p>Available from: {}</p>'.format(added_time[:8] + " " + added_time[9:14].replace("_", ":"))
        except:
            return ""

    def send_daily_update(self, date_string):
        def image_html(item):
            template = '''<img src="{}" height="150" />'''
            html = ''
            for url in item['assets']:
                html += template.format('https:' + url['url'])
            return html

        def added_products_html(daily_delta_data):
            added_item_html = '<h2>New Items ({} in total):</h2>'
            added_product_template = '''
                        <hr />
                        <p>{}</p>
                        <p>{} - <a href="{}">{}</a></p>
                        <p>Price: ${}</p>
                        {}
                        <p>Category: {}</p>
                        '''
            added_item_count = 0
            visited = set()
            for category_code in supported_categories():
                if category_code not in daily_delta_data:
                    print('{} not in daily_delta_data'.format(category_code))
                    continue
                if "ADDED" in daily_delta_data[category_code]:
                    for sku in daily_delta_data[category_code]['ADDED']:
                        if sku in visited:
                            continue
                        visited.add(sku)
                        added_item_count += 1
                        item = daily_delta_data[category_code]['ADDED'][sku]
                        item_html = added_product_template.format(image_html(item), added_item_count,
                                                                  'https://www.hermes.com/us/en' + item['url'],
                                                                  item['title'],
                                                                  item['price'],
                                                                  self.item_added_time_html_string(category_code, sku),
                                                                  category_code)
                        added_item_html += item_html
            return added_item_html.format(added_item_count)

        def removed_products_html(daily_delta_data):
            removed_item_html = '<h2>Sold out Items ({} in total):</h2>'
            removed_product_template = '''
                <hr />
                <p>{}</p>
                <p><span style="text-decoration: line-through;">{} - <a href="{}">{}</a></span></p>
                <p><span style="text-decoration: line-through;">Price: ${} </span>(SOLD OUT)</p>
                {}
                <p>Category: {}</p>
                '''
            removed_item_count = 0
            visited = set()
            for category_code in supported_categories():
                if category_code not in daily_delta_data:
                    print('{} not in daily_delta_data'.format(category_code))
                    continue
                if "REMOVED" in daily_delta_data[category_code]:
                    for sku in daily_delta_data[category_code]['REMOVED']:
                        if sku in visited:
                            continue
                        visited.add(sku)
                        removed_item_count += 1
                        item = daily_delta_data[category_code]['REMOVED'][sku]
                        item_html = removed_product_template.format(image_html(item), removed_item_count,
                                                                    'https://www.hermes.com/us/en' + item['url'],
                                                                    item['title'],
                                                                    item['price'],
                                                                    self.item_removed_time_html_string(category_code,
                                                                                                       sku),
                                                                    category_code)
                        removed_item_html += item_html
            return removed_item_html.format(removed_item_count)

        daily_delta_db_path = "delta_daily/{}/{}".format(date_string[:-2], date_string)
        print('sending email for {} update'.format(daily_delta_db_path))
        daily_delta_data = self.database.child(daily_delta_db_path).get().val()
        if not daily_delta_data:
            print("daily delta not exists: {}".format(daily_delta_db_path))
            return

        html_content = '''
        <h1>Update on {}</h1>
        {}
        {}
        <p><strong><br />Thanks for staying updated with us!</strong></p>
        '''.format(date_string, added_products_html(daily_delta_data), removed_products_html(daily_delta_data))
        print(daily_delta_data)

        message = sendgrid.Mail(
            from_email='jiangcheng1214@gmail.com',
            to_emails=[
                'chengjiang1214@gmail.com',
                'haotianwu3@gmail.com',
                'limeihui816@hotmail.com'
            ],
            subject='Hermes product daily update ({})'.format(date_string),
            html_content=html_content
        )
        response = self.sg.send(message)
        print(response.status_code)
        print(response.body)
        print(response.headers)

    def notice_admins_on_exception(self, exception, context, retry):
        message = sendgrid.Mail(
            from_email='jiangcheng1214@gmail.com',
            to_emails=[
                'chengjiang1214@gmail.com',
            ],
            subject='Hermes scraper exception on ({}) retry = {}'.format(get_current_pst_format_timestamp(), retry),
            html_content='''
            <h1>Exception: {}</h1>
            <h1>Context: {}</h1>
            '''.format(exception, context)
        )
        response = self.sg.send(message)
        print(response.status_code)
        print(response.body)
        print(response.headers)


# sender = EmailSender()
# sender.send_daily_update('20210601')
# response = sg.send(message)
# print(response.status_code)
# print(response.body)
# print(response.headers)
