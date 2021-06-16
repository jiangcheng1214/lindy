import json

import pyrebase
import sendgrid

from Utils import supported_categories, get_current_pst_format_timestamp, flag_for_country


class EmailSender:
    def __init__(self):
        with open('credentials/firebase_credentials.json', 'r') as f:
            firebase_credentials = json.load(f)
        self.firebase = pyrebase.initialize_app(firebase_credentials)
        self.database = self.firebase.database()

        with open('credentials/sendgrid_credentials.json', 'r') as f:
            sendgrid_credentials = json.load(f)
        self.sg = sendgrid.SendGridAPIClient(sendgrid_credentials['apiKey'])

    def item_added_html_string(self, locale_code, category_code, sku):
        try:
            added_time = self.database.child(
                '{}/product_updates/{}/ADDED/{}/{}'.format(locale_code, category_code, sku, 'time_added')).get().val()
            return '<p>Available from: {}</p>'.format(added_time[:8] + " " + added_time[9:14].replace("_", ":"))
        except Exception:
            return ""

    def item_added_time_html_string(self, locale_code, category_code, sku):
        try:
            added_time = self.database.child(
                '{}/product_updates/{}/ADDED/{}/{}'.format(locale_code, category_code, sku, 'time_added')).get().val()
            formatted_added_time = added_time[:8] + " " + added_time[9:14].replace("_", ":")
            return '<p>Available from: {}</p>'.format(formatted_added_time)
        except Exception:
            return '<p>Available from: N/A</p>'

    def item_removed_time_html_string(self, locale_code, category_code, sku):
        ret = ""
        try:
            added_time = self.database.child(
                '{}/product_updates/{}/REMOVED/{}/{}'.format(locale_code, category_code, sku, 'time_added')).get().val()
            formatted_added_time = added_time[:8] + " " + added_time[9:14].replace("_", ":")
            ret += '<p>Available from: {}</p>'.format(formatted_added_time)
        except Exception:
            ret += '<p>Available from: N/A</p>'
        try:
            removed_time = self.database.child(
                '{}/product_updates/{}/REMOVED/{}/{}'.format(locale_code, category_code, sku,
                                                             'time_removed')).get().val()
            available_time_hours = self.database.child(
                '{}/product_updates/{}/REMOVED/{}/{}'.format(locale_code, category_code, sku,
                                                             'time_available_hours')).get().val()
            formatted_removed_time = removed_time[:8] + " " + removed_time[9:14].replace("_", ":")
            ret += '<p>Sold out at: {} ({} hours after release)</p>'.format(formatted_removed_time,
                                                                            "{:.2f}".format(available_time_hours))
        except Exception:
            ret += '<p>Sold out at: N/A (N/A hours after release)</p>'

        return ret

    def send_realtime_update(self, locale_code, last_update_stamp=None):
        def image_html(item):
            template = '''<img src="{}" height="150" />'''
            html = ''
            for url in item['assets']:
                html += template.format('https:' + url['url'])
            return html

        def added_products_html(locale_code, daily_delta_data):
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
                                                                  'https://www.hermes.com/{}'.format(locale_code) + item['url'],
                                                                  item['title'],
                                                                  item['price'],
                                                                  self.item_added_time_html_string(locale_code,
                                                                                                   category_code, sku),
                                                                  category_code)
                        added_item_html += item_html
            if added_item_count > 0:
                return added_item_html.format(added_item_count)
            else:
                return ""

        def removed_products_html(locale_code, daily_delta_data):
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
                                                                    'https://www.hermes.com/{}'.format(locale_code) + item['url'],
                                                                    item['title'],
                                                                    item['price'],
                                                                    self.item_removed_time_html_string(locale_code,
                                                                                                       category_code,
                                                                                                       sku),
                                                                    category_code)
                        removed_item_html += item_html
            if removed_item_count > 0:
                return removed_item_html.format(removed_item_count)
            else:
                return ""

        if not last_update_stamp:
            last_update_stamp = self.database.child("{}/delta_realtime/last_update".format(locale_code)).get().val()
        delta_realtime_path = "{}/delta_realtime/{}/{}".format(locale_code, last_update_stamp[:6], last_update_stamp)
        print('sending email for {} real time update'.format(delta_realtime_path))
        realtime_delta_data = self.database.child(delta_realtime_path).get().val()
        if not realtime_delta_data:
            print("realtime delta not exists: {}".format(delta_realtime_path))
            return
        added_products_html_string = added_products_html(locale_code, realtime_delta_data)
        removed_products_html_string = removed_products_html(locale_code, realtime_delta_data)
        if not added_products_html_string:
            print("no added product update detected")
            return
        html_content = '''
        <h1>Realtime Update {}</h1>
        {}
        {}
        <p><strong><br />Thanks for staying updated with us!</strong></p>
        '''.format(flag_for_country(locale_code), added_products_html_string, removed_products_html_string)
        print(realtime_delta_data)

        update_stamp = last_update_stamp.split('_to_')[-1]
        message = sendgrid.Mail(
            from_email='jiangcheng1214@gmail.com',
            to_emails=[
                'chengjiang1214@gmail.com',
                'haotianwu3@gmail.com',
                'limeihui816@hotmail.com'
            ],
            subject='📢 Hermes realtime update {} {}'.format(flag_for_country(locale_code), update_stamp),
            html_content=html_content
        )
        response = self.sg.send(message)
        print(response.status_code)
        print(response.body)
        print(response.headers)

    def send_daily_update(self, date_string, locale_code):
        def image_html(item):
            template = '''<img src="{}" height="150" />'''
            html = ''
            for url in item['assets']:
                html += template.format('https:' + url['url'])
            return html

        def added_products_html(locale_code, daily_delta_data):
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
                                                                  'https://www.hermes.com/{}'.format(locale_code) + item['url'],
                                                                  item['title'],
                                                                  item['price'],
                                                                  self.item_added_time_html_string(locale_code,
                                                                                                   category_code, sku),
                                                                  category_code)
                        added_item_html += item_html
            if added_item_count > 0:
                return added_item_html.format(added_item_count)
            else:
                return ""

        def removed_products_html(locale_code, daily_delta_data):
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
                                                                    'https://www.hermes.com/{}'.format(locale_code) + item['url'],
                                                                    item['title'],
                                                                    item['price'],
                                                                    self.item_removed_time_html_string(locale_code,
                                                                                                       category_code,
                                                                                                       sku),
                                                                    category_code)
                        removed_item_html += item_html
            if removed_item_count > 0:
                return removed_item_html.format(removed_item_count)
            else:
                return ""

        daily_delta_db_path = "{}/delta_daily/{}/{}".format(locale_code, date_string[:-2], date_string)
        print('sending email for {} update'.format(daily_delta_db_path))
        daily_delta_data = self.database.child(daily_delta_db_path).get().val()
        if not daily_delta_data:
            print("daily delta not exists: {}".format(daily_delta_db_path))
            return

        added_products_html_string = added_products_html(locale_code, daily_delta_data)
        removed_products_html_string = removed_products_html(locale_code, daily_delta_data)
        if not added_products_html_string and not removed_products_html_string:
            print("no product update detected")
            return
        html_content = '''
        <h1>Update on {} {}</h1>
        {}
        {}
        <p><strong><br />Thanks for staying updated with us!</strong></p>
        '''.format(date_string, flag_for_country(locale_code), added_products_html_string, removed_products_html_string)
        print(daily_delta_data)

        message = sendgrid.Mail(
            from_email='jiangcheng1214@gmail.com',
            to_emails=[
                'chengjiang1214@gmail.com',
                'haotianwu3@gmail.com',
                'limeihui816@hotmail.com'
            ],
            subject='📢 {} Hermes daily update {}'.format(date_string, flag_for_country(locale_code)),
            html_content=html_content
        )
        response = self.sg.send(message)
        print(response.status_code)
        print(response.body)
        print(response.headers)

    def notice_admins_on_exception(self, exception, local_code, job_type):
        message = sendgrid.Mail(
            from_email='jiangcheng1214@gmail.com',
            to_emails=[
                'chengjiang1214@gmail.com',
            ],
            subject='Hermes scraper exception {}'.format(get_current_pst_format_timestamp()),
            html_content='''
            <h1>Exception: {}</h1>
            <h2>local_code: {}</h2>
            <h2>job_type: {}</h2>
            '''.format(exception, local_code, job_type)
        )
        response = self.sg.send(message)
        print(response.status_code)
        print(response.body)
        print(response.headers)

# sender = EmailSender()
# sender.send_realtime_update('us_en')
# sender.send_realtime_update("us_en", "20210615_14_27_28_to_20210615_15_45_30")
# sender.send_realtime_update("us_en", "20210607_23_09_00_to_20210607_23_54_01")
# sender.send_daily_update('20210611', 'cn_zh')
# response = sg.send(message)
# print(response.status_code)
# print(response.body)
# print(response.headers)
