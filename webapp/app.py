from datetime import datetime

from flask import Flask, render_template

from helpers.bson_utils import int_to_bson_timestamp
from state.ddb_store import DynamoDbStore

app = Flask(__name__)


@app.route('/')
def home():
    items = get_state()
    return render_template('index.html', items=items, updat=datetime.utcnow().strftime('%d %b %Y %H:%M:%S %z'))


def get_state():
    store = DynamoDbStore('','')
    items = store.read_all_state()
    return process_state(items)


def process_state(state: list):
    for s in state:
        try:
            s['ldt_h'] = datetime.fromtimestamp(int_to_bson_timestamp(int(s['ldt'])).time)
        except:
            s['ldt_h'] = s['ldt']
    return state


if __name__ == '__main__':
    app.run()
