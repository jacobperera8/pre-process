import io
from io import StringIO
from flask import Flask, jsonify, make_response, request
import boto3
import pandas as pd

app = Flask(__name__)


@app.route("/")
def hello_from_root():
    return jsonify(message='Hello from root!')


@app.route("/hello")
def hello():
    return jsonify(message='Hello from path!')

@app.route('/post-process-save', methods=['POST'])
def insert2():
    if request.method == "POST":
        url_key = request.get_json(silent=True)['urlKey']
        dir = request.get_json(silent=True)['dir']
        s3 = boto3.client('s3')
        bucket_name = 'test-upload-bucket-devy'
        object_key = url_key
        obj = s3.get_object(Bucket=bucket_name, Key=object_key)

        df = pd.read_excel(io.BytesIO(obj['Body'].read()))

        print(df.head())

        csv_buffer = StringIO()
        df.to_csv(csv_buffer)
        s3_resource = boto3.resource('s3')
        s3_resource.Object(bucket_name, 'pre-processed-data/df.csv').put(Body=csv_buffer.getvalue())

        return jsonify(message='completed postprocessing the dataset!',
                       urlKey=url_key,
                       s3Dir=dir,
                       savedDir='pre-processed-data/df.csv')

@app.errorhandler(404)
def resource_not_found(e):
    return make_response(jsonify(error='Not found!'), 404)

