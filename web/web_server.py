from flask import Flask, request

from web import response_builder
from api.base.api_executer import ApiExecutor
from web.regex_router_converter import RegexRouterConverter

app = Flask(__name__)
# 这里是定义数据类性的名字，并注册到url_map中
app.url_map.converters['regex'] = RegexRouterConverter


@app.route('/<regex:path>', methods=['GET', 'POST'])
def execute_api(path):
    output = ApiExecutor.execute(request)

    app.logger.info("Info message")

    # 将 ApiOutput 对象转为 http response
    return response_builder.build(output)


if __name__ == '__main__':
    app.debug = True
    app.run(host='0.0.0.0',port=5000) 
