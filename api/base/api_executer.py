import time

from api.base.base_api import BaseApi
from api.base.dto.api_output import ApiOutput

"""
所有 API 的统一入口 
"""


class ApiExecutor:

    @staticmethod
    def execute(request) -> ApiOutput:
        start_time = time.time()

        # 创建 api 入参对象
        api_params = ApiExecutor.build_api_params(request)

        # 创建 api 实例
        api = ApiExecutor.create_api_instance(request)
        if api is None:
            return ApiOutput.fail(-1, '未找到 api：' + request.path)

        output: ApiOutput = None
        try:
            # 执行 api
            output = api.run(api_params)
        except Exception as e:
            output = ApiOutput.fail(-1, str(e))

        # 计算耗时
        output.spend = int((time.time() - start_time) * 1000)

        return output

    @staticmethod
    def create_api_instance(request) -> BaseApi:
        """
        根据请求路径创建 api 实例
        """

        # 将路径统一转为小写，使得外部大小写都能访问
        api_path = request.path.lower()
        # 兼容反斜杠
        api_path = api_path.replace('\\', '/')
        # 移除头尾的斜杠
        api_path = api_path.strip('/')

        # 拼接 api 文件路径
        array = api_path.split('/')
        array.insert(0, 'api')
        api_file_name = '.'.join(array) + '_api'

        # 创建 api 实例
        api: BaseApi = None
        try:
            module_meta = __import__(api_file_name, globals(), locals(), ['Api', 'Input'])

            api_class_meta = getattr(module_meta, 'Api')
            api = api_class_meta()
        except ModuleNotFoundError as e:
            print(e.msg)

        return api

    @staticmethod
    def build_api_params(request) -> dict:
        """创建 api 请求参数对象"""

        # get 参数
        get_params = request.args
        # post 参数
        post_params = request.get_json()

        # 合并 get 参数和 post 参数
        all_params = get_params.to_dict()

        if post_params is not None:
            all_params.update(post_params)

        return all_params
