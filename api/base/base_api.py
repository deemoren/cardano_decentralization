from api.base.dto.api_output import ApiOutput


class BaseApi(object):

    def run(self, input) -> ApiOutput:
        # 所有 api 需要重写此方法
        return ApiOutput()
