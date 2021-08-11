from api.base.base_api import BaseApi
from api.base.dto.api_output import ApiOutput

class Api(BaseApi):

    def run(self, input) -> ApiOutput:

        return ApiOutput.success("hello, you have tested successfully")
