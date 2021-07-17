from web import web_server


class AppLauncher:
    """
    程序启动入口
    """

    @staticmethod
    def start_app():
        # 启动 web 服务
        web_server.app.run(host='0.0.0.0')


if __name__ == '__main__':
    AppLauncher.start_app()
