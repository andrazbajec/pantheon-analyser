from color import Color

class Logger:
    @staticmethod
    def error(message):
        print(message)
    @staticmethod
    def success(message):
        print(f'{Color.GREEN_LIGHT}{message}{Color.ESCAPE}')
    def warn(message):
        print(f'{Color.YELLOW_LIGHT}{message}{Color.ESCAPE}')
    @staticmethod
    def input(message, color = Color.YELLOW):
        return input(f'{color}* {message}{Color.ESCAPE}')