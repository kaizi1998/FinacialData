import os
import datetime
import logging
import sys
import traceback
from ConfigReader import logs_path


def get_log_filepath():
    year = datetime.datetime.now().year
    month = datetime.datetime.now().month

    log_filename = f"{year}-{month:02d}.log"
    log_filepath = os.path.join(logs_path, log_filename)

    # 检查日志文件是否存在
    if not os.path.exists(log_filepath):
        # 创建日志文件
        with open(log_filepath, 'w') as log_file:
            log_file.write(f"Log file created on {datetime.datetime.now()}\n")
        print(f"Created log file at {log_filepath}")
    else:
        print(f"Log file already exists at {log_filepath}")

    return log_filepath


def set_logging():
    import re
    from io import StringIO

    log_filepath = get_log_filepath()
    # 配置日志记录
    logging.basicConfig(
        filename=f"{log_filepath}",
        level=logging.DEBUG,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # 保存原始的stdout和stderr
    original_stdout = sys.stdout
    original_stderr = sys.stderr

    # 改进的stderr处理类，缓存输出以处理多行错误
    class StderrTee:
        def __init__(self, stream):
            self.stream = stream
            self.buffer = StringIO()
            self.line_buffer = ""

        def write(self, message):
            self.stream.write(message)

            # 将消息添加到缓冲区
            self.line_buffer += message

            # 检查是否有完整的行
            if '\n' in self.line_buffer:
                lines = self.line_buffer.split('\n')
                # 处理除最后一部分外的所有完整行
                for line in lines[:-1]:
                    if line:  # 避免空行
                        # 特殊处理连续的^符号行
                        if re.match(r'^\s*\^+\s*$', line):
                            self.buffer.write(line)
                        else:
                            # 如果缓冲区有内容且当前行不是^行，先记录缓冲区
                            buffered = self.buffer.getvalue()
                            if buffered.strip():
                                logging.error(buffered)
                                self.buffer = StringIO()
                            # 记录当前行
                            logging.error(line)

                # 保留最后一部分（可能是不完整的行）
                self.line_buffer = lines[-1]

        def flush(self):
            # 如果缓冲区有剩余内容，记录它
            if self.line_buffer.strip():
                logging.error(self.line_buffer)
                self.line_buffer = ""

            buffered = self.buffer.getvalue()
            if buffered.strip():
                logging.error(buffered)
                self.buffer = StringIO()

            self.stream.flush()

        # 确保支持其他方法
        def __getattr__(self, attr):
            return getattr(self.stream, attr)

    # 标准输出处理类
    class StdoutTee:
        def __init__(self, stream):
            self.stream = stream

        def write(self, message):
            self.stream.write(message)
            if message.strip():  # 避免记录空行
                logging.info(message)

        def flush(self):
            self.stream.flush()

        # 确保支持其他方法
        def __getattr__(self, attr):
            return getattr(self.stream, attr)

    # 替换标准输出和标准错误
    sys.stdout = StdoutTee(original_stdout)
    sys.stderr = StderrTee(original_stderr)

    # 捕获未处理的异常
    def exception_handler(exc_type, exc_value, exc_traceback):
        # 排除键盘中断异常的正常日志记录
        if issubclass(exc_type, KeyboardInterrupt):
            sys.__excepthook__(exc_type, exc_value, exc_traceback)
            return

        # 记录异常到日志
        traceback_text = ''.join(traceback.format_exception(exc_type, exc_value, exc_traceback))
        logging.error("未捕获的异常:\n%s", traceback_text)

        # 同时在原始stderr显示异常
        original_stderr.write(traceback_text)

    # 设置异常处理器
    sys.excepthook = exception_handler

    # 在脚本结束时刷新缓冲区
    import atexit
    def exit_handler():
        sys.stdout.flush()
        sys.stderr.flush()
        # 可以选择恢复原始stdout和stderr
        # sys.stdout = original_stdout
        # sys.stderr = original_stderr

    atexit.register(exit_handler)

set_logging()


print("==========================================================================")
print("==========================================================================")
print("将终端输出信息在日志文件中重新输出一遍")



