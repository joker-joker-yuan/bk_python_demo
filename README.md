# python学习笔记

## 1. 主入口（main.py）

### 1.1 导入逻辑
（1）导入标准库

```python
import logging # 记录日志
import sys # 当代码报错时，退出程序
import time # 阻塞主线程
from typing import List # 类型提示标注
```
（2）导入自定义模块

```python
from config import config # 自定义配置文件
from services.base import Service # 类型提示标注用
from services.otlp import OtlpService # otlp初始化资源
from services.profiling import DatadogProfilingService as ProfilingService # 性能分析服务
from services.querier import QuerierService # 轮询http服务
from services.server import HttpService # 开启http服务
```

### 1.2 主要代码

（1）日志初始化

```python
# 基础配置，输出格式：时间戳 + 日志级别 + 日志消息
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
# 日志名为当前模块名
logger = logging.getLogger(__name__)
```

（2）初始化服务列表并调用

```python
# 四个主要服务组成列表
services: List[Service] = [
    OtlpService(config),
    ProfilingService(config),
    HttpService(config),
    QuerierService(config),
]
# 循环遍历服务列表，调用服务的 start 方法
# 如果有异常，则输出日志，并且退出程序
for service in services:
    try:
        service.start()
        logger.info("[%s] service started", service)
    except Exception as e:
        logger.error("[%s] failed to start: %s", service, e)
        sys.exit(1)
```

（3）阻塞主线程，等待键盘中断

```python
try:
    while True:
        time.sleep(0.1) # 死循环睡眠等待异常
except KeyboardInterrupt:
    pass
```

（4）资源清理

```python
for service in services:
    try:
        service.stop() # 调用服务的 stop 方法做清理动作
        logger.info("[%s] service stopped", service)
    except Exception as e:
        logger.error("[%s] failed to stop: %s", service, e)
```

（5）主程序运行

```python
# 当前模块作为主程序运行时，执行 main 函数
if __name__ == "__main__":
    main()
```

## 2. 配置文件（config.py）

### 2.1 导入逻辑

```python
import os # 获取环境变量
from enum import Enum # 创建枚举类
```

### 2.2 主要代码

（1）导出枚举

```python
# 根据环境变量 OTLP_EXPORTER_TYPE 的值，选择对应类型
class ExporterType(Enum):
    GRPC = "grpc"
    HTTP = "http"
```

（2）配置类

```python
class Config:
    def __init__(self):
        # 根据环境变量的值，初始化配置
        self.debug = self._get_env_bool("DEBUG", False)
        self.token = os.getenv("TOKEN", "todo")
        self.service_name = os.getenv("SERVICE_NAME", "helloworld")
        self.otlp_endpoint = os.getenv("OTLP_ENDPOINT", "http://localhost:4317")
        self.otlp_exporter_type = ExporterType(os.getenv("OTLP_EXPORTER_TYPE", "grpc").lower())
        self.enable_logs = self._get_env_bool("ENABLE_LOGS", self.debug)
        self.enable_traces = self._get_env_bool("ENABLE_TRACES", self.debug)
        self.enable_metrics = self._get_env_bool("ENABLE_METRICS", self.debug)
        self.enable_profiling = self._get_env_bool("ENABLE_PROFILING", self.debug)
        self.enable_memory_profiling = self._get_env_bool("ENABLE_MEMORY_PROFILING", True)
        self.profiling_endpoint = os.getenv("PROFILING_ENDPOINT", "http://localhost:4040")
        self.http_scheme = "http"
        self.http_address = "0.0.0.0"
        self.http_port = 8080

    # 明确规定环境变量的 True 值，只能是 true，1，yes 中的一种
    @staticmethod
    def _get_env_bool(key: str, default: bool) -> bool:
        value = os.getenv(key)
        if value is None:
            return default
        return value.lower() in ("true", "1", "yes")
```

（3）创建配置对象

```python
# 创建 config 对象，供其它模块导入访问
config = Config()
```

## 3. 服务基类（services/base.py）

### 3.1 导入逻辑

```python
# 抽象基类，抽象方法
from abc import ABC, abstractmethod
```

### 3.2 主要代码

```python
# 定义抽象类和抽象方法，确保子类实现两个方法：start 和 stop
class Service(ABC):
    @abstractmethod
    def start(self):
        raise NotImplementedError

    @abstractmethod
    def stop(self):
        raise NotImplementedError
```

## 4. OTLP服务类（services/otlp.py）

### 4.1 导入逻辑

```python
import logging # 记录日志
import platform # 访问底层平台的数据，这里是获取操作系统名称
import socket # 获取当前机器的主机名
from dataclasses import dataclass # 创建数据类
from typing import Optional, Type # 类型提示

# 导入包：metrics，trace
from opentelemetry import metrics, trace

# 管理导出后后端的导出类
# 会将数据导出到收集器，再由收集器转发到可观测后端
from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter as GRPCLogExporter
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter as GRPCMetricExporter
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter as GRPCSpanExporter
from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter as HTTPLogExporter
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter as HTTPMetricExporter
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter as HTTPSpanExporter

# 提供检测应用程序的工具
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.sdk.metrics import Histogram, MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.metrics.view import ExplicitBucketHistogramAggregation, View
from opentelemetry.sdk.resources import ProcessResourceDetector, Resource, ResourceDetector, get_aggregated_resources
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

# 语义约定
from opentelemetry.semconv.resource import ResourceAttributes

# 集成性能检测
from pyroscope.otel import PyroscopeSpanProcessor

# 类型检查
from typing_extensions import assert_never

# 导入自定义 config 和 service
from config import Config, ExporterType
from services.base import Service

# 试图导入 OsResourceDetector
try:
    from opentelemetry.sdk.resources import OsResourceDetector
except ImportError:
    OsResourceDetector: Optional[Type[ResourceDetector]] = None
```

### 4.2 主要代码结构

```python
# 数据类，定义 OtlpConfig
@dataclass
class OtlpConfig:
    token: str
    service_name: str
    endpoint: str
    exporter_type: ExporterType
    enable_traces: bool
    enable_metrics: bool
    enable_logs: bool
    enable_profiling: bool

# 构建 Otlp 服务
class OtlpService(Service):
    # 初始化基本数据
    def __init__(self, config: Config):
        self.config = OtlpConfig(
            token=config.token,
            service_name=config.service_name,
            endpoint=config.otlp_endpoint,
            exporter_type=config.otlp_exporter_type,
            enable_traces=config.enable_traces,
            enable_metrics=config.enable_metrics,
            enable_logs=config.enable_logs,
            enable_profiling=config.enable_profiling,
        )
        self.tracer_provider: Optional[TracerProvider] = None
        self.meter_provider: Optional[MeterProvider] = None
        self.logger_provider: Optional[LoggerProvider] = None

    # 服务开始方法——构建生命周期中需要的钩子
    def start(self):
        resource = self._create_resource()

        if self.config.enable_traces:
            self._setup_traces(resource)

        if self.config.enable_metrics:
            self._setup_metrics(resource)

        if self.config.enable_logs:
            self._setup_logs(resource)

    # 服务结束方法——释放资源或确保尾部数据不丢失
    def stop(self):
        if self.tracer_provider:
            self.tracer_provider.shutdown()

        if self.meter_provider:
            self.meter_provider.shutdown()

        if self.logger_provider:
            self.logger_provider.shutdown()

    # 创建资源
    def _create_resource(self) -> Resource:
        pass

    # 配置 trace
    def _setup_traces(self, resource: Resource):
        pass

    # 配置 trace exporter
    def _setup_trace_exporter(self):
        pass

    # 配置 metric
    def _setup_metrics(self, resource: Resource):
        pass

    # 配置 metric exporter 
    def _setup_metric_exporter(self):
        pass

    # 配置 log
    def _setup_logs(self, resource: Resource):
        pass

    # 配置 log exporter
    def _setup_log_exporter(self):
        pass

    # 定义对象的“可打印”字符串表示形式
    def __str__(self):
        pass
```

### 4.3 四条主线

（1）创建资源

```python

class OtlpService(Service):
    def start(self):
        # 创建资源的逻辑，封装在内部方法
        resource = self._create_resource()

    def _create_resource(self) -> Resource:
        # detectors 存放需要的资源检测器
        detectors = [ProcessResourceDetector()]
        if OsResourceDetector is not None:
            detectors.append(OsResourceDetector())

        # 初始化资源对象
        # create 提供了部分 SDK 默认属性
        initial_resource = Resource.create(
            {
                # ❗❗【非常重要】请传入应用 Token
                "bk.data.token": self.config.token,
                # ❗❗【非常重要】应用服务唯一标识
                ResourceAttributes.SERVICE_NAME: self.config.service_name, # 服务名
                ResourceAttributes.OS_TYPE: platform.system().lower(), # 操作系统名称
                ResourceAttributes.HOST_NAME: socket.gethostname(), # 机器主机名
            }
        )
        # 检测器检测到的信息会合入到初始资源对象
        return get_aggregated_resources(detectors, initial_resource)
```

（2）配置 trace

```python
class OtlpService(Service):
    def start(self):
        # 配置 trace 由 enable_traces 配置项控制
        # 传入主线（1）创建得到的资源对象
        if self.config.enable_traces:
            self._setup_traces(resource)

    def _setup_traces(self, resource: Resource):
        # 创建满足配置的导出器
        otlp_exporter = self._setup_trace_exporter()

        # 创建一个新的线程，通过 otlp_exporter 去将收集到的 span 批量发送出去
        span_processor = BatchSpanProcessor(otlp_exporter)
        
        # 初始化 tracer provider
        self.tracer_provider = TracerProvider(resource=resource)

        # 添加需要的 span processor
        self.tracer_provider.add_span_processor(span_processor)
        if self.config.enable_profiling:
            self.tracer_provider.add_span_processor(PyroscopeSpanProcessor())
        
        # 配置全局的 tracer_provider，无法覆盖
        trace.set_tracer_provider(self.tracer_provider)

    def _setup_trace_exporter(self):
        # 导出器由 exporter_typ 和 endpoint 配置项控制
        # 导出器可以将数据导出到收集器
        if self.config.exporter_type == ExporterType.GRPC:
            return GRPCSpanExporter(endpoint=self.config.endpoint, insecure=True)
        elif self.config.exporter_type == ExporterType.HTTP:
            return HTTPSpanExporter(endpoint=f"{self.config.endpoint}/v1/traces")
        else:
            assert_never(self.config.exporter_type)

```
（3）配置 metric

```python

class OtlpService(Service):
    def start(self):
        # 配置 metrics 由 enable_metrics 配置项控制
        # 传入主线（1）创建得到的资源对象
        if self.config.enable_metrics:
            self._setup_metrics(resource)

    def _setup_metrics(self, resource: Resource):
        # 创建满足配置的导出器
        otlp_exporter = self._setup_metric_exporter()

        # 创建一个新的线程，周期性的读取收集
        # 通过 otlp exporter 导出去
        # _collect 方法是在初始化 provider 时绑定的
        reader = PeriodicExportingMetricReader(otlp_exporter)

        # 配置如何处理和聚合这些指标
        histogram_view = View(
            instrument_type=Histogram, # 直方图类型的指标
            instrument_unit="s", # 测量单位是秒
            aggregation=ExplicitBucketHistogramAggregation(
                boundaries=[0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 1.0, 2.5, 5.0]
            ), # 将测量值分配到不同的桶里
        )

        # 绑定 _collect 方法到 reader 上
        # 在测量消费者初始化时，将reader 和 readerstorage 一一映射
        self.meter_provider = MeterProvider(resource=resource, metric_readers=[reader], views=[histogram_view])

        # 配置全局的 meter_provider，无法覆盖
        metrics.set_meter_provider(self.meter_provider)

    def _setup_metric_exporter(self):
        # 导出器由 exporter_typ 和 endpoint 配置项控制
        # 导出器可以将数据导出到收集器
        if self.config.exporter_type == ExporterType.GRPC:
            return GRPCMetricExporter(endpoint=self.config.endpoint, insecure=True)
        elif self.config.exporter_type == ExporterType.HTTP:
            return HTTPMetricExporter(endpoint=f"{self.config.endpoint}/v1/metrics")
        else:
            assert_never(self.config.exporter_type)

```


（4）配置 log

```python

class OtlpService(Service):
    def start(self):
        # 配置 log 由 enable_logs 配置项控制
        # 传入主线（1）创建得到的资源对象
        if self.config.enable_logs:
            self._setup_logs(resource)

    def _setup_logs(self, resource: Resource):
        # 创建满足配置的导出器
        # 流程和 trace 相似
        otlp_exporter = self._setup_log_exporter()
        self.logger_provider = LoggerProvider(resource=resource)
        self.logger_provider.add_log_record_processor(BatchLogRecordProcessor(otlp_exporter))
        handler = LoggingHandler(level=logging.NOTSET, logger_provider=self.logger_provider)
        logging.getLogger("otel").addHandler(handler)

    def _setup_log_exporter(self):
        # 导出器由 exporter_typ 和 endpoint 配置项控制
        # 导出器可以将数据导出到收集器
        if self.config.exporter_type == ExporterType.GRPC:
            return GRPCLogExporter(endpoint=self.config.endpoint, insecure=True)
        elif self.config.exporter_type == ExporterType.HTTP:
            return HTTPLogExporter(endpoint=f"{self.config.endpoint}/v1/logs")
        else:
            assert_never(self.config.exporter_type)

```

## 5. web服务类（services/server.py）

### 5.1 导入逻辑

```python
import logging # 记录日志
import random # 生成随机数，或者随机选择列表中的某一项
import threading # 开启一个新的线程做 http 服务
import time # 用来计算时长
from dataclasses import dataclass # 定义配置数据类
from typing import Iterable, Tuple # 类型提示

# Flask 创建应用实例，Request 做类型提示，request 记录请求的上下文变量
from flask import Flask, Request, request
from opentelemetry import metrics, trace # 用来开启测量

# 在 Flask 应用中集成分布式追踪和度量功能
from opentelemetry.instrumentation.flask import FlaskInstrumentor

# CallbackOptions 回调函数的参数，这里用来做类型提示
# Observation 回调函数的返回数据类型
from opentelemetry.metrics import CallbackOptions, Observation

# Span 用来做类型提示
# Status 用来实例化 Span 的状态对象
# StatusCode 预定义的枚举值
from opentelemetry.sdk.trace import Span, Status, StatusCode

# 导入自定义配置
from config import Config
```

### 5.2 主要代码结构

```python
# 获取日志记录器
logger = logging.getLogger(__name__)
otel_logger = logging.getLogger("otel")
logging.getLogger("werkzeug").disabled = True  # 关闭 werkzeug 日志输出

# 定义数据类：存储需要的服务配置
@dataclass
class ServerConfig:
    service_name: str
    scheme: str
    address: str
    port: int

# 自定义异常类
class APIException(Exception):
    status_code = 500

# /helloworld 路由的处理
class HelloWorldHandler:
    pass

# 开启 flask web 服务
class HttpService:
    pass

```

### 5.3 http 服务开启

```python
class HttpService:
    def __init__(self, config: Config):
        # 基础配置
        service_name = config.service_name
        self.config = ServerConfig(
            service_name=service_name,
            scheme=config.http_scheme,
            address=config.http_address,
            port=config.http_port,
        )
        # 创建 flask 应用实例
        self.app = Flask(service_name)
        # 自动化添加分布式跟踪
        FlaskInstrumentor().instrument_app(self.app)

        # 实例化 HelloWorldHandler 实例
        # 实例方法 handle 用来处理路由 /helloworld 的请求
        self.handler = HelloWorldHandler(service_name)
        self.app.add_url_rule("/helloworld", view_func=self.handler.handle)

        # 注册自定义的错误处理器，用于捕获和处理 APIException 类型的异常
        self.app.register_error_handler(APIException, self._error_handler)

    # start 开启一个线程跑 runserver
    def start(self):
        server_thread = threading.Thread(target=self._run_server, daemon=True)
        server_thread.start()
        logger.info("[%s] start to listen http server at %s:%s", self, self.config.address, self.config.port)

    # APIException 类型异常发生时的相应方法
    @staticmethod
    def _error_handler(e: APIException) -> Tuple[str, int]:
        return str(e), e.status_code


    def _run_server(self):
        self.app.run(host=self.config.address, port=self.config.port)

    @staticmethod
    def stop():
        pass

    def __str__(self):
        return "http"
```

### 5.4 路由处理类

```python
class HelloWorldHandler:
    # 国家枚举值
    COUNTRIES = [
        "United States",
        "Canada",
        "United Kingdom",
        "Germany",
        "France",
        "Japan",
        "Australia",
        "China",
        "India",
        "Brazil",
    ]
    # 控制错误率
    ERROR_RATE = 0.1
    # 自定义的错误信息
    CUSTOM_ERROR_MESSAGES = [
        "mysql connect timeout",
        "user not found",
        "network unreachable",
        "file not found",
    ]

    # 初始化 handle 实例
    def __init__(self, service_name: str):
        # 初始化 tracer 和 meter 的实例
        self.tracer = trace.get_tracer(service_name)
        self.meter = metrics.get_meter(service_name)

        # 创建一个计数器指标
        self.requests_total = self.meter.create_counter(
            "requests_total",
            description="Total number of HTTP requests",
        )
        # 创建一个直方图指标
        self.task_execute_duration_seconds = self.meter.create_histogram(
            "task_execute_duration_seconds",
            unit="s",
            description="Task execute duration in seconds",
        )
        # 创建一个仪表指标
        # Metrics（指标）- 使用 Gauge 类型指标
        self.meter.create_observable_gauge(
            "memory_usage",
            callbacks=[self.generate_random_usage],
            unit="%",
            description="Memory usage",
        )

    # 仪表指标的回调函数
    @staticmethod
    def generate_random_usage(options: CallbackOptions) -> Iterable[Observation]:
        usage = round(random.random(), 4)
        yield Observation(usage, {})

    # 路由 /helloworld 的处理方法
    def handle(self) -> str:
        # 第一个自定义路由处理的 span（handle/hello_world）
        with self.tracer.start_as_current_span(
            "handle/hello_world", record_exception=False, set_status_on_exception=False
        ):
            country = self.choice_country()
            otel_logger.info("get country -> %s", country)

            # Logs（日志）
            self.logs_demo(request)

            # Metrics（指标） - Counter 类型
            self.metrics_counter_demo(country)
            # Metrics（指标） - Histograms 类型
            self.metrics_histogram_demo()
            
            # 第二个自定义路由处理的 span（custom_span_demo/do_something）
            # Traces（调用链）- 自定义 Span
            self.traces_custom_span_demo()

            # 第三个自定义路由处理的 span（span_event_demo/do_something）
            # Traces（调用链）- Span 事件
            self.traces_span_event_demo()

            # 这里会修改第一个 span 的状态
            # Traces（调用链）- 模拟错误
            self.traces_random_error_demo()

            # 路由返回值
            return self.generate_greeting(country)

    # Logs（日志）打印日志
    @staticmethod
    def logs_demo(req: Request):
        otel_logger.info("received request: %s %s", req.method, req.path)

    def choice_country(self) -> str:
        return random.choice(self.COUNTRIES)

    # Metrics（指标）- 使用 Counter 类型指标
    # Refer: https://opentelemetry.io/docs/languages/python/instrumentation/#creating-and-using-synchronous-instruments
    def metrics_counter_demo(self, country: str):
        self.requests_total.add(1, {"country": country})

    # Metrics（指标）- 使用 Histogram 类型指标
    def metrics_histogram_demo(self):
        start_time = time.time()
        self.do_something(100)
        duration = time.time() - start_time
        self.task_execute_duration_seconds.record(duration)

    # Traces（调用链）- 增加自定义 Span
    # Refer: https://opentelemetry.io/docs/languages/python/instrumentation/#creating-spans
    def traces_custom_span_demo(self):
        with self.tracer.start_as_current_span("custom_span_demo/do_something") as span:
            # 添加 Span 自定义属性
            # Refer: https://opentelemetry.io/docs/languages/python/instrumentation/#add-attributes-to-a-span
            # 也可以使用 span.set_attributes() 批量设置
            span.set_attribute("helloworld.kind", 1)
            span.set_attribute("helloworld.step", "traces_custom_span_demo")

            self.do_something(50)

    # Traces（调用链）- Span 事件
    # Refer: https://opentelemetry.io/docs/languages/python/instrumentation/#adding-events
    def traces_span_event_demo(self):
        with self.tracer.start_as_current_span("span_event_demo/do_something") as span:
            attributes = {
                "helloworld.kind": 2,
                "helloworld.step": "traces_span_event_demo",
            }
            span.add_event("Before do_something", attributes)
            self.do_something(50)
            span.add_event("After do_something", attributes)

    # Traces（调用链）- 异常事件、状态
    # Refer: https://opentelemetry.io/docs/languages/python/instrumentation/#record-exceptions-in-spans
    def traces_random_error_demo(self):
        try:
            if random.random() < self.ERROR_RATE:
                error_message = random.choice(self.CUSTOM_ERROR_MESSAGES)
                raise APIException(error_message)
        except APIException as e:
            otel_logger.error("[traces_random_error_demo] got error -> %s", e)
            current_span: Span = trace.get_current_span()
            current_span.set_status(Status(StatusCode.ERROR, str(e)))
            current_span.record_exception(e)
            raise

    @staticmethod
    def generate_greeting(country: str) -> str:
        return f"Hello World, {country}!"

    @staticmethod
    def do_something(max_ms: int):
        duration = max(10, random.randint(0, max_ms)) / 1000
        i = 0
        start = time.time()
        while time.time() - start < duration:
            i += 1

```

## 6. 查询服务类（services/querier.py）

### 6.1 导入逻辑

```python
import logging # 获取日志记录
import threading # 开启线程，不断轮询 http 服务
from dataclasses import dataclass # 定义配置数据类

import requests # 用来发起请求
from opentelemetry import trace # 导入 trace 包
from opentelemetry.instrumentation.requests import RequestsInstrumentor # 自动捕获和追踪通过 requests 库发起的 HTTP 请求

from config import Config # 导入自定义配置
```

### 6.2 主要代码结构

```python
# 获取日志记录器
logger = logging.getLogger(__name__)
otel_logger = logging.getLogger("otel")

# 设置等待时间
INTERVAL = 3

# 定义配置的数据类
@dataclass
class QuerierConfig:
    service_name: str
    scheme: str
    address: str
    port: int

    @property
    def url(self) -> str:
        return f"{self.scheme}://{self.address}:{self.port}/helloworld"

# 定义轮询类
class QuerierService:
    pass
```

### 6.3 轮询类代码

```python
# 请求报文参考
"""
GET /helloworld HTTP/1.1
Host: 0.0.0.0:8080
User-Agent: python-requests/2.32.3
Accept-Encoding: gzip, deflate
Accept: */*
Connection: keep-alive
traceparent: 00-49f54509d1cd93298e60b4fdc3ab15c9-3a8f96036b6d5e23-01
"""
class QuerierService:
    def __init__(self, config: Config):
        # 初始化配置实例
        self.config = QuerierConfig(
            service_name=config.service_name,
            scheme=config.http_scheme,
            address=config.http_address,
            port=config.http_port,
        )
        # 初始化 tracer 实例
        self.tracer = trace.get_tracer(self.config.service_name)
        # 实例化 Event 实例，用来控制是否 stop
        self.stopped = threading.Event()

        # 启动 request 自动化跟踪
        # 包裹了 session 的 send 方法
        RequestsInstrumentor().instrument()

    def start(self):
        # 清空 Event，开启新线程去轮询
        self.stopped.clear()
        thread = threading.Thread(target=self._loop_query_hello_world)
        thread.start()

    def stop(self):
        # 停止时，通过 Event 让新线程跳出循环
        self.stopped.set()

    # 新线程执行的循环
    def _loop_query_hello_world(self):
        logger.info("[%s] start loop_query_hello_world to periodically request %s", self, self.config.url)

        while not self.stopped.wait(INTERVAL):
            try:
                self._query_hello_world()
            except Exception as e:
                otel_logger.error("[query_hello_world] got error -> %s", e)

        logger.info("[%s] loop_query_hello_world stopped", self)

    # 轮询的主要逻辑
    def _query_hello_world(self):
        # 会开启一个 span（caller/query_hello_world）
        # 将 trace 信息注入 header
        with self.tracer.start_as_current_span("caller/query_hello_world"):
            otel_logger.info("[query_hello_world] send request")
            response = requests.get(self.config.url)
            otel_logger.info("[query_hello_world] received: %s", response.text)

    def __str__(self):
        return "querier"

```

## 7. 性能分析服务类（services/profiling.py）

### 7.1 导入逻辑

```python
# 数据类
from dataclasses import dataclass

# 性能分析
from ddtrace.profiling.profiler import Profiler

# 自定义配置
from config import Config

# 自定义函数：将ddtrace收集到的数据发到pyroscope
from .patch import patch_ddtrace_to_pyroscope

```

### 7.2 主要代码

```python
# 定义配置数据类
@dataclass
class ProfilingConfig:
    enabled: bool
    token: str
    service_name: str
    endpoint: str
    enable_memory_profiling: bool = True


# 定义性能分析基类
class BaseProfilingService:
    def __init__(self, config: Config):
        self.config = ProfilingConfig(
            # ❗❗【非常重要】请传入应用 Token
            token=config.token,
            # ❗❗【非常重要】应用服务唯一标识
            service_name=config.service_name,
            # ❗❗【非常重要】数据上报地址，请根据页面指引提供的接入地址进行填写
            endpoint=config.profiling_endpoint,
            enabled=config.enable_profiling,
            enable_memory_profiling=config.enable_memory_profiling,
        )
    def start(self):
        pass

    def stop(self):
        pass

    def __str__(self):
        return "profiling"

# 继承性能分析基类，实现 start 方法
class DatadogProfilingService(BaseProfilingService):
    def start(self):
        if not self.config.enabled:
            return

        # 将 ddtrace 的数据也打包到 pyroscope
        patch_ddtrace_to_pyroscope(
            service_name=self.config.service_name,
            token=self.config.token,
            endpoint=self.config.endpoint,
            enable_memory_profiling=self.config.enable_memory_profiling,
        )
        prof = Profiler()
        prof.start()
```

## 8. 补丁服务类（services/patch.py）

这一块好复杂

### 8.1 导入逻辑

```python
import binascii 
import gzip
import json
import logging
import os
import typing
import urllib.parse

import six
import tenacity
from ddtrace.internal import agent
from ddtrace.profiling import exporter
from ddtrace.profiling.exporter.http import UploadFailed
from ddtrace.profiling.exporter.pprof import PprofExporter  # noqa
from six.moves import http_client
```