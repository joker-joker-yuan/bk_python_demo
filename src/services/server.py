# -*- coding: utf-8 -*-
"""
Tencent is pleased to support the open source community by making
蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
Copyright (C) 2022 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://opensource.org/licenses/MIT
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.
"""
import logging
import random
import threading
import time
from dataclasses import dataclass
from typing import Iterable, Tuple, List

from flask import Flask, Request, request

from opentelemetry import metrics, trace
from opentelemetry.propagate import inject, extract
from opentelemetry.context import get_current
from opentelemetry.instrumentation.flask import FlaskInstrumentor
from opentelemetry.metrics import CallbackOptions, Observation
from opentelemetry.sdk.trace import Span, Status, StatusCode

from config import Config

logger = logging.getLogger(__name__)
otel_logger = logging.getLogger("otel")
logging.getLogger("werkzeug").disabled = True  # 关闭 werkzeug 日志输出


@dataclass
class ServerConfig:
    service_name: str
    scheme: str
    address: str
    port: int


class APIException(Exception):
    status_code = 500


class TravelHandler:
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
    ERROR_RATE = 0.1
    SLEEP_RATE = 0.2
    CUSTOM_ERROR_MESSAGES = [
        "mysql connect timeout",
        "user not found",
        "network unreachable",
        "file not found",
    ]
    def __init__(self, service_name: str):
        self.tracer = trace.get_tracer(service_name)
        self.meter = metrics.get_meter(service_name)

        # counter
        # 计算 visit 函数调用次数
        self.visit_requests_total = self.meter.create_counter(
            "visit_requests_total",
            description="Total number of HTTP requests",
        )

        # histogram
        # 计算 visit 函数耗时
        self.visit_execute_duration_seconds = self.meter.create_histogram(
            "visit_execute_duration_seconds",
            unit="s",
            description="visit function execute duration in seconds",
        )
        # 计算 parallel_visit 函数耗时
        self.parallel_visit_execute_duration_seconds = self.meter.create_histogram(
            "parallel_visit_execute_duration_seconds",
            unit="s",
            description="parallel visit function execute duration in seconds",
        )
        # 计算 serial_visit 函数耗时
        self.serial_visit_execute_duration_seconds = self.meter.create_histogram(
            "serial_visit_execute_duration_seconds",
            unit="s",
            description="serial visit function execute duration in seconds",
        )

    def visit_handle(self) -> str:
        # 不自动设置异常状态和记录异常，以展示手动设置方法 (traces_random_error_demo)
        with self.tracer.start_as_current_span(
            "travel/visit_handle", record_exception=False, set_status_on_exception=False
        ):
            self.logs_demo(request)
            self.countries = self.choice_countries()
            otel_logger.info("get countries -> %s", self.countries)

            self.parallel_visit()

            self.serial_visit()

            return "Travel Success"
    
    def choice_countries(self) -> List[str]:
        return random.sample(self.COUNTRIES, 3)
    
    @staticmethod
    def logs_demo(req: Request):
        otel_logger.info("received request: %s %s", req.method, req.path)
    
    def parallel_task(self, country, trace_context):
        context_content = extract(trace_context)
        with self.tracer.start_as_current_span("travel/parallel_task", context=context_content) as span:
            self.visit2(country)

    def visit2(self, country: str):
        start_time = time.time()
        with self.tracer.start_as_current_span("travel/visit2") as span:
            self.visit_requests_total.add(1, {"country": country})
            random_value = random.random()

            if random_value < self.ERROR_RATE:
                try:
                    error_message = random.choice(self.CUSTOM_ERROR_MESSAGES)
                    raise APIException(error_message)
                except APIException as e:
                    otel_logger.error("[traces_random_error_demo] got error -> %s", e)
                    current_span: Span = trace.get_current_span()
                    current_span.set_status(Status(StatusCode.ERROR, str(e)))
                    current_span.record_exception(e)
                    raise
                finally:
                    duration = time.time() - start_time
                    self.visit_execute_duration_seconds.record(duration)
            elif random_value < self.SLEEP_RATE:
                time.sleep(random_value / 10)
            
            duration = time.time() - start_time
            self.visit_execute_duration_seconds.record(duration)
    
    def visit1(self, country: str):
        start_time = time.time()
        with self.tracer.start_as_current_span("travel/visit1") as span:
            self.visit_requests_total.add(1, {"country": country})
            random_value = random.random()

            if random_value < self.ERROR_RATE:
                try:
                    error_message = random.choice(self.CUSTOM_ERROR_MESSAGES)
                    raise APIException(error_message)
                except APIException as e:
                    otel_logger.error("[traces_random_error_demo] got error -> %s", e)
                    current_span: Span = trace.get_current_span()
                    current_span.set_status(Status(StatusCode.ERROR, str(e)))
                    current_span.record_exception(e)
                    raise
                finally:
                    duration = time.time() - start_time
                    self.visit_execute_duration_seconds.record(duration)
            elif random_value < self.SLEEP_RATE:
                time.sleep(random_value / 10)
            
            duration = time.time() - start_time
            self.visit_execute_duration_seconds.record(duration)
    
    def parallel_visit(self):
        with self.tracer.start_as_current_span("travel/parallel_visit") as span:
            trace_context = {}
            inject(trace_context, get_current())
            otel_logger.info("parallel_visit start")
            threads = []
            for country in self.countries:
                thread = threading.Thread(target=self.parallel_task, args=(country, trace_context))
                threads.append(thread)
                thread.start()
            for thread in threads:
                thread.join()
            otel_logger.info("parallel_visit end")

    def serial_visit(self):
        with self.tracer.start_as_current_span("travel/serial_visit") as span:
            otel_logger.info("serial_visit start")
            for country in self.countries:
                self.visit1(country)
            otel_logger.info("serial_visit end")
        


class HelloWorldHandler:
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
    ERROR_RATE = 0.1
    CUSTOM_ERROR_MESSAGES = [
        "mysql connect timeout",
        "user not found",
        "network unreachable",
        "file not found",
    ]

    def __init__(self, service_name: str):
        self.tracer = trace.get_tracer(service_name)
        self.meter = metrics.get_meter(service_name)

        self.requests_total = self.meter.create_counter(
            "requests_total",
            description="Total number of HTTP requests",
        )
        self.task_execute_duration_seconds = self.meter.create_histogram(
            "task_execute_duration_seconds",
            unit="s",
            description="Task execute duration in seconds",
        )
        # Metrics（指标）- 使用 Gauge 类型指标
        self.meter.create_observable_gauge(
            "memory_usage",
            callbacks=[self.generate_random_usage],
            unit="%",
            description="Memory usage",
        )

    @staticmethod
    def generate_random_usage(options: CallbackOptions) -> Iterable[Observation]:
        usage = round(random.random(), 4)
        yield Observation(usage, {})

    def handle(self) -> str:
        # 不自动设置异常状态和记录异常，以展示手动设置方法 (traces_random_error_demo)
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

            # Traces（调用链）- 自定义 Span
            self.traces_custom_span_demo()
            # Traces（调用链）- Span 事件
            self.traces_span_event_demo()
            # Traces（调用链）- 模拟错误
            self.traces_random_error_demo()

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


class HttpService:
    def __init__(self, config: Config):
        service_name = config.service_name
        self.config = ServerConfig(
            service_name=service_name,
            scheme=config.http_scheme,
            address=config.http_address,
            port=config.http_port,
        )
        self.app = Flask(service_name)
        FlaskInstrumentor().instrument_app(self.app)

        self.handler = HelloWorldHandler(service_name)
        self.travel_handler = TravelHandler(service_name)

        self.app.add_url_rule("/helloworld", view_func=self.handler.handle)
        self.app.add_url_rule("/travel", view_func=self.travel_handler.visit_handle)

        self.app.register_error_handler(APIException, self._error_handler)

    def start(self):
        server_thread = threading.Thread(target=self._run_server, daemon=True)
        server_thread.start()
        logger.info("[%s] start to listen http server at %s:%s", self, self.config.address, self.config.port)

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
