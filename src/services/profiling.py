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
from dataclasses import dataclass

import pyroscope
from ddtrace.profiling.profiler import Profiler

from config import Config
from .patch import patch_ddtrace_to_pyroscope


@dataclass
class ProfilingConfig:
    enabled: bool
    token: str
    service_name: str
    endpoint: str
    enable_memory_profiling: bool = True


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


class PyroscopeProfilingService(BaseProfilingService):
    def start(self):
        if not self.config.enabled:
            return

        pyroscope.configure(
            application_name=self.config.service_name,
            server_address=self.config.endpoint,
            auth_token=self.config.token,
            detect_subprocesses=True,
        )

    def stop(self):
        if self.config.enabled:
            pyroscope.shutdown()


class DatadogProfilingService(BaseProfilingService):
    def start(self):
        if not self.config.enabled:
            return

        patch_ddtrace_to_pyroscope(
            service_name=self.config.service_name,
            token=self.config.token,
            endpoint=self.config.endpoint,
            enable_memory_profiling=self.config.enable_memory_profiling,
        )
        prof = Profiler()
        prof.start()
