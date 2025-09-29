/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

// http_client.cpp
#include "http_client.h"

#include <nlohmann/json.hpp>

#include "cpr/body.h"

namespace iceberg::rest {

template <typename R, typename E, std::uint16_t SuccessCode>
Result<R> HttpClient::Get(const std::string& target, const cpr::Parameters& params) {
  session_.SetUrl(cpr::Url{session_.GetFullRequestUrl() + target});
  session_.SetParameters(params);
  cpr::Response r = session_.Get();
  return ProcessResponse<R, E>(r, SuccessCode);
}

template <typename R, typename E, std::uint16_t SuccessCode>
Result<R> HttpClient::Post(const std::string& target, const cpr::Body& body) {
  session_.SetUrl(cpr::Url{session_.GetFullRequestUrl() + target});
  session_.SetBody(body);
  cpr::Response r = session_.Post();
  return ProcessResponse<R, E>(r, SuccessCode);
}

template <typename E, std::uint16_t SuccessCode>
Result<void> HttpClient::Head(const std::string& target) {
  session_.SetUrl(cpr::Url{session_.GetFullRequestUrl() + target});
  cpr::Response r = session_.Head();
  return ProcessResponse<void, E>(r, SuccessCode);
}

template <typename E, std::uint16_t SuccessCode>
Result<void> HttpClient::Delete(const std::string& target) {
  session_.SetUrl(cpr::Url{session_.GetFullRequestUrl() + target});
  cpr::Response r = session_.Delete();
  return ProcessResponse<void, E>(r, SuccessCode);
}

template <typename R, typename E>
Result<R> HttpClient::ProcessResponse(const cpr::Response& r,
                                      std::uint16_t success_code) {
  if (r.error) {
    return IOError("Network error: {}", r.error.message);
  }

  if (r.status_code != success_code) {
    try {
      auto json_err = nlohmann::json::parse(r.text);
      E api_error = json_err.get<E>();
      return std::unexpected(static_cast<Error>(api_error));
    } catch (const nlohmann::json::exception& e) {
      return JsonParseError("Failed to parse error response (status {}): {}. Body: {}",
                            r.status_code, e.what(), r.text);
    }
  }

  // 对于 void 返回类型 (Status)
  if constexpr (std::is_void_v<R>) {
    return {};
  } else {
    try {
      auto json_ok = nlohmann::json::parse(r.text);
      return json_ok.get<R>();
    } catch (const nlohmann::json::exception& e) {
      return JsonParseError("Failed to parse success response: {}. Body: {}", e.what(),
                            r.text);
    }
  }
}

}  // namespace iceberg::rest
