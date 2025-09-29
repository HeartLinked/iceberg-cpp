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

#pragma once

#include <string>

#include <cpr/cpr.h>
#include <nlohmann/json.hpp>

#include "iceberg/catalog/rest/iceberg_rest_export.h"
#include "iceberg/result.h"

/// \file iceberg/catalog/rest/http_client.h
/// Http client for Iceberg REST API.

namespace iceberg::rest {

class ICEBERG_REST_EXPORT HttpClient {
 public:
  explicit HttpClient(cpr::Url base_url, cpr::Header headers = {});

  /// \brief Sends a GET request to retrieve a resource.
  /// \param target The target URL.
  /// \param params The parameters to send with the request.
  /// \return The result of the request.
  template <typename R, typename E, std::uint16_t SuccessCode>
  iceberg::Result<R> Get(const std::string& target, const cpr::Parameters& params = {});

  /// \brief Sends a POST request to create a resource.
  /// \param target The target URL.
  /// \param body The body of the request.
  /// \return The result of the request.
  template <typename R, typename E, std::uint16_t SuccessCode>
  iceberg::Result<R> Post(const std::string& target, const cpr::Body& body);

  /// \brief Sends a HEAD request to retrieve a resource.
  /// \param target The target URL.
  /// \return The result of the request.
  template <typename E, std::uint16_t SuccessCode>
  iceberg::Status Head(const std::string& target);

  /// \brief Sends a DELETE request to delete a resource.
  /// \param target The target URL.
  /// \return The result of the request.
  template <typename E, std::uint16_t SuccessCode>
  iceberg::Status Delete(const std::string& target);

 private:
  // Internal helper function to process common logic
  template <typename R, typename E>
  static iceberg::Result<R> ProcessResponse(const cpr::Response& r,
                                            std::uint16_t success_code);

  cpr::Session session_;
};

}  // namespace iceberg::rest
