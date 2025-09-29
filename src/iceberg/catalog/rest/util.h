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

#include <format>
#include <string>
#include <string_view>

#include "iceberg/table_identifier.h"

namespace iceberg::rest {

std::string_view TrimTrailingSlash(std::string_view uri_sv) {
  if (uri_sv.ends_with('/')) {
    uri_sv.remove_suffix(1);
  }
  return uri_sv;
}

std::string EncodeUriComponent(std::string_view value) {
  std::string escaped;
  escaped.reserve(value.length());
  for (const unsigned char c : value) {
    if (isalnum(c) || c == '-' || c == '_' || c == '.' || c == '~') {
      escaped += c;
    } else {
      std::format_to(std::back_inserter(escaped), "%{:02X}", c);
    }
  }
  return escaped;
}

/// \brief Encode the namespace in URL format
std::string EncodeNamespaceForUrl(const Namespace& ns) {
  if (ns.levels.empty()) {
    return "";
  }

  std::string joined_string;
  joined_string.append(ns.levels.front());
  for (size_t i = 1; i < ns.levels.size(); ++i) {
    joined_string.append("\x1F");
    joined_string.append(ns.levels[i]);
  }

  return EncodeUriComponent(joined_string);
}

}  // namespace iceberg::rest
