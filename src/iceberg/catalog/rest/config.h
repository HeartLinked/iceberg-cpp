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

// namespace iceberg::rest {

// class ICEBERG_REST_EXPORT RestCatalogConfig {
//  public:
//   std::string uri;
//   std::optional<std::string> warehouse;
//   std::map<std::string, std::string> props;

//   // Endpoint builder methods
//   std::string config_endpoint() const;
//   std::string namespaces_endpoint() const;
//   std::string namespace_endpoint(const Namespace& ns) const;
//   std::string tables_endpoint(const Namespace& ns) const;
//   std::string table_endpoint(const TableIdentifier& table) const;
//   std::string rename_table_endpoint() const;
// };

// }  // namespace iceberg::rest

#pragma once

#include <format>
#include <map>
#include <optional>
#include <string>

#include "constant.h"
#include "iceberg/catalog/rest/iceberg_rest_export.h"
#include "iceberg/table_identifier.h"
#include "iceberg/util/config.h"
#include "util.h"

/// \file iceberg/catalog/rest/config.h
/// RestCatalogConfig implementation for Iceberg REST API.

namespace iceberg::rest {

class ICEBERG_REST_EXPORT RestCatalogConfig : public ConfigBase<RestCatalogConfig> {
 public:
  const std::string& GetUri() const { return uri_; }

  const std::optional<std::string>& GetWarehouse() const { return warehouse_; }

  RestCatalogConfig& SetUri(std::string uri) {
    uri_ = std::move(uri);
    return *this;
  }

  RestCatalogConfig& SetWarehouse(std::string warehouse) {
    warehouse_ = std::move(warehouse);
    return *this;
  }

  std::string GetConfigEndpoint() const {
    return std::format("{}/{}/config", TrimTrailingSlash(uri_), kPathV1);
  }

  /// \brief Get the namespaces endpoint
  std::string GetNamespacesEndpoint() const {
    return std::format("{}/{}/namespaces", TrimTrailingSlash(uri_), kPathV1);
  }

  /// \brief Get the namespace endpoint
  std::string GetNamespaceEndpoint(const Namespace& ns) const {
    return std::format("{}/{}/namespaces/{}", TrimTrailingSlash(uri_), kPathV1,
                       EncodeNamespaceForUrl(ns));
  }

  /// \brief Get the tables endpoint
  std::string GetTablesEndpoint(const Namespace& ns) const {
    return std::format("{}/{}/namespaces/{}/tables", TrimTrailingSlash(uri_), kPathV1,
                       EncodeNamespaceForUrl(ns));
  }

  /// \brief Get the rename table endpoint
  std::string GetRenameTableEndpoint() const {
    return std::format("{}/{}/tables/rename", TrimTrailingSlash(uri_), kPathV1);
  }

  /// \brief Get the table endpoint
  std::string GetTableEndpoint(const TableIdentifier& table) const {
    return std::format("{}/{}/namespaces/{}/tables/{}", TrimTrailingSlash(uri_), kPathV1,
                       EncodeNamespaceForUrl(table.ns), table.name);
  }

 private:
  std::string uri_;
  std::optional<std::string> warehouse_;
};

}  // namespace iceberg::rest
