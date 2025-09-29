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

#include "rest_catalog.h"

#include <memory>
#include <utility>

#include <cpr/cpr.h>

#include "constant.h"
#include "iceberg/catalog.h"
#include "iceberg/result.h"
#include "iceberg/table.h"

namespace iceberg::rest {

Result<RestCatalog> RestCatalog::Make(RestCatalogConfig config) {
  // TODO(Feiyang Li): Support customize http headers
  cpr::Header headers{
      {std::string(kHeaderContentType), std::string(kMimeTypeApplicationJson)},
      {std::string(kHeaderAccept), std::string(kMimeTypeApplicationJson)},
      {std::string(kHeaderXClientVersion), std::string(kClientVersion)},
      {std::string(kHeaderUserAgent), std::string(kUserAgent)}};

  // TODO(Feiyang Li): Add OAuth support
  std::unique_ptr<HttpClient> client =
      std::make_unique<HttpClient>(config.GetUri(), headers);
  // iceberg::Status status = catalog.UpdateConfig();
  // if (!status.ok()) {
  //     return status;
  // }
  return RestCatalog(std::move(config), std::move(client));
}

RestCatalog::RestCatalog(RestCatalogConfig config, std::unique_ptr<HttpClient> client)
    : config_(std::move(config)), client_(std::move(client)) {}

std::string_view RestCatalog::name() const { return config_.GetUri(); }

Result<std::vector<Namespace>> RestCatalog::ListNamespaces(const Namespace& ns) const {
  // cpr::Parameters params;
  // if (ns.parent) {
  //     params.Add({"parent", ns.parent->encode_in_url()});
  // }

  // auto result = client_->Get<ListNamespacesResponse, ApiErrorResponse, 200>(
  //     config_.GetNamespacesEndpoint(), params);

  return NotImplemented("Not implemented");
}

Status RestCatalog::CreateNamespace(
    const Namespace& ns, const std::unordered_map<std::string, std::string>& properties) {
  return NotImplemented("Not implemented");
}

Result<std::unordered_map<std::string, std::string>> RestCatalog::GetNamespaceProperties(
    const Namespace& ns) const {
  return NotImplemented("Not implemented");
}

Status RestCatalog::DropNamespace(const Namespace& ns) {
  return NotImplemented("Not implemented");
}

Result<bool> RestCatalog::NamespaceExists(const Namespace& ns) const {
  return NotImplemented("Not implemented");
}

Status RestCatalog::UpdateNamespaceProperties(
    const Namespace& ns, const std::unordered_map<std::string, std::string>& updates,
    const std::unordered_set<std::string>& removals) {
  return NotImplemented("Not implemented");
}

Result<std::vector<TableIdentifier>> RestCatalog::ListTables(const Namespace& ns) const {
  return NotImplemented("Not implemented");
}

Result<std::unique_ptr<Table>> RestCatalog::CreateTable(
    const TableIdentifier& identifier, const Schema& schema, const PartitionSpec& spec,
    const std::string& location,
    const std::unordered_map<std::string, std::string>& properties) {
  return NotImplemented("Not implemented");
}

Result<std::unique_ptr<Table>> RestCatalog::UpdateTable(
    const TableIdentifier& identifier,
    const std::vector<std::unique_ptr<UpdateRequirement>>& requirements,
    const std::vector<std::unique_ptr<MetadataUpdate>>& updates) {
  return NotImplemented("Not implemented");
}

Result<std::shared_ptr<Transaction>> RestCatalog::StageCreateTable(
    const TableIdentifier& identifier, const Schema& schema, const PartitionSpec& spec,
    const std::string& location,
    const std::unordered_map<std::string, std::string>& properties) {
  return NotImplemented("Not implemented");
}

Status RestCatalog::DropTable(const TableIdentifier& identifier, bool purge) {
  return NotImplemented("Not implemented");
}

Result<bool> RestCatalog::TableExists(const TableIdentifier& identifier) const {
  return NotImplemented("Not implemented");
}

Result<std::unique_ptr<Table>> RestCatalog::LoadTable(const TableIdentifier& identifier) {
  return NotImplemented("Not implemented");
}

Result<std::shared_ptr<Table>> RestCatalog::RegisterTable(
    const TableIdentifier& identifier, const std::string& metadata_file_location) {
  return NotImplemented("Not implemented");
}

std::unique_ptr<RestCatalog::TableBuilder> RestCatalog::BuildTable(
    const TableIdentifier& identifier, const Schema& schema) const {
  return nullptr;
}

// iceberg::Status RestCatalog::UpdateConfig() {
//     //... 实现从 /v1/config 获取配置并更新 this->config_.props 的逻辑
//     // 这是一个 GET 请求，需要定义一个 CatalogConfig DTO
//     return {};
// }

// iceberg::Result<std::vector<NamespaceIdent>> RestCatalog::ListNamespaces(const
// std::optional<NamespaceIdent>& parent) {
//     cpr::Parameters params;
//     if (parent) {
//         params.Add({"parent", parent->encode_in_url()});
//     }

//     auto result = client_->Get<model::ListNamespacesResponse, model::ApiErrorResponse,
//     200>(
//         config_.namespaces_endpoint(), params);

//     if (!result) {
//         return std::unexpected(result.error());
//     }

//     // 将 DTO 转换为业务对象
//     std::vector<NamespaceIdent> final_namespaces;
//     for (const auto& ns_vec : result->namespaces) {
//         // 假设 NamespaceIdent 有一个从 std::vector<string> 构造的工厂或构造函数
//         final_namespaces.push_back(NamespaceIdent::from_vec(ns_vec));
//     }
//     return final_namespaces;
// }

//... 其他方法的实现...

}  // namespace iceberg::rest
