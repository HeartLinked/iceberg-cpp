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

#include "iceberg/catalog/rest/json_internal.h"

#include <format>
#include <string>
#include <unordered_map>
#include <vector>

#include <nlohmann/json.hpp>

#include "iceberg/json_internal.h"
#include "iceberg/partition_spec.h"
#include "iceberg/result.h"
#include "iceberg/schema.h"
#include "iceberg/sort_order.h"
#include "iceberg/table_identifier.h"
#include "iceberg/table_metadata.h"

namespace iceberg::rest {

namespace {

// REST API JSON field constants
constexpr std::string_view kNamespaces = "namespaces";
constexpr std::string_view kNextPageToken = "next-page-token";
constexpr std::string_view kRemovals = "removals";
constexpr std::string_view kUpdates = "updates";
constexpr std::string_view kUpdated = "updated";
constexpr std::string_view kRemoved = "removed";
constexpr std::string_view kMissing = "missing";
constexpr std::string_view kIdentifiers = "identifiers";
constexpr std::string_view kSource = "source";
constexpr std::string_view kDestination = "destination";
constexpr std::string_view kMetadataLocation = "metadata-location";
constexpr std::string_view kMetadata = "metadata";
constexpr std::string_view kConfig = "config";
constexpr std::string_view kName = "name";
constexpr std::string_view kLocation = "location";
constexpr std::string_view kSchema = "schema";
constexpr std::string_view kPartitionSpec = "partition-spec";
constexpr std::string_view kWriteOrder = "write-order";
constexpr std::string_view kStageCreate = "stage-create";
constexpr std::string_view kProperties = "properties";
constexpr std::string_view kOverwrite = "overwrite";
constexpr std::string_view kNamespace = "namespace";

/// Helper function to convert TableIdentifier to JSON
nlohmann::json TableIdentifierToJson(const TableIdentifier& identifier) {
  nlohmann::json json;
  json[kNamespace] = identifier.ns.levels;
  json[kName] = identifier.name;
  return json;
}

/// Helper function to parse TableIdentifier from JSON
Result<TableIdentifier> TableIdentifierFromJson(const nlohmann::json& json) {
  try {
    TableIdentifier identifier;

    if (json.contains(kNamespace)) {
      identifier.ns.levels = json[kNamespace].get<std::vector<std::string>>();
    }

    if (!json.contains(kName)) {
      return std::unexpected(InvalidArgument("TableIdentifier missing 'name' field"));
    }
    identifier.name = json[kName].get<std::string>();

    return identifier;
  } catch (const std::exception& e) {
    return std::unexpected(
        InvalidArgument(std::format("Failed to parse TableIdentifier: {}", e.what())));
  }
}

}  // namespace

nlohmann::json ToJson(const ListNamespaceResponse& response) {
  nlohmann::json json;
  json[kNamespaces] = response.namespaces;

  return json;
}

Result<ListNamespaceResponse> ListNamespaceResponseFromJson(const nlohmann::json& json) {
  try {
    ListNamespaceResponse response;

    if (!json.contains(kNamespaces)) {
      return std::unexpected(
          InvalidArgument("ListNamespaceResponse missing 'namespaces' field"));
    }
    response.namespaces = json[kNamespaces].get<std::vector<std::vector<std::string>>>();

    if (json.contains(kNextPageToken)) {
      response.next_page_token = json[kNextPageToken].get<std::string>();
    }

    return response;
  } catch (const std::exception& e) {
    return std::unexpected(InvalidArgument(
        std::format("Failed to parse ListNamespaceResponse: {}", e.what())));
  }
}

nlohmann::json ToJson(const UpdateNamespacePropsRequest& request) {
  nlohmann::json json;

  if (request.removals.has_value()) {
    json[kRemovals] = request.removals.value();
  }

  if (request.updates.has_value()) {
    json[kUpdates] = request.updates.value();
  }

  return json;
}

Result<UpdateNamespacePropsRequest> UpdateNamespacePropsRequestFromJson(
    const nlohmann::json& json) {
  try {
    UpdateNamespacePropsRequest request;

    if (json.contains(kRemovals)) {
      request.removals = json[kRemovals].get<std::vector<std::string>>();
    }

    if (json.contains(kUpdates)) {
      request.updates =
          json[kUpdates].get<std::unordered_map<std::string, std::string>>();
    }

    return request;
  } catch (const std::exception& e) {
    return std::unexpected(Status::InvalidArgument(
        std::format("Failed to parse UpdateNamespacePropsRequest: {}", e.what())));
  }
}

nlohmann::json ToJson(const UpdateNamespacePropsResponse& response) {
  nlohmann::json json;
  json[kUpdated] = response.updated;
  json[kRemoved] = response.removed;

  if (response.missing.has_value()) {
    json[kMissing] = response.missing.value();
  }

  return json;
}

Result<UpdateNamespacePropsResponse> UpdateNamespacePropsResponseFromJson(
    const nlohmann::json& json) {
  try {
    UpdateNamespacePropsResponse response;

    if (!json.contains(kUpdated)) {
      return std::unexpected(Status::InvalidArgument(
          "UpdateNamespacePropsResponse missing 'updated' field"));
    }
    response.updated = json[kUpdated].get<std::vector<std::string>>();

    if (!json.contains(kRemoved)) {
      return std::unexpected(Status::InvalidArgument(
          "UpdateNamespacePropsResponse missing 'removed' field"));
    }
    response.removed = json[kRemoved].get<std::vector<std::string>>();

    if (json.contains(kMissing)) {
      response.missing = json[kMissing].get<std::vector<std::string>>();
    }

    return response;
  } catch (const std::exception& e) {
    return std::unexpected(Status::InvalidArgument(
        std::format("Failed to parse UpdateNamespacePropsResponse: {}", e.what())));
  }
}

nlohmann::json ToJson(const ListTableResponse& response) {
  nlohmann::json json;

  nlohmann::json identifiers_json = nlohmann::json::array();
  for (const auto& identifier : response.identifiers) {
    identifiers_json.push_back(TableIdentifierToJson(identifier));
  }
  json[kIdentifiers] = identifiers_json;

  if (response.next_page_token.has_value()) {
    json[kNextPageToken] = response.next_page_token.value();
  }

  return json;
}

Result<ListTableResponse> ListTableResponseFromJson(const nlohmann::json& json) {
  try {
    ListTableResponse response;

    if (!json.contains(kIdentifiers)) {
      return std::unexpected(
          Status::InvalidArgument("ListTableResponse missing 'identifiers' field"));
    }

    for (const auto& id_json : json[kIdentifiers]) {
      auto id_result = TableIdentifierFromJson(id_json);
      if (!id_result.has_value()) {
        return std::unexpected(id_result.error());
      }
      response.identifiers.push_back(id_result.value());
    }

    if (json.contains(kNextPageToken)) {
      response.next_page_token = json[kNextPageToken].get<std::string>();
    }

    return response;
  } catch (const std::exception& e) {
    return std::unexpected(Status::InvalidArgument(
        std::format("Failed to parse ListTableResponse: {}", e.what())));
  }
}

nlohmann::json ToJson(const RenameTableRequest& request) {
  nlohmann::json json;
  json[kSource] = TableIdentifierToJson(request.source);
  json[kDestination] = TableIdentifierToJson(request.destination);
  return json;
}

Result<RenameTableRequest> RenameTableRequestFromJson(const nlohmann::json& json) {
  try {
    RenameTableRequest request;

    if (!json.contains(kSource)) {
      return std::unexpected(
          Status::InvalidArgument("RenameTableRequest missing 'source' field"));
    }
    auto source_result = TableIdentifierFromJson(json[kSource]);
    if (!source_result.has_value()) {
      return std::unexpected(source_result.error());
    }
    request.source = source_result.value();

    if (!json.contains(kDestination)) {
      return std::unexpected(
          Status::InvalidArgument("RenameTableRequest missing 'destination' field"));
    }
    auto dest_result = TableIdentifierFromJson(json[kDestination]);
    if (!dest_result.has_value()) {
      return std::unexpected(dest_result.error());
    }
    request.destination = dest_result.value();

    return request;
  } catch (const std::exception& e) {
    return std::unexpected(Status::InvalidArgument(
        std::format("Failed to parse RenameTableRequest: {}", e.what())));
  }
}

nlohmann::json ToJson(const LoadTableResponse& response) {
  nlohmann::json json;

  if (response.metadata_location.has_value()) {
    json[kMetadataLocation] = response.metadata_location.value();
  }

  json[kMetadata] = iceberg::ToJson(response.metadata);

  if (response.config.has_value()) {
    json[kConfig] = response.config.value();
  }

  return json;
}

Result<LoadTableResponse> LoadTableResponseFromJson(const nlohmann::json& json) {
  try {
    LoadTableResponse response;

    if (json.contains(kMetadataLocation)) {
      response.metadata_location = json[kMetadataLocation].get<std::string>();
    }

    if (!json.contains(kMetadata)) {
      return std::unexpected(
          Status::InvalidArgument("LoadTableResponse missing 'metadata' field"));
    }
    auto metadata_result = iceberg::TableMetadataFromJson(json[kMetadata]);
    if (!metadata_result.has_value()) {
      return std::unexpected(metadata_result.error());
    }
    response.metadata = *metadata_result.value();

    if (json.contains(kConfig)) {
      response.config = json[kConfig].get<std::unordered_map<std::string, std::string>>();
    }

    return response;
  } catch (const std::exception& e) {
    return std::unexpected(Status::InvalidArgument(
        std::format("Failed to parse LoadTableResponse: {}", e.what())));
  }
}

nlohmann::json ToJson(const CreateTableRequest& request) {
  nlohmann::json json;
  json[kName] = request.name;

  if (request.location.has_value()) {
    json[kLocation] = request.location.value();
  }

  json[kSchema] = iceberg::ToJson(request.schema);

  if (request.partition_spec.has_value()) {
    json[kPartitionSpec] = iceberg::ToJson(request.partition_spec.value());
  }

  if (request.write_order.has_value()) {
    json[kWriteOrder] = iceberg::ToJson(request.write_order.value());
  }

  if (request.stage_create.has_value()) {
    json[kStageCreate] = request.stage_create.value();
  }

  if (request.properties.has_value()) {
    json[kProperties] = request.properties.value();
  }

  return json;
}

Result<CreateTableRequest> CreateTableRequestFromJson(const nlohmann::json& json) {
  try {
    CreateTableRequest request;

    if (!json.contains(kName)) {
      return std::unexpected(
          Status::InvalidArgument("CreateTableRequest missing 'name' field"));
    }
    request.name = json[kName].get<std::string>();

    if (json.contains(kLocation)) {
      request.location = json[kLocation].get<std::string>();
    }

    if (!json.contains(kSchema)) {
      return std::unexpected(
          Status::InvalidArgument("CreateTableRequest missing 'schema' field"));
    }
    auto schema_result = iceberg::SchemaFromJson(json[kSchema]);
    if (!schema_result.has_value()) {
      return std::unexpected(schema_result.error());
    }
    request.schema = *schema_result.value();

    if (json.contains(kPartitionSpec)) {
      auto spec_result = iceberg::PartitionSpecFromJson(
          std::make_shared<Schema>(request.schema), json[kPartitionSpec]);
      if (!spec_result.has_value()) {
        return std::unexpected(spec_result.error());
      }
      request.partition_spec = *spec_result.value();
    }

    if (json.contains(kWriteOrder)) {
      auto order_result = iceberg::SortOrderFromJson(json[kWriteOrder]);
      if (!order_result.has_value()) {
        return std::unexpected(order_result.error());
      }
      request.write_order = *order_result.value();
    }

    if (json.contains(kStageCreate)) {
      request.stage_create = json[kStageCreate].get<bool>();
    }

    if (json.contains(kProperties)) {
      request.properties =
          json[kProperties].get<std::unordered_map<std::string, std::string>>();
    }

    return request;
  } catch (const std::exception& e) {
    return std::unexpected(Status::InvalidArgument(
        std::format("Failed to parse CreateTableRequest: {}", e.what())));
  }
}

nlohmann::json ToJson(const RegisterTableRequest& request) {
  nlohmann::json json;
  json[kName] = request.name;
  json[kMetadataLocation] = request.metadata_location;

  if (request.overwrite.has_value()) {
    json[kOverwrite] = request.overwrite.value();
  }

  return json;
}

Result<RegisterTableRequest> RegisterTableRequestFromJson(const nlohmann::json& json) {
  try {
    RegisterTableRequest request;

    if (!json.contains(kName)) {
      return std::unexpected(
          Status::InvalidArgument("RegisterTableRequest missing 'name' field"));
    }
    request.name = json[kName].get<std::string>();

    if (!json.contains(kMetadataLocation)) {
      return std::unexpected(Status::InvalidArgument(
          "RegisterTableRequest missing 'metadata-location' field"));
    }
    request.metadata_location = json[kMetadataLocation].get<std::string>();

    if (json.contains(kOverwrite)) {
      request.overwrite = json[kOverwrite].get<bool>();
    }

    return request;
  } catch (const std::exception& e) {
    return std::unexpected(Status::InvalidArgument(
        std::format("Failed to parse RegisterTableRequest: {}", e.what())));
  }
}

}  // namespace iceberg::rest
