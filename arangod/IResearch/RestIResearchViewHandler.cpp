//////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2016 ArangoDB GmbH, Cologne, Germany
///
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
///
///     http://www.apache.org/licenses/LICENSE-2.0
///
/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// See the License for the specific language governing permissions and
/// limitations under the License.
///
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
///
/// @author
////////////////////////////////////////////////////////////////////////////////

#include "RestIResearchViewHandler.h"

#include <velocypack/Builder.h>
#include <velocypack/velocypack-aliases.h>

#include "Rest/HttpRequest.h"
#include "Rest/Version.h"

using namespace arangodb;
using namespace arangodb::basics;
using namespace arangodb::rest;

std::string const RestIResearchViewHandler::IRESEARCH_VIEW_PATH = "/_api/view";

RestIResearchViewHandler::RestIResearchViewHandler(
    GeneralRequest* request,
    GeneralResponse* response)
  : RestBaseHandler(request, response) {
}

RestStatus RestIResearchViewHandler::execute() {
  // extract the sub-request type
  auto const type = _request->requestType();

  // execute one of the CRUD methods
  switch (type) {
    case rest::RequestType::DELETE_REQ:
      handleDelete();
      break;
    case rest::RequestType::GET:
      handleRead();
      break;
    case rest::RequestType::POST:
      handleCreate();
      break;
    case rest::RequestType::PUT:
      handleUpdate();
      break;
    default: {
      generateError(
        rest::ResponseCode::NOT_IMPLEMENTED, TRI_ERROR_NOT_IMPLEMENTED,
        "'" + IRESEARCH_VIEW_PATH + "' not implemented"
      );
      break;
    }
  }

  // this handler is done
  return RestStatus::DONE;
}

bool RestIResearchViewHandler::handleDelete() {
  if (1 != _request->suffixes().size()) {
    generateError(
      rest::ResponseCode::BAD,
      TRI_ERROR_HTTP_BAD_PARAMETER,
      "expecting DELETE "
        + IRESEARCH_VIEW_PATH + "/<view-name>"
        + " or " + IRESEARCH_VIEW_PATH + "/<view-name>?collection=<collection-name>"
    );
    return false;
  }

  bool isLink;
  std::string const collectionName = _request->value("collection", isLink);

  return isLink ? deleteViewLink(collectionName) : deleteView();
}

bool RestIResearchViewHandler::handleRead() {
  size_t const len = _request->suffixes().size();

  switch (len) {
    case 0:
      return readAllViews();
    case 1: {
      bool isLink;
      std::string const collectionName = _request->value("collection", isLink);

      return isLink ? readViewLink(collectionName) : readView();
    }
  }

  generateError(
    rest::ResponseCode::BAD,
    TRI_ERROR_HTTP_SUPERFLUOUS_SUFFICES,
    "expecting GET " + IRESEARCH_VIEW_PATH
        + " or " + IRESEARCH_VIEW_PATH + "/<view-name>" +
        + " or " + IRESEARCH_VIEW_PATH + "/<view-name>?collection=<collection-name>"
  );

  return false;
}

bool RestIResearchViewHandler::handleCreate() {
  size_t const len = _request->suffixes().size();

  switch (len) {
    case 0:
      return createView();
    case 1: {
      return createViewLink();
    }
  }

  generateError(
    rest::ResponseCode::BAD,
    TRI_ERROR_HTTP_SUPERFLUOUS_SUFFICES,
    "expecting POST " + IRESEARCH_VIEW_PATH
        + " or " + IRESEARCH_VIEW_PATH + "/<view-name>"
  );

  return false;
}

bool RestIResearchViewHandler::handleUpdate() {
  if (1 != _request->suffixes().size()) {
    generateError(
      rest::ResponseCode::BAD,
      TRI_ERROR_HTTP_BAD_PARAMETER,
      "expecting PUT"
        + IRESEARCH_VIEW_PATH + "/<view-name>"
        + " or " + IRESEARCH_VIEW_PATH + "/<view-name>?collection=<collection-name>"
    );
    return false;
  }

  bool isLink;
  std::string const collectionName = _request->value("collection", isLink);

  return isLink ? updateViewLink(collectionName) : updateView();
}

bool RestIResearchViewHandler::deleteView() {
  // decode URL suffixes
  auto const suffixes = _request->decodedSuffixes();
  TRI_ASSERT(1 == suffixes.size());

  std::string const& viewName = suffixes[0];

  VPackBuilder result;
  result.add(VPackValue(VPackValueType::Object));
  result.add("server", VPackValue("arango"));
  result.add("version", VPackValue(ARANGODB_VERSION));
  result.add("name", VPackValue(viewName));
  result.close();

  generateResult(rest::ResponseCode::OK, result.slice());

  return true;
}

bool RestIResearchViewHandler::deleteViewLink(const std::string& collectionName) {
  // decode URL suffixes
  auto const suffixes = _request->decodedSuffixes();
  TRI_ASSERT(1 == suffixes.size());

  std::string const& viewName = suffixes[0];

  VPackBuilder result;
  result.add(VPackValue(VPackValueType::Object));
  result.add("server", VPackValue("arango"));
  result.add("version", VPackValue(ARANGODB_VERSION));
  result.add("name", VPackValue(viewName));
  result.add("action", VPackValue(__FUNCTION__));
  result.close();

  generateResult(rest::ResponseCode::OK, result.slice());

  return true;
}

bool RestIResearchViewHandler::readView() {
  // decode URL suffixes
  auto const suffixes = _request->decodedSuffixes();
  TRI_ASSERT(1 == suffixes.size());

  std::string const& viewName = suffixes[0];

  VPackBuilder result;
  result.add(VPackValue(VPackValueType::Object));
  result.add("server", VPackValue("arango"));
  result.add("version", VPackValue(ARANGODB_VERSION));
  result.add("name", VPackValue(viewName));
  result.add("action", VPackValue(__FUNCTION__));
  result.close();

  generateResult(rest::ResponseCode::OK, result.slice());

  return true;
}

bool RestIResearchViewHandler::readAllViews() {
  TRI_ASSERT(_request->suffixes().empty());

  VPackBuilder result;
  result.add(VPackValue(VPackValueType::Object));
  result.add("server", VPackValue("arango"));
  result.add("version", VPackValue(ARANGODB_VERSION));
  result.add("action", VPackValue(__FUNCTION__));
  result.close();

  generateResult(rest::ResponseCode::OK, result.slice());

  return true;
}

bool RestIResearchViewHandler::readViewLink(std::string const& collectionName) {
  // decode URL suffixes
  auto const suffixes = _request->decodedSuffixes();
  TRI_ASSERT(1 == suffixes.size());

  std::string const& viewName = suffixes[0];

  VPackBuilder result;
  result.add(VPackValue(VPackValueType::Object));
  result.add("server", VPackValue("arango"));
  result.add("version", VPackValue(ARANGODB_VERSION));
  result.add("name", VPackValue(viewName));
  result.add("collectionName", VPackValue(viewName));
  result.add("action", VPackValue(__FUNCTION__));
  result.close();

  generateResult(rest::ResponseCode::OK, result.slice());

  return true;
}

bool RestIResearchViewHandler::createView() {
  TRI_ASSERT(_request->suffixes().empty());

  bool parseSuccess = true;
  // copy default options
  VPackOptions options = VPackOptions::Defaults;
  options.checkAttributeUniqueness = true;
  auto const parsedBody = parseVelocyPackBody(&options, parseSuccess);

  if (!parseSuccess) {
    return false;
  }

  VPackSlice body = parsedBody->slice();
  VPackSlice viewName = body.get("name");

  VPackBuilder result;
  result.add(VPackValue(VPackValueType::Object));
  result.add("server", VPackValue("arango"));
  result.add("version", VPackValue(ARANGODB_VERSION));
  result.add("action", VPackValue(__FUNCTION__));
  result.close();

  generateResult(rest::ResponseCode::OK, result.slice());

  return true;
}

bool RestIResearchViewHandler::createViewLink() {
  // decode URL suffixes
  auto const suffixes = _request->decodedSuffixes();
  TRI_ASSERT(1 == suffixes.size());

  bool parseSuccess = true;
  // copy default options
  VPackOptions options = VPackOptions::Defaults;
  options.checkAttributeUniqueness = true;
  auto const parsedBody = parseVelocyPackBody(&options, parseSuccess);

  if (!parseSuccess) {
    return false;
  }

  auto const& viewName = suffixes[0];
  VPackSlice body = parsedBody->slice();
  VPackSlice collectionName = body.get("collection");

  VPackBuilder result;
  result.add(VPackValue(VPackValueType::Object));
  result.add("server", VPackValue("arango"));
  result.add("version", VPackValue(ARANGODB_VERSION));
  result.add("name", VPackValue(viewName));
  result.add("action", VPackValue(__FUNCTION__));
  result.close();

  generateResult(rest::ResponseCode::OK, result.slice());

  return true;
}

bool RestIResearchViewHandler::updateView() {
  // decode URL suffixes
  auto const suffixes = _request->decodedSuffixes();
  TRI_ASSERT(1 == suffixes.size());

  bool parseSuccess = true;
  // copy default options
  VPackOptions options = VPackOptions::Defaults;
  options.checkAttributeUniqueness = true;
  auto const parsedBody = parseVelocyPackBody(&options, parseSuccess);

  if (!parseSuccess) {
    return false;
  }

  std::string const& viewName = suffixes[0];

  VPackBuilder result;
  result.add(VPackValue(VPackValueType::Object));
  result.add("server", VPackValue("arango"));
  result.add("version", VPackValue(ARANGODB_VERSION));
  result.add("name", VPackValue(viewName));
  result.add("action", VPackValue(__FUNCTION__));
  result.close();

  generateResult(rest::ResponseCode::OK, result.slice());

  return true;
}

bool RestIResearchViewHandler::updateViewLink(std::string const& collectionName) {
  // decode URL suffixes
  auto const suffixes = _request->decodedSuffixes();
  TRI_ASSERT(1 == suffixes.size());

  bool parseSuccess = true;
  // copy default options
  VPackOptions options = VPackOptions::Defaults;
  options.checkAttributeUniqueness = true;
  auto const parsedBody = parseVelocyPackBody(&options, parseSuccess);

  if (!parseSuccess) {
    return false;
  }

  std::string const& viewName = suffixes[0];
  VPackSlice body = parsedBody->slice();

  VPackBuilder result;
  result.add(VPackValue(VPackValueType::Object));
  result.add("server", VPackValue("arango"));
  result.add("version", VPackValue(ARANGODB_VERSION));
  result.add("name", VPackValue(viewName));
  result.add("action", VPackValue(__FUNCTION__));
  result.add("collectionName", VPackValue(collectionName));
  result.close();

  generateResult(rest::ResponseCode::OK, result.slice());

  return true;
}
