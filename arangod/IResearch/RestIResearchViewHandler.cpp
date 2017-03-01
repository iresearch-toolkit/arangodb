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

const std::string RestIResearchViewHandler::IRESEARCH_VIEW_PATH = "/_api/view";

RestIResearchViewHandler::RestIResearchViewHandler(GeneralRequest* request,
                                 GeneralResponse* response)
  : RestBaseHandler(request, response) {
}

RestStatus RestIResearchViewHandler::execute() {
  // extract the sub-request type
  auto const type = _request->requestType();

  // execute one of the CRUD methods
  switch (type) {
    case rest::RequestType::DELETE_REQ:
      deleteView();
      break;
    case rest::RequestType::GET:
      readView();
      break;
    case rest::RequestType::POST:
      createView();
      break;
    case rest::RequestType::PUT:
      replaceView();
      break;
    case rest::RequestType::PATCH:
      updateView();
      break;
    default: { generateNotImplemented("ILLEGAL " + IRESEARCH_VIEW_PATH); }
  }

  // this handler is done
  return RestStatus::DONE;
}

void RestIResearchViewHandler::deleteView() {
  LOG_TOPIC(INFO, arangodb::Logger::FIXME) << __FUNCTION__;
}

void RestIResearchViewHandler::readView() {
  LOG_TOPIC(INFO, arangodb::Logger::FIXME) << __FUNCTION__;
}

void RestIResearchViewHandler::createView() {
  LOG_TOPIC(INFO, arangodb::Logger::FIXME) << __FUNCTION__;
}

void RestIResearchViewHandler::replaceView() {
  LOG_TOPIC(INFO, arangodb::Logger::FIXME) << __FUNCTION__;
}

void RestIResearchViewHandler::updateView() {
  LOG_TOPIC(INFO, arangodb::Logger::FIXME) << __FUNCTION__;
}

void RestIResearchViewHandler::generateNotImplemented(std::string const& path) {
  generateError(rest::ResponseCode::NOT_IMPLEMENTED, TRI_ERROR_NOT_IMPLEMENTED,
                "'" + path + "' not implemented");
}
