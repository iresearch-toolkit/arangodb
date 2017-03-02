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

#ifndef ARANGOD_REST_HANDLER_IRESEARCH_VIEW_HANDLER_H
#define ARANGOD_REST_HANDLER_IRESEARCH_VIEW_HANDLER_H 1

#include "RestHandler/RestBaseHandler.h"

namespace arangodb {

////////////////////////////////////////////////////////////////////////////////
/// @brief IResearch view request handler
////////////////////////////////////////////////////////////////////////////////

class RestIResearchViewHandler : public arangodb::RestBaseHandler {
 public:
  static std::string const IRESEARCH_VIEW_PATH;

  RestIResearchViewHandler(GeneralRequest*, GeneralResponse*);

  virtual char const* name() const override { return "RestIResearchViewHandler"; }
  bool isDirect() const override { return true; }
  RestStatus execute() override;

 private:
  bool handleDelete();
  bool deleteView();
  bool deleteViewLink(std::string const& collectionName);

  bool handleRead();
  bool readAllViews();
  bool readView();
  bool readViewLink(std::string const& collectionName);

  bool handleCreate();
  bool createView();
  bool createViewLink();

  bool handleUpdate();
  bool updateView();
  bool updateViewLink(std::string const& collectionName);
};

}

#endif

