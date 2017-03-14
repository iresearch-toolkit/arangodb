//////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2017 EMC Corporation
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
/// Copyright holder is EMC Corporation
///
/// @author Andrey Abramov
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////

#ifndef ARANGOD_IRESEARCH__REST_VIEW_HANDLER_H
#define ARANGOD_IRESEARCH__REST_VIEW_HANDLER_H 1

#include "RestHandler/RestBaseHandler.h"

#include <functional>

struct TRI_vocbase_t;

namespace arangodb {

class VocbaseContext;
class StringRef;

////////////////////////////////////////////////////////////////////////////////
/// @brief IResearch view request handler
////////////////////////////////////////////////////////////////////////////////

class RestViewHandler : public arangodb::RestBaseHandler {
 public:
  typedef std::function<bool( // return code
    StringRef const& type,    // view type
    VPackSlice params,        // view parameters
    TRI_vocbase_t* vocbase
  )> ViewFactory;

  static std::string const VIEW_PATH;

  RestViewHandler(
    GeneralRequest* request,
    GeneralResponse* response,
    const ViewFactory* viewFactory
  );

  virtual char const* name() const override final { return "RestViewHandler"; }
  bool isDirect() const override final { return true; }
  RestStatus execute() override final;

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

  //////////////////////////////////////////////////////////////////////////////
  /// @brief view factory
  //////////////////////////////////////////////////////////////////////////////

  ViewFactory _viewFactory;

  //////////////////////////////////////////////////////////////////////////////
  /// @brief request context
  //////////////////////////////////////////////////////////////////////////////

  VocbaseContext* _context;

  //////////////////////////////////////////////////////////////////////////////
  /// @brief the vocbase
  //////////////////////////////////////////////////////////////////////////////

  TRI_vocbase_t* _vocbase;
};

}

#endif

