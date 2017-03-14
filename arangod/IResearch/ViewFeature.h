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
/// @author Vasily Nabatchikov
////////////////////////////////////////////////////////////////////////////////

#ifndef ARANGOD_IRESEARCH__VIEW_FEATURE_H
#define ARANGOD_IRESEARCH__VIEW_FEATURE_H 1

#include "ApplicationFeatures/ApplicationFeature.h"
#include "Basics/StringRef.h"

struct TRI_vocbase_t;

namespace arangodb {

class ViewFeature final : public application_features::ApplicationFeature {
 public:
  typedef std::function<bool( // return code
    VPackSlice params,        // view parameters
    TRI_vocbase_t* vocbase
  )> ViewFactory;

  static void registerFactory(
    StringRef const& type,
    ViewFactory const& factory
  );

  ViewFeature(application_features::ApplicationServer* server);

 public:
  void collectOptions(std::shared_ptr<options::ProgramOptions>) override final;
  void validateOptions(std::shared_ptr<options::ProgramOptions>) override final;
  void prepare() override final;
  void start() override final;
};
}

#endif
