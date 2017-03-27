
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

#include "IResearchFeature.h"
#include "IResearchView.h"

#include "RestServer/ViewTypesFeature.h"

#include "Logger/Logger.h"

#include "formats/formats.hpp"
#include "analysis/analyzers.hpp"

using namespace arangodb::iresearch;
using namespace arangodb::options;

IResearchFeature::IResearchFeature(arangodb::application_features::ApplicationServer* server)
    : ApplicationFeature(server, "IResearch") {
  setOptional(true);
  requiresElevatedPrivileges(false);
  startsAfter("ViewTypes");
  startsAfter("Logger");
}

void IResearchFeature::collectOptions(std::shared_ptr<ProgramOptions> options) {
}

void IResearchFeature::validateOptions(std::shared_ptr<ProgramOptions> options) {
}

void IResearchFeature::prepare() {
  // load all known codecs
  ::iresearch::formats::init();

  // load all known analyzers
  ::iresearch::analysis::analyzers::init();

  // register 'iresearch' view
  ViewTypesFeature::registerViewImplementation(
     IResearchView::type(),
     IResearchView::make
   );
}

void IResearchFeature::start() {
}
