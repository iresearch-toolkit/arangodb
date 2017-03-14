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

#include "search/scorers.hpp"
#include "utils/locale_utils.hpp"

#include "VelocyPackHelper.h"
#include "Basics/StringUtils.h"
#include "velocypack/Builder.h"
#include "velocypack/Iterator.h"

#include "IResearchViewMeta.h"

NS_LOCAL

////////////////////////////////////////////////////////////////////////////////
/// @brief functrs for initializing scorers
////////////////////////////////////////////////////////////////////////////////
struct ScorerMeta {
  // @return success
  typedef std::function<bool(irs::flags& flags)> fnFeatures_f;
  typedef irs::iql::order_function fnScorer_t;
  typedef fnScorer_t::contextual_function_t fnScorer_f;
  typedef fnScorer_t::contextual_function_args_t fnScoreArgs_t;
  bool _isDefault;
  irs::flags _features;
  fnScorer_t const& _scorer;
  ScorerMeta(
    irs::flags const& features, fnScorer_t const& scorer, bool isDefault = false
  ) : _isDefault(isDefault), _features(features), _scorer(scorer) {}
};

std::unordered_multimap<std::string, ScorerMeta> const& allKnownScorers() {
  static const struct AllScorers {
    std::unordered_multimap<std::string, ScorerMeta> _scorers;
    AllScorers() {
      auto visitor = [this](
        std::string const& name,
        iresearch::flags const& features,
        iresearch::iql::order_function const& builder,
        bool isDefault
        )->bool {
        _scorers.emplace(name, ScorerMeta(features, builder, isDefault));
        return true;
      };
      static ScorerMeta::fnScorer_f FIXME_SCORER = [](
        iresearch::order& order,
        const std::locale&,
        void* cookie,
        bool ascending,
        const ScorerMeta::fnScoreArgs_t& args
        )->bool {
        return false;
      };

      irs::scorers::visit([this](const irs::string_ref& name)->bool{
        _scorers.emplace(name, ScorerMeta(irs::flags::empty_instance(), FIXME_SCORER));
        return true;
      });
      //iResearchDocumentAdapter::visitScorers(visitor); FIXME TODO
    }
  } KNOWN_SCORERS;

  return KNOWN_SCORERS._scorers;
}

bool initCommitBaseMeta(
  arangodb::iresearch::IResearchViewMeta::CommitBaseMeta& meta,
  arangodb::velocypack::Slice const& slice,
  std::string& errorField,
  arangodb::iresearch::IResearchViewMeta::CommitBaseMeta const& defaults
) noexcept {
  bool tmpSeen;

  {
    // optional size_t
    static const std::string fieldName("cleanupIntervalStep");

    if (!arangodb::iresearch::getNumber(meta._cleanupIntervalStep, slice, fieldName, tmpSeen, meta._cleanupIntervalStep)) {
      errorField = fieldName;

      return false;
    }
  }

  {
    // optional enum->{size_t,float} map
    static const std::string fieldName("consolidate");

    if (slice.hasKey(fieldName)) {
      auto field = slice.get(fieldName);

      if (!field.isObject()) {
        errorField = fieldName;

        return false;
      }

      // mark all as unset
      for (size_t i = 0, count = arangodb::iresearch::ConsolidationPolicy::eLast; i < count; ++i) {
        meta._consolidate[i]._intervalStep = 0;
        meta._consolidate[i]._threshold = std::numeric_limits<float>::infinity();
      }

      for (arangodb::velocypack::ObjectIterator itr(field); itr.valid(); ++itr) {
        auto key = itr.key();

        if (!key.isString()) {
          errorField = fieldName + "=>[" + arangodb::basics::StringUtils::itoa(itr.index()) + "]";

          return false;
        }

        static const std::unordered_map<std::string, arangodb::iresearch::ConsolidationPolicy::Type> policies = {
          { "bytes", arangodb::iresearch::ConsolidationPolicy::BYTES },
          { "bytes_accum", arangodb::iresearch::ConsolidationPolicy::BYTES_ACCUM },
          { "count", arangodb::iresearch::ConsolidationPolicy::COUNT },
          { "fill", arangodb::iresearch::ConsolidationPolicy::FILL },
        };

        auto name = key.copyString();
        auto policyItr = policies.find(name);
        auto value = itr.value();

        if (!value.isObject() || policyItr == policies.end()) {
          errorField = fieldName + "=>" + name;

          return false;
        }

        {
          // optional size_t
          static const std::string subFieldName("intervalStep");

          if (!arangodb::iresearch::getNumber(meta._consolidate[policyItr->second]._intervalStep, value, subFieldName, tmpSeen, defaults._consolidate[policyItr->second]._intervalStep)) {
            errorField = fieldName + "=>" + name + "=>" + subFieldName;

            return false;
          }
        }

        {
          // optional float
          static const std::string subFieldName("threshold");

          if (!arangodb::iresearch::getNumber(meta._consolidate[policyItr->second]._threshold, value, subFieldName, tmpSeen, defaults._consolidate[policyItr->second]._threshold) ||
              meta._consolidate[policyItr->second]._threshold < 0. || meta._consolidate[policyItr->second]._threshold > 1.) {
            errorField = fieldName + "=>" + name + "=>" + subFieldName;

            return false;
          }
        }
      }
    }
  }

  return true;
}

bool jsonCommitBaseMeta(
  arangodb::velocypack::Builder& builder,
  arangodb::iresearch::IResearchViewMeta::CommitBaseMeta const& meta
) {
  if (!builder.isOpenObject()) {
    return false;
  }

  builder.add("cleanupIntervalStep", arangodb::velocypack::Value(meta._cleanupIntervalStep));

  struct DefragmentPolicyHash { size_t operator()(arangodb::iresearch::ConsolidationPolicy::Type const& value) const { return value; } }; // for GCC compatibility
  static const std::unordered_map<arangodb::iresearch::ConsolidationPolicy::Type, std::string, DefragmentPolicyHash> policies = {
    { arangodb::iresearch::ConsolidationPolicy::BYTES, "bytes" },
    { arangodb::iresearch::ConsolidationPolicy::BYTES_ACCUM, "bytes_accum" },
    { arangodb::iresearch::ConsolidationPolicy::COUNT, "count" },
    { arangodb::iresearch::ConsolidationPolicy::FILL, "fill" },
  };

  arangodb::velocypack::Builder subBuilder;

  {
    arangodb::velocypack::ObjectBuilder subBuilderWrapper(&subBuilder);

    for (size_t i = 0, count = arangodb::iresearch::ConsolidationPolicy::eLast; i < count; ++i) {
      auto& policy = meta._consolidate[i];

      if (policy._intervalStep) {
        auto itr = policies.find(static_cast<arangodb::iresearch::ConsolidationPolicy::Type>(i));

        if (itr != policies.end()) {
          arangodb::velocypack::Builder policyBuilder;

          {
            arangodb::velocypack::ObjectBuilder policyBuilderWrapper(&policyBuilder);

            policyBuilderWrapper->add("intervalStep", arangodb::velocypack::Value(policy._intervalStep));
            policyBuilderWrapper->add("threshold", arangodb::velocypack::Value(policy._threshold));
          }

          subBuilderWrapper->add(itr->second, policyBuilder.slice());
        }
      }
    }
  }

  builder.add("consolidate", subBuilder.slice());

  return true;
}

NS_END

NS_BEGIN(arangodb)
NS_BEGIN(iresearch)

bool IResearchViewMeta::CommitBaseMeta::operator==(
  CommitBaseMeta const& other
) const noexcept {
  return _cleanupIntervalStep == other._cleanupIntervalStep
    && memcmp(_consolidate, other._consolidate, arangodb::iresearch::ConsolidationPolicy::eLast * sizeof(decltype(_consolidate))) == 0;
}

bool IResearchViewMeta::CommitBulkMeta::operator==(
  CommitBulkMeta const& other
) const noexcept {
  return _commitIntervalBatchSize == other._commitIntervalBatchSize
    && *static_cast<CommitBaseMeta const*>(this) == other;
}

bool IResearchViewMeta::CommitBulkMeta::operator!=(
  CommitBulkMeta const& other
) const noexcept {
  return !(*this == other);
}

bool IResearchViewMeta::CommitItemMeta::operator==(
  CommitItemMeta const& other
) const noexcept {
  return _commitIntervalMsec == other._commitIntervalMsec
    && *static_cast<CommitBaseMeta const*>(this) == other;
}

bool IResearchViewMeta::CommitItemMeta::operator!=(
  CommitItemMeta const& other
  ) const noexcept {
  return !(*this == other);
}

IResearchViewMeta::Mask::Mask(bool mask /*=false*/) noexcept
  : _collections(mask),
    _commitBulk(mask),
    _commitItem(mask),
    _dataPath(mask),
    _iid(mask),
    _locale(mask),
    _name(mask),
    _nestingDelimiter(mask),
    _nestingListOffsetPrefix(mask),
    _nestingListOffsetSuffix(mask),
    _scorers(mask),
    _threadsMaxIdle(mask),
    _threadsMaxTotal(mask) {
}

IResearchViewMeta::IResearchViewMeta()
  : _dataPath(""),
    _iid(0),
    _locale(std::locale::classic()),
    _name(""),
    _nestingDelimiter("."),
    _nestingListOffsetPrefix("["),
    _nestingListOffsetSuffix("]"),
    _threadsMaxIdle(5),
    _threadsMaxTotal(5) {
  _commitBulk._cleanupIntervalStep = 10;
  _commitBulk._commitIntervalBatchSize = 10000;
  _commitItem._cleanupIntervalStep = 10;
  _commitItem._commitIntervalMsec = 60 * 1000;

  for (size_t i = 0, count = arangodb::iresearch::ConsolidationPolicy::eLast; i < count; ++i) {
    _commitBulk._consolidate[i]._intervalStep = 10;
    _commitBulk._consolidate[i]._threshold = 0.85f;
    _commitItem._consolidate[i]._intervalStep = 10;
    _commitItem._consolidate[i]._threshold = 0.85f;
  }

  for (auto& scorer: allKnownScorers()) {
    if (scorer.second._isDefault) {
      _features |= scorer.second._features;
      _scorers.emplace(scorer.first, scorer.second._scorer);
    }
  }
}

IResearchViewMeta::IResearchViewMeta(IResearchViewMeta const& defaults) {
  *this = defaults;
}

IResearchViewMeta::IResearchViewMeta(IResearchViewMeta&& other) noexcept {
  *this = std::move(other);
}

IResearchViewMeta& IResearchViewMeta::operator=(IResearchViewMeta&& other) noexcept {
  if (this != &other) {
    _collections = std::move(other._collections);
    _commitBulk = std::move(other._commitBulk);
    _commitItem = std::move(other._commitItem);
    _dataPath = std::move(other._dataPath);
    _features = std::move(other._features);
    _iid = std::move(other._iid);
    _locale = std::move(other._locale);
    _name = std::move(other._name);
    _nestingDelimiter = std::move(other._nestingDelimiter);
    _nestingListOffsetPrefix = std::move(other._nestingListOffsetPrefix);
    _nestingListOffsetSuffix = std::move(other._nestingListOffsetSuffix);
    _scorers = std::move(other._scorers);
    _threadsMaxIdle = std::move(other._threadsMaxIdle);
    _threadsMaxTotal = std::move(other._threadsMaxTotal);
  }

  return *this;
}

IResearchViewMeta& IResearchViewMeta::operator=(IResearchViewMeta const& other) {
  if (this != &other) {
    _collections = other._collections;
    _commitBulk = other._commitBulk;
    _commitItem = other._commitItem;
    _dataPath = other._dataPath;
    _features = other._features;
    _iid = other._iid;
    _locale = other._locale;
    _name = other._name;
    _nestingDelimiter = other._nestingDelimiter;
    _nestingListOffsetPrefix = other._nestingListOffsetPrefix;
    _nestingListOffsetSuffix = other._nestingListOffsetSuffix;
    _scorers = other._scorers;
    _threadsMaxIdle = other._threadsMaxIdle;
    _threadsMaxTotal = other._threadsMaxTotal;
  }

  return *this;
}

bool IResearchViewMeta::operator==(IResearchViewMeta const& other) const noexcept {
  if (_collections != other._collections) {
    return false; // values do not match
  }

  if (_commitBulk != other._commitBulk) {
    return false; // values do not match
  }

  if (_commitItem != other._commitItem) {
    return false; // values do not match
  }

  if (_dataPath != other._dataPath) {
    return false; // values do not match
  }

  if (_features != other._features) {
    return false; // values do not match
  }

  if (_iid != other._iid) {
    return false; // values do not match
  }

  if (_locale != other._locale) {
    return false; // values do not match
  }

  if (_name != other._name) {
    return false; // values do not match
  }

  if (_nestingDelimiter != other._nestingDelimiter) {
    return false; // values do not match
  }

  if (_nestingListOffsetPrefix != other._nestingListOffsetPrefix) {
    return false; // values do not match
  }

  if (_nestingListOffsetSuffix != other._nestingListOffsetSuffix) {
    return false; // values do not match
  }

  if (_scorers != other._scorers) {
    return false; // values do not match
  }

  if (_threadsMaxIdle != other._threadsMaxIdle) {
    return false; // values do not match
  }

  if (_threadsMaxTotal != other._threadsMaxTotal) {
    return false; // values do not match
  }

  return true;
}

bool IResearchViewMeta::operator!=(
  IResearchViewMeta const& other
  ) const noexcept {
  return !(*this == other);
}

/*static*/ const IResearchViewMeta& IResearchViewMeta::DEFAULT() {
  static const IResearchViewMeta meta;

  return meta;
}

bool IResearchViewMeta::init(
  arangodb::velocypack::Slice const& slice,
  std::string& errorField,
  IResearchViewMeta const& defaults /*= DEFAULT()*/,
  Mask* mask /*= nullptr*/
) noexcept {
  if (!slice.isObject()) {
    return false;
  }

  Mask tmpMask;

  if (!mask) {
    mask = &tmpMask;
  }

  {
    // optional uint64 list
    static const std::string fieldName("collections");

    mask->_collections = slice.hasKey(fieldName);

    if (!mask->_collections) {
      _collections = defaults._collections;
    } else {
      auto field = slice.get(fieldName);

      if (!field.isArray()) {
        errorField = fieldName;

        return false;
      }

      _collections.clear(); // reset to match read values exactly

      for (arangodb::velocypack::ArrayIterator itr(field); itr.valid(); ++itr) {
        decltype(_collections)::key_type value;

        if (!getNumber(value, itr.value())) { // [ <collectionId 1> ... <collectionId N> ]
          errorField = fieldName + "=>[" + arangodb::basics::StringUtils::itoa(itr.index()) + "]";

          return false;
        }

        _collections.emplace(value);
      }
    }
  }

  {
    // optional jSON object
    static const std::string fieldName("commitBulk");

    mask->_commitBulk = slice.hasKey(fieldName);

    if (!mask->_commitBulk) {
      _commitBulk = defaults._commitBulk;
    } else {
      auto field = slice.get(fieldName);

      if (!field.isObject()) {
        errorField = fieldName;

        return false;
      }

      {
        // optional size_t
        static const std::string subFieldName("commitIntervalBatchSize");
        bool tmpBool;

        if (!getNumber(_commitBulk._commitIntervalBatchSize, field, subFieldName, tmpBool, defaults._commitBulk._commitIntervalBatchSize)) {
          errorField = fieldName + "=>" + subFieldName;

          return false;
        }
      }

      std::string errorSubField;

      if (!initCommitBaseMeta(_commitBulk, field, errorSubField, defaults._commitBulk)) {
        errorField = fieldName + "=>" + errorSubField;

        return false;
      }
    }
  }

  {
    // optional jSON object
    static const std::string fieldName("commitItem");

    mask->_commitItem = slice.hasKey(fieldName);

    if (!mask->_commitItem) {
      _commitItem = defaults._commitItem;
    } else {
      auto field = slice.get(fieldName);

      if (!field.isObject()) {
        errorField = fieldName;

        return false;
      }

      {
        // optional size_t
        static const std::string subFieldName("commitIntervalMsec");
        bool tmpBool;

        if (!getNumber(_commitItem._commitIntervalMsec, field, subFieldName, tmpBool, defaults._commitItem._commitIntervalMsec)) {
          errorField = fieldName + "=>" + subFieldName;

          return false;
        }
      }

      std::string errorSubField;

      if (!initCommitBaseMeta(_commitItem, field, errorSubField, defaults._commitItem)) {
        errorField = fieldName + "=>" + errorSubField;

        return false;
      }
    }
  }

  {
    // optional string
    static const std::string fieldName("dataPath");

    if (!getString(_dataPath, slice, fieldName, mask->_dataPath, defaults._dataPath)) {
      errorField = fieldName;

      return false;
    }
  }

  {
    // optional uint64
    static const std::string fieldName("id");

    if (!getNumber(_iid, slice, fieldName, mask->_iid, defaults._iid)) {
      errorField = fieldName;

      return false;
    }
  }

  {
    // optional locale name
    static const std::string fieldName("locale");

    mask->_locale = slice.hasKey(fieldName);

    if (!mask->_locale) {
      _locale = defaults._locale;
    } else {
      auto field = slice.get(fieldName);

      if (!field.isString()) {
        errorField = fieldName;

        return false;
      }

      auto locale = field.copyString();

      // use UTF-8 encoding since that is what JSON objects use
      _locale = std::locale::classic().name() == locale
        ? std::locale::classic()
        : irs::locale_utils::locale(locale, true);
    }
  }

  {
    // required string
    static const std::string fieldName("name");

    mask->_name = slice.hasKey(fieldName);

    if (!mask->_name) {
      errorField = fieldName;

      return false;
    }

    auto field = slice.get(fieldName);

    if (!field.isString()) {
      errorField = fieldName;

      return false;
    }

    _name = field.copyString();
  }

  {
    // optional string
    static const std::string fieldName("nestingDelimiter");

    if (!getString(_nestingDelimiter, slice, fieldName, mask->_nestingDelimiter, defaults._nestingDelimiter)) {
      errorField = fieldName;

      return false;
    }
  }

  {
    // optional string
    static const std::string fieldName("nestingListOffsetPrefix");

    if (!getString(_nestingListOffsetPrefix, slice, fieldName, mask->_nestingListOffsetPrefix, defaults._nestingListOffsetPrefix)) {
      errorField = fieldName;

      return false;
    }
  }

  {
    // optional string
    static const std::string fieldName("nestingListOffsetSuffix");

    if (!getString(_nestingListOffsetSuffix, slice, fieldName, mask->_nestingListOffsetSuffix, defaults._nestingListOffsetSuffix)) {
      errorField = fieldName;

      return false;
    }
  }

  {
    // optional string list
    static const std::string fieldName("scorers");

    mask->_scorers = slice.hasKey(fieldName);
    _features |= defaults._features; // add features from default scorers
    _scorers = defaults._scorers; // always add default scorers

    if (mask->_scorers) {
      auto field = slice.get(fieldName);

      if (!field.isArray()) { // [ <scorerName 1> ... <scorerName N> ]
        errorField = fieldName;

        return false;
      }

      for (arangodb::velocypack::ArrayIterator itr(field); itr.valid(); ++itr) {
        auto entry = itr.value();

        if (!entry.isString()) {
          errorField = fieldName + "=>[" + arangodb::basics::StringUtils::itoa(itr.index()) + "]";

          return false;
        }

        auto name = entry.copyString();

        if (_scorers.find(name) != _scorers.end()) {
          continue; // do not insert duplicates
        }

        auto& knownScorers = allKnownScorers();
        auto knownScorersItr = knownScorers.equal_range(name);

        if (knownScorersItr.first == knownScorersItr.second) {
          errorField = fieldName + "=>" + name;

          return false; // unknown scorer
        }

        for (auto scorerItr = knownScorersItr.first; scorerItr != knownScorersItr.second; ++scorerItr) {
          _features |= scorerItr->second._features;
          _scorers.emplace(scorerItr->first, scorerItr->second._scorer);
        }
      }
    }
  }

  {
    // optional size_t
    static const std::string fieldName("threadsMaxIdle");

    if (!getNumber(_threadsMaxIdle, slice, fieldName, mask->_threadsMaxIdle, defaults._threadsMaxIdle)) {
      errorField = fieldName;

      return false;
    }
  }

  {
    // optional size_t
    static const std::string fieldName("threadsMaxTotal");

    if (!getNumber(_threadsMaxTotal, slice, fieldName, mask->_threadsMaxTotal, defaults._threadsMaxTotal) || !_threadsMaxTotal) {
      errorField = fieldName;

      return false;
    }
  }

  return true;
}

bool IResearchViewMeta::json(
  arangodb::velocypack::Builder& builder,
  IResearchViewMeta const* ignoreEqual /*= nullptr*/,
  Mask const* mask /*= nullptr*/
) const {
  if (!builder.isOpenObject()) {
    return false;
  }

  if ((!ignoreEqual || _collections != ignoreEqual->_collections) && (!mask || mask->_collections)) {
    arangodb::velocypack::Builder subBuilder;

    {
      arangodb::velocypack::ArrayBuilder subBuilderWrapper(&subBuilder);

      for (auto& cid: _collections) {
        subBuilderWrapper->add(arangodb::velocypack::Value(cid));
      }
    }

    builder.add("collections", subBuilder.slice());
  }

  if ((!ignoreEqual || _commitBulk != ignoreEqual->_commitBulk) && (!mask || mask->_commitBulk)) {
    arangodb::velocypack::Builder subBuilder;

    {
      arangodb::velocypack::ObjectBuilder subBuilderWrapper(&subBuilder);

      subBuilderWrapper->add("commitIntervalBatchSize", arangodb::velocypack::Value(_commitBulk._commitIntervalBatchSize));

      if (!jsonCommitBaseMeta(*(subBuilderWrapper.builder), _commitBulk)) {
        return false;
      }
    }

    builder.add("commitBulk", subBuilder.slice());
  }

  if ((!ignoreEqual || _commitItem != ignoreEqual->_commitItem) && (!mask || mask->_commitItem)) {
    arangodb::velocypack::Builder subBuilder;

    {
      arangodb::velocypack::ObjectBuilder subBuilderWrapper(&subBuilder);

      subBuilderWrapper->add("commitIntervalMsec", arangodb::velocypack::Value(_commitItem._commitIntervalMsec));

      if (!jsonCommitBaseMeta(*(subBuilderWrapper.builder), _commitItem)) {
        return false;
      }
    }

    builder.add("commitItem", subBuilder.slice());
  }

  if ((!ignoreEqual || _dataPath != ignoreEqual->_dataPath) && (!mask || mask->_dataPath) && !_dataPath.empty()) {
    builder.add("dataPath", arangodb::velocypack::Value(_dataPath));
  }

  if ((!ignoreEqual || _iid != ignoreEqual->_iid) && (!mask || mask->_iid)) {
    builder.add("id", arangodb::velocypack::Value(_iid));
  }

  if ((!ignoreEqual || _locale != ignoreEqual->_locale) && (!mask || mask->_locale)) {
    builder.add("locale", arangodb::velocypack::Value(irs::locale_utils::name(_locale)));
  }

  if ((!ignoreEqual || _name != ignoreEqual->_name) && (!mask || mask->_name)) {
    builder.add("name", arangodb::velocypack::Value(_name));
  }

  if ((!ignoreEqual || _nestingDelimiter != ignoreEqual->_nestingDelimiter) && (!mask || mask->_nestingDelimiter)) {
    builder.add("nestingDelimiter", arangodb::velocypack::Value(_nestingDelimiter));
  }

  if ((!ignoreEqual || _nestingListOffsetPrefix != ignoreEqual->_nestingListOffsetPrefix) && (!mask || mask->_nestingListOffsetPrefix)) {
    builder.add("nestingListOffsetPrefix", arangodb::velocypack::Value(_nestingListOffsetPrefix));
  }

  if ((!ignoreEqual || _nestingListOffsetSuffix != ignoreEqual->_nestingListOffsetSuffix) && (!mask || mask->_nestingListOffsetSuffix)) {
    builder.add("nestingListOffsetSuffix", arangodb::velocypack::Value(_nestingListOffsetSuffix));
  }

  if ((!ignoreEqual || _scorers != ignoreEqual->_scorers) && (!mask || mask->_scorers)) {
    arangodb::velocypack::Builder subBuilder;

    {
      arangodb::velocypack::ArrayBuilder subBuilderWrapper(&subBuilder);

      for (auto& scorer: _scorers) {
        subBuilderWrapper->add(arangodb::velocypack::Value(scorer.first));
      }
    }

    builder.add("scorers", subBuilder.slice());
  }

  if ((!ignoreEqual || _threadsMaxIdle != ignoreEqual->_threadsMaxIdle) && (!mask || mask->_threadsMaxIdle)) {
    builder.add("threadsMaxIdle", arangodb::velocypack::Value(_threadsMaxIdle));
  }

  if ((!ignoreEqual || _threadsMaxTotal != ignoreEqual->_threadsMaxTotal) && (!mask || mask->_threadsMaxTotal)) {
    builder.add("threadsMaxTotal", arangodb::velocypack::Value(_threadsMaxTotal));
  }

  return true;
}

bool IResearchViewMeta::json(
  arangodb::velocypack::ObjectBuilder const& builder,
  IResearchViewMeta const* ignoreEqual /*= nullptr*/,
  Mask const* mask /*= nullptr*/
) const {
  return builder.builder && json(*(builder.builder), ignoreEqual, mask);
}

size_t IResearchViewMeta::memSize() const {
  auto size = sizeof(IResearchViewMeta);

  size += sizeof(TRI_voc_cid_t) * _collections.size();
  size += _dataPath.size();
  size += sizeof(irs::flags::type_map::key_type) * _features.size();
  size += _name.length();
  size += _nestingDelimiter.size();
  size += _nestingListOffsetPrefix.size();
  size += _nestingListOffsetSuffix.size();

  for (auto& scorer: _scorers) {
    size += scorer.first.length() + sizeof(scorer.second);
  }

  return size;
}

NS_END // iresearch
NS_END // arangodb

// -----------------------------------------------------------------------------
// --SECTION--                                                       END-OF-FILE
// -----------------------------------------------------------------------------