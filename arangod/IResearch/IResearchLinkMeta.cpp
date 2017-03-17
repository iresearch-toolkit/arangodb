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

#include <limits>
#include <unordered_map>
#include <unordered_set>

#include "analysis/analyzers.hpp"
#include "analysis/token_attributes.hpp"
#include "utils/hash_utils.hpp"
#include "utils/locale_utils.hpp"

#include "VelocyPackHelper.h"
#include "Basics/StringUtils.h"
#include "velocypack/Builder.h"
#include "velocypack/Iterator.h"

#include "IResearchLinkMeta.h"

NS_LOCAL

const size_t DEFAULT_POOL_SIZE = 8; // arbitrary value
const irs::string_ref IDENTITY_TOKENIZER_NAME("identity");

class IdentityValue: public irs::term_attribute {
public:
  DECLARE_FACTORY_DEFAULT();

  virtual ~IdentityValue() {}

  virtual void clear() override {
    _value = irs::bytes_ref::nil;
  }

  virtual const irs::bytes_ref& value() const {
    return _value;
  }

  void value(irs::bytes_ref const& data) {
    _value = data;
  }

 private:
  iresearch::bytes_ref _value;
};

DEFINE_FACTORY_DEFAULT(IdentityValue);

class IdentityTokenizer: public irs::analysis::analyzer {
 public:
  DECLARE_ANALYZER_TYPE();
  DECLARE_FACTORY_DEFAULT(irs::string_ref const& args); // args ignored

  IdentityTokenizer();
  virtual iresearch::attributes const& attributes() const NOEXCEPT override;
  virtual bool next() override;
  virtual bool reset(irs::string_ref const& data) override;

 private:
  irs::attributes _attrs;
  bool _empty;
  irs::string_ref _value;
};

DEFINE_ANALYZER_TYPE_NAMED(IdentityTokenizer, IDENTITY_TOKENIZER_NAME);
REGISTER_ANALYZER(IdentityTokenizer);

/*static*/ irs::analysis::analyzer::ptr IdentityTokenizer::make(irs::string_ref const& args) {
  PTR_NAMED(IdentityTokenizer, ptr); \
  return ptr; \
}

IdentityTokenizer::IdentityTokenizer()
  : irs::analysis::analyzer(IdentityTokenizer::type()), _empty(true) {
  _attrs.add<IdentityValue>();
}

irs::attributes const& IdentityTokenizer::attributes() const NOEXCEPT {
  return _attrs;
}

bool IdentityTokenizer::next() {
  auto empty = _empty;

  _attrs.get<IdentityValue>()->value(irs::ref_cast<irs::byte_type>(_value));
  _empty = true;
  _value = irs::string_ref::nil;

  return !empty;
}

bool IdentityTokenizer::reset(irs::string_ref const& data) {
  _empty = false;
  _value = data;

  return !_empty;
}

bool equalTokenizers(
  arangodb::iresearch::IResearchLinkMeta::Tokenizers const& lhs,
  arangodb::iresearch::IResearchLinkMeta::Tokenizers const& rhs
) noexcept {
  if (lhs.size() != rhs.size()) {
    return false;
  }

  struct Hash {
    size_t operator()(std::pair<irs::string_ref, irs::string_ref> const& value) const {
      static irs::string_ref_hash_t hasher;
      return hasher(value.first) ^ hasher(value.second);
    }
  };
  std::unordered_multiset<std::pair<irs::string_ref, irs::string_ref>, Hash> expected;

  for (auto& entry: lhs) {
    expected.emplace(entry.name(), entry.args());
  }

  for (auto& entry: rhs) {
    auto itr = expected.find(std::make_pair(entry.name(), entry.args()));

    if (itr == expected.end()) {
      return false; // values do not match
    }

    expected.erase(itr); // ensure same count of duplicates
  }

  return true;
}

NS_END

NS_BEGIN(arangodb)
NS_BEGIN(iresearch)

IResearchLinkMeta::Mask::Mask(bool mask /*= false*/) noexcept
  : _boost(mask),
    _fields(mask),
    _includeAllFields(mask),
    _listValuation(mask),
    _locale(mask),
    _tokenizers(mask) {
}

/*static*/ irs::analysis::analyzer::ptr IResearchLinkMeta::TokenizerPool::Builder::make(
  irs::string_ref const& name, irs::string_ref const& args
) {
  return irs::analysis::analyzers::get(name, args);
}

IResearchLinkMeta::TokenizerPool::TokenizerPool(
  std::string const& name, std::string const& args
): _args(args), _name(name), _pool(DEFAULT_POOL_SIZE) {
}

bool IResearchLinkMeta::TokenizerPool::operator==(TokenizerPool const& other) const noexcept {
  return _name == other._name && _args == other._args;
}

std::string const& IResearchLinkMeta::TokenizerPool::args() const noexcept {
  return _args;
}

std::string const& IResearchLinkMeta::TokenizerPool::name() const noexcept {
  return _name;
}

irs::analysis::analyzer::ptr IResearchLinkMeta::TokenizerPool::tokenizer() const {
  return _pool.emplace(_name, _args);
}

size_t IResearchLinkMeta::TokenizerPool::Hash::operator()(TokenizerPool const& value) const {
  static irs::string_ref_hash_t hasher;
  return hasher(value._name) ^ hasher(value._args);
}

IResearchLinkMeta::IResearchLinkMeta()
  : _boost(1.0), // no boosting of field preference in view ordering
    //_fields(<empty>), // empty map to match all encounteredfields
    _includeAllFields(false), // true to match all encountered fields, false match only fields in '_fields'
    _listValuation(ListValuation::MULTIVALUED), // treat as SQL-IN
    _locale(std::locale::classic()) {
  _tokenizers.emplace(IDENTITY_TOKENIZER_NAME, ""); // identity-only tokenization
}

IResearchLinkMeta::IResearchLinkMeta(IResearchLinkMeta const& other) {
  *this = other;
}

IResearchLinkMeta::IResearchLinkMeta(IResearchLinkMeta&& other) noexcept {
  *this = std::move(other);
}

IResearchLinkMeta& IResearchLinkMeta::operator=(IResearchLinkMeta&& other) noexcept {
  if (this != &other) {
    _boost = std::move(other._boost);
    _fields = std::move(other._fields);
    _includeAllFields = std::move(other._includeAllFields);
    _listValuation = std::move(other._listValuation);
    _locale = std::move(other._locale);
    _tokenizers = std::move(other._tokenizers);
  }

  return *this;
}

IResearchLinkMeta& IResearchLinkMeta::operator=(IResearchLinkMeta const& other) {
  if (this != &other) {
    _boost = other._boost;
    _fields = other._fields;
    _includeAllFields = other._includeAllFields;
    _listValuation = other._listValuation;
    _locale = other._locale;
    _tokenizers = other._tokenizers;
  }

  return *this;
}

bool IResearchLinkMeta::operator==(
  IResearchLinkMeta const& other
) const noexcept {
  if (_boost != other._boost) {
    return false; // values do not match
  }

  if (_fields.size() != other._fields.size()) {
    return false; // values do not match
  }

  auto itr = other._fields.begin();

  for (auto& entry: _fields) {
    if (itr->first != entry.first || itr->second != entry.second) {
      return false; // values do not match
    }

    ++itr;
  }

  if (_includeAllFields != other._includeAllFields) {
    return false; // values do not match
  }

  if (_listValuation != other._listValuation) {
    return false; // values do not match
  }

  if (_locale != other._locale) {
    return false; // values do not match
  }

  if (!equalTokenizers(_tokenizers, other._tokenizers)) {
    return false; // values do not match
  }

  return true;
}

bool IResearchLinkMeta::operator!=(
  IResearchLinkMeta const& other
) const noexcept {
  return !(*this == other);
}

/*static*/ const IResearchLinkMeta& IResearchLinkMeta::DEFAULT() {
  static const IResearchLinkMeta meta;

  return meta;
}

bool IResearchLinkMeta::init(
  arangodb::velocypack::Slice const& slice,
  std::string& errorField,
  IResearchLinkMeta const& defaults /*= DEFAULT()*/,
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
    // optional floating point number
    static const std::string fieldName("boost");

    if (!getNumber(_boost, slice, fieldName, mask->_boost, defaults._boost)) {
      errorField = fieldName;

      return false;
    }
  }

  {
    // optional bool
    static const std::string fieldName("includeAllFields");

    mask->_includeAllFields = slice.hasKey(fieldName);

    if (!mask->_includeAllFields) {
      _includeAllFields = defaults._includeAllFields;
    } else {
      auto field = slice.get(fieldName);

      if (!field.isBool()) {
        errorField = fieldName;

        return false;
      }

      _includeAllFields = field.getBool();
    }
  }

  {
    // optional enum string
    static const std::string fieldName("listValuation");

    mask->_listValuation = slice.hasKey(fieldName);

    if (!mask->_listValuation) {
      _listValuation = defaults._listValuation;
    } else {
      auto field = slice.get(fieldName);

      if (!field.isString()) {
        errorField = fieldName;

        return false;
      }

      static const std::unordered_map<std::string, ListValuation::Type> listValuation = {
        { "ignored", ListValuation::IGNORED },
        { "ordered", ListValuation::ORDERED },
        { "multivalued", ListValuation::MULTIVALUED },
      };

      auto itr = listValuation.find(field.copyString());

      if (itr == listValuation.end()) {
        errorField = fieldName;

        return false;
      }

      _listValuation = itr->second;
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
    // optional enum string map<name, args>
    static const std::string fieldName("tokenizers");

    mask->_tokenizers = slice.hasKey(fieldName);

    if (!mask->_tokenizers) {
      _tokenizers = defaults._tokenizers;
    } else {
      auto field = slice.get(fieldName);

      if (!field.isObject()) {
        errorField = fieldName;

        return false;
      }

      _tokenizers.clear(); // reset to match read values exactly

      for (arangodb::velocypack::ObjectIterator itr(field); itr.valid(); ++itr) {
        auto key = itr.key();

        if (!key.isString()) {
          errorField = fieldName + "=>[" + arangodb::basics::StringUtils::itoa(itr.index()) + "]";

          return false;
        }

        auto name = key.copyString();
        auto value = itr.value();

        if (!value.isArray()) {
          errorField = fieldName + "=>" + name;

          return false;
        }

        // inserting two identical values for name+args is a poor-man's boost multiplier
        for (arangodb::velocypack::ArrayIterator entryItr(value); entryItr.valid(); ++entryItr) {
          auto entry = entryItr.value();

          if (entry.isString()) {
            _tokenizers.emplace(name, entry.copyString());
          } else if (entry.isObject()) {
            _tokenizers.emplace(name, entry.toJson());
          } else {
            errorField = fieldName + "=>" + name + "=>[" + arangodb::basics::StringUtils::itoa(entryItr.index()) + "]";

            return false;
          }
        }
      }
    }
  }

  // .............................................................................
  // process fields last since children inherit from parent
  // .............................................................................

  {
    // optional string list
    static const std::string fieldName("fields");

    mask->_fields = slice.hasKey(fieldName);

    if (!mask->_fields) {
      _fields = defaults._fields;
    } else {
      auto field = slice.get(fieldName);

      if (!field.isObject()) {
        errorField = fieldName;

        return false;
      }

      auto subDefaults = *this;

      subDefaults._fields.clear(); // do not inherit fields and overrides from this field
      _fields.clear(); // reset to match either defaults or read values exactly

      for (arangodb::velocypack::ObjectIterator itr(field); itr.valid(); ++itr) {
        auto key = itr.key();
        auto value = itr.value();

        if (!key.isString()) {
          errorField = fieldName + "=>[" + arangodb::basics::StringUtils::itoa(itr.index()) + "]";

          return false;
        }

        auto name = key.copyString();

        if (!value.isObject()) {
          errorField = fieldName + "=>" + name;

          return false;
        }

        std::string childErrorField;

        if (!_fields[name].init(value, errorField, subDefaults)) {
          errorField = fieldName + "=>" + name + "=>" + childErrorField;

          return false;
        }
      }
    }
  }

  return true;
}

bool IResearchLinkMeta::json(
  arangodb::velocypack::Builder& builder,
  IResearchLinkMeta const* ignoreEqual /*= nullptr*/,
  Mask const* mask /*= nullptr*/
) const {
  if (!builder.isOpenObject()) {
    return false;
  }

  if ((!ignoreEqual || _boost != ignoreEqual->_boost) && (!mask || mask->_boost)) {
    builder.add("boost", arangodb::velocypack::Value(_boost));
  }

  if (!mask || mask->_fields) { // fields are not inherited from parent
    arangodb::velocypack::Builder fieldsBuilder;

    {
      arangodb::velocypack::ObjectBuilder fieldsBuilderWrapper(&fieldsBuilder);
      arangodb::velocypack::Builder fieldBuilder;
      Mask mask(true); // output all non-matching fields
      auto subDefaults = *this;

      subDefaults._fields.clear(); // do not inherit fields and overrides overrides from this field

      for(auto& entry: _fields) {
        mask._fields = !entry.second._fields.empty(); // do not output empty fields on subobjects

        if (!entry.second.json(arangodb::velocypack::ObjectBuilder(&fieldBuilder), &subDefaults, &mask)) {
          return false;
        }

        fieldsBuilderWrapper->add(entry.first, fieldBuilder.slice());
        fieldBuilder.clear();
      }
    }

    builder.add("fields", fieldsBuilder.slice());
  }

  if ((!ignoreEqual || _includeAllFields != ignoreEqual->_includeAllFields) && (!mask || mask->_includeAllFields)) {
    builder.add("includeAllFields", arangodb::velocypack::Value(_includeAllFields));
  }

  if ((!ignoreEqual || _listValuation != ignoreEqual->_listValuation) && (!mask || mask->_listValuation)) {
    struct ListValuationHash { size_t operator()(ListValuation::Type const& value) const { return value; } }; // for GCC compatibility
    static const std::unordered_map<ListValuation::Type, std::string, ListValuationHash> listValuation = {
      { ListValuation::IGNORED, "ignored" },
      { ListValuation::ORDERED, "ordered" },
      { ListValuation::MULTIVALUED, "multivalued" },
    };

    auto itr = listValuation.find(_listValuation);

    if (itr != listValuation.end()) {
      builder.add("listValuation", arangodb::velocypack::Value(itr->second));
    }
  }

  if ((!ignoreEqual || _locale != ignoreEqual->_locale) && (!mask || mask->_locale)) {
    builder.add("locale", arangodb::velocypack::Value(irs::locale_utils::name(_locale)));
  }

  if ((!ignoreEqual || !equalTokenizers(_tokenizers, ignoreEqual->_tokenizers)) && (!mask || mask->_tokenizers)) {
    std::multimap<irs::string_ref, irs::string_ref> tokenizers;
    arangodb::velocypack::Builder tokenizersBuilder;

    for (auto& entry: _tokenizers) {
      tokenizers.emplace(entry.name(), entry.args());
    }

    {
      arangodb::velocypack::ObjectBuilder tokenizersBuilderWrapper(&tokenizersBuilder);
      arangodb::velocypack::Builder tokenizerBuilder;
      irs::string_ref lastKey = irs::string_ref::nil;

      tokenizerBuilder.openArray();

      for (auto& entry: tokenizers) {
        if (!lastKey.null() && lastKey != entry.first) {
          tokenizerBuilder.close();
          tokenizersBuilderWrapper->add(lastKey, tokenizerBuilder.slice());
          tokenizerBuilder.clear();
          tokenizerBuilder.openArray();
        }

        lastKey = entry.first;
        tokenizerBuilder.add(arangodb::velocypack::Value(entry.second));
      }

      // add last key
      if (!lastKey.null()) {
        tokenizerBuilder.close();
        tokenizersBuilderWrapper->add(lastKey, tokenizerBuilder.slice());
      }
    }

    builder.add("tokenizers", tokenizersBuilder.slice());
  }

  return true;
}

bool IResearchLinkMeta::json(
  arangodb::velocypack::ObjectBuilder const& builder,
  IResearchLinkMeta const* ignoreEqual /*= nullptr*/,
  Mask const* mask /*= nullptr*/
) const {
  return builder.builder && json(*(builder.builder), ignoreEqual, mask);
}

size_t IResearchLinkMeta::memory() const {
  auto size = sizeof(IResearchLinkMeta);

  size += _fields.size() * sizeof(decltype(_fields)::value_type);

  for (auto& entry: _fields) {
    size += entry.first.size();
    size += entry.second.memory();
  }

  size += _tokenizers.size() * sizeof(decltype(_tokenizers)::value_type);

  for (auto& entry: _tokenizers) {
    size += entry.name().size();
    size += entry.args().size();
    size += DEFAULT_POOL_SIZE * sizeof(irs::analysis::analyzer::ptr); // don't know size of actual implementation
  }

  return size;
}

NS_END // iresearch
NS_END // arangodb

// -----------------------------------------------------------------------------
// --SECTION--                                                       END-OF-FILE
// -----------------------------------------------------------------------------