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

#include "IResearchDocument.h"
#include "IResearchViewMeta.h"

#include "search/boolean_filter.hpp"
#include "search/term_filter.hpp"

#include "utils/log.hpp"

namespace {

irs::string_ref const CID_FIELD("@_CID");
irs::string_ref const RID_FIELD("@_REV");
irs::string_ref const PK_COLUMN("@_PK");

// wrapper for use objects with the IResearch unbounded_object_pool
template<typename T>
struct AnyFactory {
  typedef std::shared_ptr<T> ptr;

  template<typename... Args>
  static ptr make(Args&&... args) {
    return std::make_shared<T>(std::forward<Args>(args)...);
  }
}; // AnyFactory

const size_t DEFAULT_POOL_SIZE = 8; // arbitrary value
irs::unbounded_object_pool<AnyFactory<std::string>> BufferPool(DEFAULT_POOL_SIZE);
irs::unbounded_object_pool<AnyFactory<irs::null_token_stream>> NullStreamPool(DEFAULT_POOL_SIZE);
irs::unbounded_object_pool<AnyFactory<irs::boolean_token_stream>> BoolStreamPool(DEFAULT_POOL_SIZE);
irs::unbounded_object_pool<AnyFactory<irs::numeric_token_stream>> NumericStreamPool(DEFAULT_POOL_SIZE);

// appends the specified 'value' to 'out'
inline void append(std::string& out, size_t value) {
  auto const size = out.size(); // intial size
  out.resize(size + 21); // enough to hold all numbers up to 64-bits
  auto const written = sprintf(&out[size], IR_SIZE_T_SPECIFIER, value);
  out.resize(size + written);
}

inline bool canHandleValue(
    VPackSlice const& value,
    arangodb::iresearch::IResearchLinkMeta const& context) noexcept {
  switch (value.type()) {
    case VPackValueType::None:
    case VPackValueType::Illegal:
      return false;
    case VPackValueType::Null:
    case VPackValueType::Bool:
    case VPackValueType::Array:
    case VPackValueType::Object:
    case VPackValueType::Double:
      return true;
    case VPackValueType::UTCDate:
    case VPackValueType::External:
    case VPackValueType::MinKey:
    case VPackValueType::MaxKey:
      return false;
    case VPackValueType::Int:
    case VPackValueType::UInt:
    case VPackValueType::SmallInt:
      return true;
    case VPackValueType::String:
      return context._tokenizers.empty();
    case VPackValueType::Binary:
    case VPackValueType::BCD:
    case VPackValueType::Custom:
      return false;
  }
}

// returns 'context' in case if can't find the specified 'field'
inline arangodb::iresearch::IResearchLinkMeta const* findMeta(
    irs::string_ref const& key,
    arangodb::iresearch::IResearchLinkMeta const* context) {
  TRI_ASSERT(context);
  auto const* meta = context->_fields.findPtr(key);
  return meta ? meta : context;
}

inline bool inObjectFiltered(
    std::string& buffer,
    arangodb::iresearch::IResearchLinkMeta const*& context,
    arangodb::iresearch::IResearchViewMeta const& /*viewMeta*/,
    arangodb::iresearch::IteratorValue const& value) {
  auto const key = arangodb::iresearch::getStringRef(value.key);

  auto const* meta = findMeta(key, context);

  if (meta == context) {
    return false;
  }

  buffer.append(key.c_str(), key.size());
  context = meta;

  return canHandleValue(value.value, *context);
}

inline bool inObject(
    std::string& buffer,
    arangodb::iresearch::IResearchLinkMeta const*& context,
    arangodb::iresearch::IResearchViewMeta const& /*viewMeta*/,
    arangodb::iresearch::IteratorValue const& value) {
  auto const key = arangodb::iresearch::getStringRef(value.key);

  buffer.append(key.c_str(), key.size());
  context = findMeta(key, context);

  return canHandleValue(value.value, context);
}

inline bool inArrayOrdered(
    std::string& buffer,
    arangodb::iresearch::IResearchLinkMeta const*& context,
    arangodb::iresearch::IResearchViewMeta const& viewMeta,
    arangodb::iresearch::IteratorValue const& value) {
  buffer += viewMeta._nestingListOffsetPrefix;
  append(buffer, value.pos);
  buffer += viewMeta._nestingListOffsetSuffix;

  return canHandleValue(value.value, context);
}

inline bool inArray(
    std::string& /*buffer*/,
    arangodb::iresearch::IResearchLinkMeta const*& context,
    arangodb::iresearch::IResearchViewMeta const& /*viewMeta*/,
    arangodb::iresearch::IteratorValue const& value) noexcept {
  return canHandleValue(value.value, context);
}

typedef bool(*Filter)(
  std::string& buffer,
  arangodb::iresearch::IResearchLinkMeta const*& context,
  arangodb::iresearch::IResearchViewMeta const& viewMeta,
  arangodb::iresearch::IteratorValue const& value
);

Filter const valueAcceptors[] = {
  &inObjectFiltered, // type == Object, nestListValues == false, includeAllValues == false
  &inObject,         // type == Object, nestListValues == false, includeAllValues == true
  &inObjectFiltered, // type == Object, nestListValues == true , includeAllValues == false
  &inObject,         // type == Object, nestListValues == true , includeAllValues == true
  &inArray,          // type == Array , nestListValues == flase, includeAllValues == false
  &inArray,          // type == Array , nestListValues == flase, includeAllValues == true
  &inArrayOrdered,   // type == Array , nestListValues == true,  includeAllValues == false
  &inArrayOrdered    // type == Array , nestListValues == true,  includeAllValues == true
};

inline Filter getFilter(
  VPackSlice value,
  arangodb::iresearch::IResearchLinkMeta const& meta
) noexcept {
  TRI_ASSERT(arangodb::iresearch::isArrayOrObject(value));

  return valueAcceptors[
    4 * value.isArray()
      + 2 * meta._nestListValues
      + meta._includeAllFields
   ];
}

typedef std::shared_ptr<irs::token_stream>(*TokenizerFactory)(
  VPackSlice const& value,
  IResearchLinkMeta::TokenizerPool* pool
);

std::shared_ptr<irs::token_stream> noopFactory(
    VPackSlice const& /*value*/,
    IResearchLinkMeta::TokenizerPool* /*pool*/) {
  return nullptr;
}

std::shared_ptr<irs::token_stream> nullTokenizerFactory(
    VPackSlice const& value,
    IResearchLinkMeta::TokenizerPool* /*pool*/) {
  TRI_ASSERT(value.isNull());
  return NullStreamPool.emplace();
}

std::shared_ptr<irs::token_stream> boolTokenizerFactory(
    VPackSlice const& value,
    IResearchLinkMeta::TokenizerPool* /*pool*/) {
  TRI_ASSERT(value.isBool());

  auto tokenizer = BoolStreamPool.emplace();
  tokenizer->reset(value.getBool());

  return tokenizer;
}

std::shared_ptr<irs::token_stream> numericTokenizerFactory(
    VPackSlice const& value,
    IResearchLinkMeta::TokenizerPool* /*pool*/) {
  TRI_ASSERT(value.isNumber());

  auto tokenizer = NumericStreamPool.emplace();
  tokenizer->reset(value.getNumber<double>());

  return tokenizer;
}

std::shared_ptr<irs::token_stream> stringTokenizerFactory(
    VPackSlice const& value,
    IResearchLinkMeta::TokenizerPool* pool) {
  TRI_ASSERT(value.isString());

  auto tokenizer = pool->tokenizer();
  tokenizer->reset(getStringRef(value));

  return tokenizer;
}

TokenizerFactory const TokenizerFactories[] {
  &noopFactory,             // None
  &noopFactory,             // Illegal
  &nullTokenizerFactory,    // Null
  &boolTokenizerFactory,    // Bool
  &noopFactory,             // Array
  &noopFactory,             // Object
  &numericTokenizerFactory, // Double
  &noopFactory,             // UTCDate
  &noopFactory,             // External
  &noopFactory,             // MinKey
  &noopFactory,             // MaxKey
  &numericTokenizerFactory, // Int
  &numericTokenizerFactory, // UInt
  &numericTokenizerFactory, // SmallInt
  &stringTokenizerFactory,  // String
  &noopFactory,             // Binary
  &noopFactory,             // BCD
  &noopFactory,             // Custom
};

inline TokenizerFactory getTokenizerFactory(VPackSlice const& value) noexcept {
  return TokenizerFactories[size_t(value.type())];
}

}

namespace arangodb {
namespace iresearch {

// ----------------------------------------------------------------------------
// --SECTION--                                     FieldIterator implementation
// ----------------------------------------------------------------------------

Field::Field(Field&& rhs)
  : _name(std::move(rhs._name)),
    _meta(rhs._meta) {
  rhs._meta = nullptr;
}

Field::Field(Field const& rhs) {
  *this = rhs;
}

Field& Field::operator=(Field const& rhs) {
  if (this != &rhs) {
    if (rhs._name) {
      _name = BufferPool.emplace(); // init buffer for name
      *_name = *rhs._name; // copy content
    }
    _meta = rhs._meta;
  }
  return *this;
}

Field& Field::operator=(Field&& rhs) {
  if (this != &rhs) {
    _name = std::move(rhs._name);
    _meta = rhs._meta;
    rhs._meta = nullptr;
  }
  return *this;
}

/*static*/ FieldIterator FieldIterator::END;

/*static*/ irs::filter::ptr FieldIterator::filter(TRI_voc_cid_t cid) {
  irs::bytes_ref const cidTerm(reinterpret_cast<irs::byte_type*>(&cid), sizeof(cid));

  auto filter = irs::by_term::make();

  // filter matching on cid
  static_cast<irs::by_term&>(*filter)
    .field(CID_FIELD) // set field
    .term(cidTerm); // set value

  return std::move(filter);
}

/*static*/ irs::filter::ptr FieldIterator::filter(
    TRI_voc_cid_t cid,
    TRI_voc_rid_t rid) {
  irs::bytes_ref const cidTerm(reinterpret_cast<irs::byte_type*>(&cid), sizeof(cid));
  irs::bytes_ref const ridTerm(reinterpret_cast<irs::byte_type*>(&rid), sizeof(rid));

  auto filter = irs::And::make();

  // filter matching on cid and rid
  static_cast<irs::And&>(*filter).add<irs::by_term>()
    .field(CID_FIELD) // set field
    .term(cidTerm);   // set value

  static_cast<irs::And&>(*filter).add<irs::by_term>()
    .field(RID_FIELD) // set field
    .term(ridTerm);   // set value

  return std::move(filter);
}

// TODO FIXME must putout system fields (cid/rid) as well to allow for building a filter on CID/RID
FieldIterator::FieldIterator(
    TRI_voc_cid_t,
    TRI_voc_rid_t,
    VPackSlice const& doc,
    IResearchLinkMeta const& linkMeta,
    IResearchViewMeta const& viewMeta)
  : _meta(&viewMeta) {

  // initialize iterator's value
  _value._name = BufferPool.emplace();
  _value._name->clear();
  _value._meta = &linkMeta;

  // push the provided 'doc' to stack
  if (isArrayOrObject(doc) && !push(doc, _value._meta)) {
    next();
  }
}

IResearchLinkMeta const* FieldIterator::nextTop() {
  auto& name = nameBuffer();
  auto& level = top();
  auto& it = level.it;
  auto const* context = level.meta;
  auto const filter = level.filter;

  name.resize(level.nameLength);
  while (it.next() &&  !filter(name, context, *_meta, it.value())) {
    // filtered out
    name.resize(level.nameLength);
  }

  return context;
}

bool FieldIterator::push(VPackSlice slice, IResearchLinkMeta const*& context) {
  auto& name = nameBuffer();

  while (isArrayOrObject(slice)) {
    if (!name.empty() && !slice.isArray()) {
      name += _meta->_nestingDelimiter;
    }

    auto const filter = getFilter(slice, *context);

    _stack.emplace_back(slice, name.size(), *context, filter);

    auto& it = top().it;

    if (!it.valid()) {
      // empty object or array, skip it
      return false;
    }

    auto& value = it.value();

    if (!filter(name, context, *_meta, value)) {
      // filtered out
      return false;
    }

    slice = value.value;
  }

  return true;
}

void FieldIterator::next() {
  TRI_ASSERT(valid());

  VPackSlice value = top().it.value().value;

  if (++_begin != _end) {
    _value._tokenizer = getTokenizerFactory(value)(value, _begin);
    return;
  }

  IResearchLinkMeta const* context;

  do {
    // advance top iterator
    context = nextTop();

    // pop all exhausted iterators
    for (; !top().it.valid(); context = nextTop()) {
      _stack.pop_back();

      if (!valid()) {
        // reached the end
        return;
      }

      // reset name to previous size
      nameBuffer().resize(top().nameLength);
    }

    value = top().it.value().value;

  } while (!push(value, context));

  // refresh tokenizers
  _begin = context->Tokenizers.begin();
  _end = context->Tokenizers.end();

  // refresh value
  _value._meta = context;
  _value._tokenizer = getTokenizerFactory(value)(value, _begin);
}

/* static */ irs::string_ref const& DocumentPrimaryKey::PK() {
  return PK_COLUMN;
}

DocumentPrimaryKey::DocumentPrimaryKey(
    TRI_voc_cid_t cid,
    TRI_voc_rid_t rid) noexcept
  : _keys{ cid, rid } {
  static_assert(sizeof(_keys) == sizeof(cid) + sizeof(rid), "Invalid size");
}

bool DocumentPrimaryKey::write(irs::data_output& out) const {
  out.write_bytes(
    reinterpret_cast<const irs::byte_type*>(_keys),
    sizeof(_keys)
  );

  return true;
}

} // iresearch
} // arangodb

// -----------------------------------------------------------------------------
// --SECTION--                                                       END-OF-FILE
// -----------------------------------------------------------------------------
