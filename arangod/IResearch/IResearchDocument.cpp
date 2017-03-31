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

#include "analysis/token_attributes.hpp"
#include "search/boolean_filter.hpp"
#include "search/term_filter.hpp"

#include "utils/log.hpp"
#include "utils/numeric_utils.hpp"

namespace {

irs::string_ref const CID_FIELD("@_CID");
irs::string_ref const RID_FIELD("@_REV");
irs::string_ref const PK_COLUMN("@_PK");

inline irs::bytes_ref toBytesRef(uint64_t const& value) {
  return irs::bytes_ref(
    reinterpret_cast<irs::byte_type const*>(&value),
    sizeof(value)
  );
}

inline void ensureLittleEndian(uint64_t& value) {
  if (irs::numeric_utils::is_big_endian()) {
    // FIXME convert to LE
  }
}

inline irs::bytes_ref toBytesRefLE(uint64_t& value) {
  ensureLittleEndian(value);
  return toBytesRef(value);
}

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
irs::unbounded_object_pool<AnyFactory<irs::string_token_stream>> StringStreamPool(DEFAULT_POOL_SIZE);
irs::unbounded_object_pool<AnyFactory<irs::null_token_stream>> NullStreamPool(DEFAULT_POOL_SIZE);
irs::unbounded_object_pool<AnyFactory<irs::boolean_token_stream>> BoolStreamPool(DEFAULT_POOL_SIZE);
irs::unbounded_object_pool<AnyFactory<irs::numeric_token_stream>> NumericStreamPool(DEFAULT_POOL_SIZE);
irs::flags NumericStreamFeatures{ irs::granularity_prefix::type() };

// appends the specified 'value' to 'out'
inline void append(std::string& out, size_t value) {
  auto const size = out.size(); // intial size
  out.resize(size + 21); // enough to hold all numbers up to 64-bits
  auto const written = sprintf(&out[size], IR_SIZE_T_SPECIFIER, value);
  out.resize(size + written);
}

inline bool canHandleValue(
    VPackSlice const& value,
    arangodb::iresearch::IResearchLinkMeta const& context
) noexcept {
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
      return !context._tokenizers.empty();
    case VPackValueType::Binary:
    case VPackValueType::BCD:
    case VPackValueType::Custom:
    default:
      return false;
  }
}

// returns 'context' in case if can't find the specified 'field'
inline arangodb::iresearch::IResearchLinkMeta const* findMeta(
    irs::string_ref const& key,
    arangodb::iresearch::IResearchLinkMeta const* context
) {
  TRI_ASSERT(context);

  auto const* meta = context->_fields.findPtr(key);
  return meta ? meta->get() : context;
}

inline bool inObjectFiltered(
    std::string& buffer,
    arangodb::iresearch::IResearchLinkMeta const*& context,
    arangodb::iresearch::IResearchViewMeta const& /*viewMeta*/,
    arangodb::iresearch::IteratorValue const& value
) {
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
    arangodb::iresearch::IteratorValue const& value
) {
  auto const key = arangodb::iresearch::getStringRef(value.key);

  buffer.append(key.c_str(), key.size());
  context = findMeta(key, context);

  return canHandleValue(value.value, *context);
}

inline bool inArrayOrdered(
    std::string& buffer,
    arangodb::iresearch::IResearchLinkMeta const*& context,
    arangodb::iresearch::IResearchViewMeta const& viewMeta,
    arangodb::iresearch::IteratorValue const& value
) {
  buffer += viewMeta._nestingListOffsetPrefix;
  append(buffer, value.pos);
  buffer += viewMeta._nestingListOffsetSuffix;

  return canHandleValue(value.value, *context);
}

inline bool inArray(
    std::string& /*buffer*/,
    arangodb::iresearch::IResearchLinkMeta const*& context,
    arangodb::iresearch::IResearchViewMeta const& /*viewMeta*/,
    arangodb::iresearch::IteratorValue const& value
) noexcept {
  return canHandleValue(value.value, *context);
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

typedef arangodb::iresearch::IResearchLinkMeta::TokenizerPool const* TokenizerPoolPtr;

void setNullValue(
    VPackSlice const& value,
    std::shared_ptr<irs::token_stream>& tokenizer,
    irs::flags const*& features
) {
  TRI_ASSERT(value.isNull());

  auto stream = NullStreamPool.emplace();
  stream->reset();

  tokenizer = stream;
  features = &irs::flags::empty_instance();
}

void setBoolValue(
    VPackSlice const& value,
    std::shared_ptr<irs::token_stream>& tokenizer,
    irs::flags const*& features
) {
  TRI_ASSERT(value.isBool());

  auto stream = BoolStreamPool.emplace();
  stream->reset(value.getBool());

  tokenizer = stream;
  features = &irs::flags::empty_instance();
}

void setNumericValue(
    VPackSlice const& value,
    std::shared_ptr<irs::token_stream>& tokenizer,
    irs::flags const*& features
) {
  TRI_ASSERT(value.isNumber());

  auto stream = NumericStreamPool.emplace();
  stream->reset(value.getNumber<double>());

  tokenizer = stream;
  features = &NumericStreamFeatures;
}

void setStringValue(
    VPackSlice const& value,
    std::shared_ptr<irs::token_stream>& tokenizer,
    irs::flags const*& features,
    TokenizerPoolPtr pool
) {
  TRI_ASSERT(value.isString());

  auto analyzer = pool->tokenizer();
  analyzer->reset(arangodb::iresearch::getStringRef(value));

  tokenizer = analyzer;
  features = pool->features();
}

void setIDValue(
    uint64_t& value,
    irs::token_stream& tokenizer
) {
#ifdef ARANGODB_ENABLE_MAINTAINER_MODE
  auto& sstream = dynamic_cast<irs::string_token_stream&>(tokenizer);
#else
  auto& sstream = static_cast<irs::string_token_stream&>(tokenizer);
#endif

  sstream.reset(toBytesRefLE(value));
}

}

namespace arangodb {
namespace iresearch {

// ----------------------------------------------------------------------------
// --SECTION--                                             Field implementation
// ----------------------------------------------------------------------------

Field::Field(Field&& rhs)
  : _features(rhs._features),
    _tokenizer(std::move(rhs._tokenizer)),
    _name(std::move(rhs._name)),
    _boost(rhs._boost) {
  rhs._features = nullptr;
}

Field& Field::operator=(Field&& rhs) {
  if (this != &rhs) {
    _features = rhs._features;
    _tokenizer = std::move(rhs._tokenizer);
    _name = std::move(rhs._name);
    _boost= rhs._boost;
    rhs._features = nullptr;
  }
  return *this;
}

// ----------------------------------------------------------------------------
// --SECTION--                                       SystemField implementation
// ----------------------------------------------------------------------------

SystemField::SystemField() {
  _tokenizer = StringStreamPool.emplace();
}

void SystemField::setCidValue(TRI_voc_cid_t cid) {
  _name = CID_FIELD;
  setIDValue(_value = cid, *_tokenizer);
}

void SystemField::setRidValue(TRI_voc_rid_t rid) {
  _name = RID_FIELD;
  setIDValue(_value = rid, *_tokenizer);
}

// ----------------------------------------------------------------------------
// --SECTION--                                     FieldIterator implementation
// ----------------------------------------------------------------------------

/*static*/ FieldIterator FieldIterator::END;

FieldIterator::FieldIterator(
    VPackSlice const& doc,
    IResearchLinkMeta const& linkMeta,
    IResearchViewMeta const& viewMeta
) : _meta(&viewMeta) {
  if (!isArrayOrObject(doc)) {
    // can't handle plain objects
    return;
  }

  auto const* context = &linkMeta;

  // initialize iterator's value
  _name = BufferPool.emplace();
  _name->clear();

  // push the provided 'doc' to stack
  if (push(doc, context)) {
    TRI_ASSERT(context);

    setValue(topValue().value, *context); // initialize current value
  } else {
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

bool FieldIterator::setValue(
    VPackSlice const& value,
    IResearchLinkMeta const& context
) {
  // ensure that default meta has only one tokenizer, since we use
  // that as a surrogate range for all non-string values
  TRI_ASSERT(1 == IResearchLinkMeta::DEFAULT()._tokenizers.size());

  resetTokenizers(IResearchLinkMeta::DEFAULT()); // set surrogate tokenizers
  _value._boost = context._boost;                // set boost
  _value._name = nameBuffer();                   // set name

  auto& tokenizer = _value._tokenizer;
  auto& features = _value._features;

  switch (value.type()) {
    case VPackValueType::None:
    case VPackValueType::Illegal:
      return false;
    case VPackValueType::Null:
      setNullValue(value, tokenizer, features);
      return true;
    case VPackValueType::Bool:
      setBoolValue(value, tokenizer, features);
      return true;
    case VPackValueType::Array:
    case VPackValueType::Object:
      return false;
    case VPackValueType::Double:
      setNumericValue(value, tokenizer, features);
      return true;
    case VPackValueType::UTCDate:
    case VPackValueType::External:
    case VPackValueType::MinKey:
    case VPackValueType::MaxKey:
      return false;
    case VPackValueType::Int:
    case VPackValueType::UInt:
    case VPackValueType::SmallInt:
      setNumericValue(value, tokenizer, features);
      return true;
    case VPackValueType::String:
      resetTokenizers(context); // reset string tokenizers
      setStringValue(value, tokenizer, features, _begin);
      return true;
    case VPackValueType::Binary:
    case VPackValueType::BCD:
    case VPackValueType::Custom:
    default:
      return false;
  }
}

void FieldIterator::next() {
  TRI_ASSERT(valid());

  VPackSlice value = topValue().value;

  if (++_begin != _end) {
    // can have multiple tokenizers for string values only
    setStringValue(value, _value._tokenizer, _value._features, _begin);
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

    value = topValue().value;

  } while (!push(value, context));

  TRI_ASSERT(context);
  setValue(topValue().value, *context); // initialize value
}

// ----------------------------------------------------------------------------
// --SECTION--                                  DocumentIterator implementation
// ----------------------------------------------------------------------------



/*static*/ DocumentIterator DocumentIterator::END;

/*static*/ irs::filter::ptr DocumentIterator::filter(TRI_voc_cid_t cid) {
  auto filter = irs::by_term::make();

  // filter matching on cid
  static_cast<irs::by_term&>(*filter)
    .field(CID_FIELD) // set field
    .term(toBytesRefLE(cid)); // set value

  return std::move(filter);
}

/*static*/ irs::filter::ptr DocumentIterator::filter(
    TRI_voc_cid_t cid,
    TRI_voc_rid_t rid
) {
  auto filter = irs::And::make();

  // filter matching on cid and rid
  static_cast<irs::And&>(*filter).add<irs::by_term>()
    .field(CID_FIELD) // set field
    .term(toBytesRefLE(cid));   // set value

  static_cast<irs::And&>(*filter).add<irs::by_term>()
    .field(RID_FIELD) // set field
    .term(toBytesRefLE(rid));   // set value

  return std::move(filter);
}

DocumentIterator::DocumentIterator(
    TRI_voc_cid_t const cid, TRI_voc_rid_t const rid,
    SystemField& header, FieldIterator& body
) noexcept
  : _body(&body), _values{ &(*body), &header }, _rid(rid) {
  systemField().setCidValue(cid); // initialize CID field
  _i += !_body->valid(); // check whether document body is valid
}

void DocumentIterator::next() {
  switch (_i) {
    case 0: {
      TRI_ASSERT(_body->valid());
      ++(*_body);
      _i += !_body->valid();
    } break;
    case 1: ++_i; break; // has been already initialized in ctor
    case 2: systemField().setRidValue(_rid); ++_i; break;
  }
}

// ----------------------------------------------------------------------------
// --SECTION--                                DocumentPrimaryKey implementation
// ----------------------------------------------------------------------------

/* static */ irs::string_ref const& DocumentPrimaryKey::PK() {
  return PK_COLUMN;
}

DocumentPrimaryKey::DocumentPrimaryKey(
    TRI_voc_cid_t cid,
    TRI_voc_rid_t rid
) noexcept
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
