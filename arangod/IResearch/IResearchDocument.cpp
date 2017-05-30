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

#include "Logger/Logger.h"
#include "Logger/LogMacros.h"

#include "analysis/token_attributes.hpp"
#include "search/boolean_filter.hpp"
#include "search/term_filter.hpp"
#include "search/range_filter.hpp"
#include "search/granular_range_filter.hpp"

#include "utils/log.hpp"
#include "utils/numeric_utils.hpp"

#define Swap8Bytes(val) \
 ( (((val) >> 56) & UINT64_C(0x00000000000000FF)) | (((val) >> 40) & UINT64_C(0x000000000000FF00)) | \
   (((val) >> 24) & UINT64_C(0x0000000000FF0000)) | (((val) >>  8) & UINT64_C(0x00000000FF000000)) | \
   (((val) <<  8) & UINT64_C(0x000000FF00000000)) | (((val) << 24) & UINT64_C(0x0000FF0000000000)) | \
   (((val) << 40) & UINT64_C(0x00FF000000000000)) | (((val) << 56) & UINT64_C(0xFF00000000000000)) )

NS_LOCAL

////////////////////////////////////////////////////////////////////////////////
/// @brief the delimiter used to separate jSON nesting levels when generating
///        flat iResearch field names
////////////////////////////////////////////////////////////////////////////////
std::string const NESTING_LEVEL_DELIMITER(".");

////////////////////////////////////////////////////////////////////////////////
/// @brief the prefix used to denote start of jSON list offset when generating
///        flat iResearch field names
////////////////////////////////////////////////////////////////////////////////
std::string const NESTING_LIST_OFFSET_PREFIX("[");

////////////////////////////////////////////////////////////////////////////////
/// @brief the suffix used to denote end of jSON list offset when generating
///        flat iResearch field names
////////////////////////////////////////////////////////////////////////////////
std::string const NESTING_LIST_OFFSET_SUFFIX("]");

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
    value = Swap8Bytes(value);
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
    arangodb::iresearch::IteratorValue const& value
) {
  buffer += NESTING_LIST_OFFSET_PREFIX;
  append(buffer, value.pos);
  buffer += NESTING_LIST_OFFSET_SUFFIX;

  return canHandleValue(value.value, *context);
}

inline bool inArray(
    std::string& /*buffer*/,
    arangodb::iresearch::IResearchLinkMeta const*& context,
    arangodb::iresearch::IteratorValue const& value
) noexcept {
  return canHandleValue(value.value, *context);
}

typedef bool(*Filter)(
  std::string& buffer,
  arangodb::iresearch::IResearchLinkMeta const*& context,
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

void mangleNull(std::string& name) {
  static irs::string_ref const SUFFIX("\0_n", 3);
  name.append(SUFFIX.c_str(), SUFFIX.size());
}

void setNullValue(
    VPackSlice const& value,
    std::string& name,
    arangodb::iresearch::Field& field
) {
  TRI_ASSERT(value.isNull());

  // mangle name
  mangleNull(name);

  // init stream
  auto stream = NullStreamPool.emplace();
  stream->reset();

  // set field properties
  field._name = name;
  field._tokenizer =  stream;
  field._features = &irs::flags::empty_instance();
}

void mangleBool(std::string& name) {
  static irs::string_ref const SUFFIX("\0_b", 3);
  name.append(SUFFIX.c_str(), SUFFIX.size());
}

void setBoolValue(
    VPackSlice const& value,
    std::string& name,
    arangodb::iresearch::Field& field
) {
  TRI_ASSERT(value.isBool());

  // mangle name
  mangleBool(name);

  // init stream
  auto stream = BoolStreamPool.emplace();
  stream->reset(value.getBool());

  // set field properties
  field._name = name;
  field._tokenizer =  stream;
  field._features = &irs::flags::empty_instance();
}

void mangleNumeric(std::string& name) {
  static irs::string_ref const SUFFIX("\0_d", 3);
  name.append(SUFFIX.c_str(), SUFFIX.size());
}

void setNumericValue(
    VPackSlice const& value,
    std::string& name,
    arangodb::iresearch::Field& field
) {
  TRI_ASSERT(value.isNumber());

  // mangle name
  mangleNumeric(name);

  // init stream
  auto stream = NumericStreamPool.emplace();
  stream->reset(value.getNumber<double>());

  // set field properties
  field._name = name;
  field._tokenizer =  stream;
  field._features = &NumericStreamFeatures;
}

void mangleStringField(
    std::string& name,
    TokenizerPoolPtr pool
) {
  name += '\0';
  name += pool->name();
  name += pool->args();
}

void unmangleStringField(
    std::string& name,
    TokenizerPoolPtr pool
) {
  // +1 for preceding '\0'
  auto const suffixSize = 1 + pool->name().size() + pool->args().size();

  TRI_ASSERT(name.size() >= suffixSize);
  name.resize(name.size() - suffixSize);
}

bool setStringValue(
    VPackSlice const& value,
    std::string& name,
    arangodb::iresearch::Field& field,
    TokenizerPoolPtr pool
) {
  TRI_ASSERT(value.isString());

  // it's important to unconditionally mangle name
  // since we unconditionally unmangle it in 'next'
  mangleStringField(name, pool);

  // init stream
  auto analyzer = pool->tokenizer();

  if (!analyzer) {
    LOG_TOPIC(WARN, arangodb::Logger::FIXME)
      << "got nullptr from tokenizer factory, name='"
      << pool->name() << "', args='"
      << pool->args() << "'";
    return false;
  }

  // init stream
  analyzer->reset(arangodb::iresearch::getStringRef(value));

  // set field properties
  field._name = name;
  field._tokenizer =  analyzer;
  field._features = &(pool->features());

  return true;
}

void setIdValue(
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

NS_END

NS_BEGIN(arangodb)
NS_BEGIN(iresearch)

// ----------------------------------------------------------------------------
// --SECTION--                                             Field implementation
// ----------------------------------------------------------------------------

/*static*/ void Field::setCidValue(Field& field, TRI_voc_cid_t& cid) {
  field._name = CID_FIELD;
  setIdValue(cid, *field._tokenizer);
  field._boost = 1.f;
  field._features = &irs::flags::empty_instance();
}

/*static*/ void Field::setCidValue(
    Field& field,
    TRI_voc_cid_t& cid,
    Field::init_stream_t
) {
  field._tokenizer = StringStreamPool.emplace();
  setCidValue(field, cid);
}

/*static*/ void Field::setRidValue(Field& field, TRI_voc_rid_t& rid) {
  field._name = RID_FIELD;
  setIdValue(rid, *field._tokenizer);
  field._boost = 1.f;
  field._features = &irs::flags::empty_instance();
}

/*static*/ void Field::setRidValue(
    Field& field,
    TRI_voc_rid_t& rid,
    Field::init_stream_t
) {
  field._tokenizer = StringStreamPool.emplace();
  setRidValue(field, rid);
}

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
// --SECTION--                                     FieldIterator implementation
// ----------------------------------------------------------------------------

/*static*/ FieldIterator const FieldIterator::END;

FieldIterator::FieldIterator(): _name(BufferPool.emplace()) {
  // initialize iterator's value
}

FieldIterator::FieldIterator(
    VPackSlice const& doc,
    IResearchLinkMeta const& linkMeta
): FieldIterator() {
  reset(doc, linkMeta);
}

void FieldIterator::reset(
    VPackSlice const& doc,
    IResearchLinkMeta const& linkMeta
) {
  // set surrogate tokenizers
  _begin = nullptr;
  _end = 1 + _begin;
  // clear stack
  _stack.clear();
  // clear field name
  _name->clear();

  if (!isArrayOrObject(doc)) {
    // can't handle plain objects
    return;
  }

  auto const* context = &linkMeta;

  // push the provided 'doc' to stack and initialize current value
  if (!push(doc, context) || !setValue(topValue().value, *context)) {
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
  while (it.next() && !filter(name, context, it.value())) {
    // filtered out
    name.resize(level.nameLength);
  }

  return context;
}

bool FieldIterator::push(VPackSlice slice, IResearchLinkMeta const*& context) {
  auto& name = nameBuffer();

  while (isArrayOrObject(slice)) {
    if (!name.empty() && !slice.isArray()) {
      name += NESTING_LEVEL_DELIMITER;
    }

    auto const filter = getFilter(slice, *context);

    _stack.emplace_back(slice, name.size(), *context, filter);

    auto& it = top().it;

    if (!it.valid()) {
      // empty object or array, skip it
      return false;
    }

    auto& value = it.value();

    if (!filter(name, context, value)) {
      // filtered out
      TRI_ASSERT(context);
      return false;
    }

    slice = value.value;
  }

  TRI_ASSERT(context);
  return true;
}

bool FieldIterator::setValue(
    VPackSlice const& value,
    IResearchLinkMeta const& context
) {
  _begin = nullptr;
  _end = 1 + _begin;               // set surrogate tokenizers
  _value._boost = context._boost;  // set boost

  switch (value.type()) {
    case VPackValueType::None:
    case VPackValueType::Illegal:
      return false;
    case VPackValueType::Null:
      setNullValue(value, nameBuffer(), _value);
      return true;
    case VPackValueType::Bool:
      setBoolValue(value, nameBuffer(), _value);
      return true;
    case VPackValueType::Array:
    case VPackValueType::Object:
      return true;
    case VPackValueType::Double:
      setNumericValue(value, nameBuffer(), _value);
      return true;
    case VPackValueType::UTCDate:
    case VPackValueType::External:
    case VPackValueType::MinKey:
    case VPackValueType::MaxKey:
      return false;
    case VPackValueType::Int:
    case VPackValueType::UInt:
    case VPackValueType::SmallInt:
      setNumericValue(value, nameBuffer(), _value);
      return true;
    case VPackValueType::String:
      resetTokenizers(context); // reset string tokenizers
      return setStringValue(value, nameBuffer(), _value, _begin);
    case VPackValueType::Binary:
    case VPackValueType::BCD:
    case VPackValueType::Custom:
    default:
      return false;
  }

  return false;
}

void FieldIterator::next() {
  TRI_ASSERT(valid());

  TokenizerIterator const prev = _begin;

  while (++_begin != _end) {
    auto& name = nameBuffer();

    // remove previous suffix
    unmangleStringField(name, prev);

    // can have multiple tokenizers for string values only
    if (setStringValue(topValue().value, name, _value, _begin)) {
      return;
    }
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

  } while (!push(topValue().value, context)
           || !setValue(topValue().value, *context));
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

bool DocumentPrimaryKey::read(irs::bytes_ref const& in) noexcept {
  if (sizeof(_keys) != in.size()) {
    return false;
  }

  std::memcpy(_keys, in.c_str(), sizeof(_keys));

  return true;
}

bool DocumentPrimaryKey::write(irs::data_output& out) const {
  out.write_bytes(
    reinterpret_cast<const irs::byte_type*>(_keys),
    sizeof(_keys)
  );

  return true;
}

// ----------------------------------------------------------------------------
// --SECTION--                                      FilerFactory implementation
// ----------------------------------------------------------------------------

/*static*/ irs::filter::ptr FilterFactory::filter(TRI_voc_cid_t cid) {
  auto filter = irs::by_term::make();

  // filter matching on cid
  static_cast<irs::by_term&>(*filter)
    .field(CID_FIELD) // set field
    .term(toBytesRefLE(cid)); // set value

  return std::move(filter);
}

/*static*/ irs::filter::ptr FilterFactory::filter(
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

template<bool Preorder, typename Visitor>
bool visit(arangodb::aql::AstNode const& root, Visitor visitor) {
  if (Preorder && !visitor(root)) {
    return false;
  }

  if (Preorder && !visitor(root)) {
    return false;
  }

  size_t const n = root.numMembers();

  for (size_t i = 0; i < n; ++i) {
    auto const* member = root.getMemberUnchecked(i);
    TRI_ASSERT(member);

    if (!visit<Preorder>(*member, visitor)) {
      return false;
    }
  }

  if (!Preorder && !visitor(root)) {
    return false;
  }

  return true;
};

inline irs::bytes_ref toBytesRef(arangodb::aql::AstNode const& node) {
  TRI_ASSERT(arangodb::aql::NODE_TYPE_VALUE == node.type
             && arangodb::aql::VALUE_TYPE_STRING == node.value.type);

  return irs::bytes_ref(
    reinterpret_cast<irs::byte_type const*>(node.getStringValue()),
    node.getStringLength()
  );
}

inline bool checkAttributeAccess(arangodb::aql::AstNode const& node) {
  return arangodb::aql::NODE_TYPE_ATTRIBUTE_ACCESS == node.type;
}

// generates field name from the specified 'node' and value 'type'
std::string nameFromAttributeAccess(
    arangodb::aql::AstNode const& node,
    arangodb::aql::AstNodeValueType type
) {
  TRI_ASSERT(arangodb::aql::NODE_TYPE_ATTRIBUTE_ACCESS == node.type);

  std::string name;

  auto visitor = [&name](arangodb::aql::AstNode const& node) mutable {
    if (arangodb::aql::NODE_TYPE_ATTRIBUTE_ACCESS == node.type) {
      name.append(node.getStringValue(), node.getStringLength());
      name += '.';
    }
    return true;
  };
  arangodb::iresearch::visit<false>(node, visitor);
  name.pop_back(); // remove extra '.'

  switch (type) {
    case arangodb::aql::VALUE_TYPE_NULL:
      mangleNull(name);
      break;
    case arangodb::aql::VALUE_TYPE_BOOL:
      mangleBool(name);
      break;
    case arangodb::aql::VALUE_TYPE_INT:
    case arangodb::aql::VALUE_TYPE_DOUBLE:
      mangleNumeric(name);
      break;
    case arangodb::aql::VALUE_TYPE_STRING:
      //mangleStringField(name);
      break;
  }

  return name;
}

bool processSubnode(
  irs::boolean_filter& rootFilter,
  arangodb::aql::AstNode const& node
);

template<typename Filter>
bool fromGroup(
  irs::boolean_filter& rootFilter,
  arangodb::aql::AstNode const& node
);

irs::by_term& byTerm(
    irs::by_term& filter,
    arangodb::aql::AstNode const& attributeNode,
    arangodb::aql::AstNode const& valueNode
) {
  switch (valueNode.value.type) {
    case arangodb::aql::VALUE_TYPE_NULL:
      filter.term(irs::null_token_stream::value_null());
      break;
    case arangodb::aql::VALUE_TYPE_BOOL:
      filter.term(valueNode.getBoolValue()
        ? irs::boolean_token_stream::value_true()
        : irs::boolean_token_stream::value_false());
      break;
    case arangodb::aql::VALUE_TYPE_INT:
    case arangodb::aql::VALUE_TYPE_DOUBLE: {
      irs::numeric_token_stream stream;
      irs::term_attribute const* term = stream.attributes().get<irs::term_attribute>();
      TRI_ASSERT(term);
      stream.reset(valueNode.getDoubleValue());
      stream.next();

      filter.term(term->value());
     } break;
    case arangodb::aql::VALUE_TYPE_STRING:
      filter.term(toBytesRef(valueNode));
      break;
  }

  filter.field(
    nameFromAttributeAccess(attributeNode, valueNode.value.type)
  );

  return filter;
}

template<irs::Bound Bound, bool Include>
irs::filter& byRange(
    irs::boolean_filter& rootFilter,
    arangodb::aql::AstNode const& attributeNode,
    arangodb::aql::AstNode const& valueNode
) {
  switch (valueNode.value.type) {
    case arangodb::aql::VALUE_TYPE_NULL: {
      auto& filter = rootFilter.add<irs::by_range>();
      filter.field(nameFromAttributeAccess(attributeNode, valueNode.value.type));
      filter.term<Bound>(irs::null_token_stream::value_null());
      filter.include<Bound>(Include);
      return filter;
    }
    case arangodb::aql::VALUE_TYPE_BOOL: {
      auto& filter = rootFilter.add<irs::by_range>();

      auto const& value = valueNode.getBoolValue()
          ? irs::boolean_token_stream::value_true()
          : irs::boolean_token_stream::value_false();

      filter.field(nameFromAttributeAccess(attributeNode, valueNode.value.type));
      filter.term<Bound>(value);
      filter.include<Bound>(Include);
      return filter;
    }
    case arangodb::aql::VALUE_TYPE_INT:
    case arangodb::aql::VALUE_TYPE_DOUBLE: {
      auto& filter = rootFilter.add<irs::by_granular_range>();
      filter.field(nameFromAttributeAccess(attributeNode, valueNode.value.type));
      filter.include<Bound>(Include);

      irs::numeric_token_stream stream;
      stream.reset(valueNode.getDoubleValue());
      filter.insert<Bound>(stream);
      return filter;
    }
    case arangodb::aql::VALUE_TYPE_STRING: {
      auto& filter = rootFilter.add<irs::by_range>();
      filter.field(nameFromAttributeAccess(attributeNode, valueNode.value.type));
      filter.term<Bound>(toBytesRef(valueNode));
      filter.include<Bound>(Include);
      return filter;
    }
  }

  TRI_ASSERT(false);
  return rootFilter;
}

template<irs::Bound Bound, bool Include>
bool fromInterval(
    irs::boolean_filter& rootFilter,
    arangodb::aql::AstNode const& node
) {
  TRI_ASSERT(
    2 == node.numMembers()
    && ((arangodb::aql::NODE_TYPE_OPERATOR_BINARY_LT == node.type && irs::Bound::MAX == Bound && !Include)
     || (arangodb::aql::NODE_TYPE_OPERATOR_BINARY_LE == node.type && irs::Bound::MAX == Bound && Include)
     || (arangodb::aql::NODE_TYPE_OPERATOR_BINARY_GT == node.type && irs::Bound::MIN == Bound && !Include)
     || (arangodb::aql::NODE_TYPE_OPERATOR_BINARY_GE == node.type && irs::Bound::MIN == Bound && Include))
  );

  auto const* attributeNode = node.getMemberUnchecked(0);
  TRI_ASSERT(attributeNode);

  if (!checkAttributeAccess(*attributeNode)) {
    return false;
  }

  auto const* valueNode = node.getMemberUnchecked(1);
  TRI_ASSERT(attributeNode);

  if (!valueNode->isConstant()) {
    return false;
  }

  byRange<Bound, Include>(rootFilter, *attributeNode, *valueNode);
  return true;
}

bool fromBinaryEq(
    irs::boolean_filter& rootFilter,
    arangodb::aql::AstNode const& node
) {
  TRI_ASSERT(
    2 == node.numMembers()
    && (arangodb::aql::NODE_TYPE_OPERATOR_BINARY_EQ == node.type
    || arangodb::aql::NODE_TYPE_OPERATOR_BINARY_NE == node.type));

  auto* attributeNode = node.getMemberUnchecked(0);
  TRI_ASSERT(attributeNode);

  if (!checkAttributeAccess(*attributeNode)) {
    return false;
  }

  auto* valueNode = node.getMemberUnchecked(1);
  TRI_ASSERT(attributeNode);

  if (!valueNode->isConstant()) {
    return false;
  }

  auto& filter =  arangodb::aql::NODE_TYPE_OPERATOR_BINARY_NE == node.type
    ? rootFilter.add<irs::Not>().filter<irs::by_term>()
    : rootFilter.add<irs::by_term>();

  byTerm(filter, *attributeNode, *valueNode);
  return true;
}

bool fromRange(
    irs::boolean_filter& rootFilter,
    arangodb::aql::AstNode const& node
) {
  TRI_ASSERT(2 == node.numMembers() && arangodb::aql::NODE_TYPE_RANGE == node.type);

  auto* min = node.getMemberUnchecked(0);
  TRI_ASSERT(min);
  auto* max = node.getMemberUnchecked(1);
  TRI_ASSERT(max);

  if (min->value.type != max->value.type) {
    // type mismatch
    return false;
  }

  //FIXME TODO
  return false;
}

bool fromArrayIn(
    irs::boolean_filter& rootFilter,
    arangodb::aql::AstNode const& node
) {
  TRI_ASSERT(
    2 == node.numMembers()
    && (arangodb::aql::NODE_TYPE_OPERATOR_BINARY_IN == node.type
    || arangodb::aql::NODE_TYPE_OPERATOR_BINARY_NIN == node.type));

  auto* attributeNode = node.getMemberUnchecked(0);
  TRI_ASSERT(attributeNode);

  if (!checkAttributeAccess(*attributeNode)) {
    return false;
  }

  auto* valueNode = node.getMemberUnchecked(1);
  TRI_ASSERT(valueNode);

  size_t const n = valueNode->numMembers();

  if (!n) {
    // nothing to do
    return true;
  }

  irs::boolean_filter& filter = arangodb::aql::NODE_TYPE_OPERATOR_BINARY_NIN == node.type
    ? static_cast<irs::boolean_filter&>(rootFilter.add<irs::Not>().filter<irs::And>())
    : static_cast<irs::boolean_filter&>(rootFilter.add<irs::Or>());

  for (size_t i = 0; i < n; ++i) {
    auto const* elementNode = valueNode->getMemberUnchecked(i);
    TRI_ASSERT(valueNode);

    if (elementNode->type != arangodb::aql::NODE_TYPE_VALUE
        || !elementNode->isConstant()) {
      return false;
    }

    byTerm(filter.add<irs::by_term>(), *attributeNode, *elementNode);
  }

  return true;
}

bool fromValue(
    irs::boolean_filter& rootFilter,
    arangodb::aql::AstNode const& node
) {
  TRI_ASSERT(arangodb::aql::NODE_TYPE_VALUE == node.type
             || arangodb::aql::NODE_TYPE_ARRAY == node.type);

  if (node.isTrue()) {
    rootFilter.add<irs::all>();
  } else {
    // FIXME empty query
    rootFilter.add<irs::Not>();
  }

  return true;
}

bool fromNegation(
    irs::boolean_filter& rootFilter,
    arangodb::aql::AstNode const& node
) {
  TRI_ASSERT(
    1 == node.numMembers()
    && arangodb::aql::NODE_TYPE_OPERATOR_UNARY_NOT == node.type
  );

  auto const* member = node.getMemberUnchecked(0);
  TRI_ASSERT(member);

  auto& filter = rootFilter.add<irs::Not>().filter<irs::And>();

  return processSubnode(filter, *member);
}

bool fromBinaryAnd(
    irs::boolean_filter& rootFilter,
    arangodb::aql::AstNode const& node
) {
  TRI_ASSERT(arangodb::aql::NODE_TYPE_OPERATOR_BINARY_AND == node.type);
  TRI_ASSERT(2 == node.numMembers());

  auto* lhs = node.getMemberUnchecked(0);
  TRI_ASSERT(lhs);
  auto* rhs = node.getMemberUnchecked(1);
  TRI_ASSERT(rhs);

  bool const includeMin = arangodb::aql::NODE_TYPE_OPERATOR_BINARY_GE == lhs->type;
  bool const includeMax = arangodb::aql::NODE_TYPE_OPERATOR_BINARY_LE == lhs->type;

  if ((includeMin || arangodb::aql::NODE_TYPE_OPERATOR_BINARY_GT == lhs->type)
      && (includeMax || arangodb::aql::NODE_TYPE_OPERATOR_BINARY_LT == lhs->type)) {
    TRI_ASSERT(2 == lhs->numMembers());
    auto const* lhsAttribute = lhs->getMemberUnchecked(0);
    auto const* lhsValue = lhs->getMemberUnchecked(1);
    TRI_ASSERT(2 == rhs->numMembers());
    auto const* rhsAttribute = rhs->getMemberUnchecked(0);
    auto const* rhsValue = rhs->getMemberUnchecked(1);

    if (lhsValue->value.type == rhsValue->value.type) {

    }
    // FIXME range case
  }

  // treat as ordinal 'And'
  return fromGroup<irs::And>(rootFilter, node);
}

template<typename Filter>
bool fromGroup(
    irs::boolean_filter& rootFilter,
    arangodb::aql::AstNode const& node
) {
  TRI_ASSERT(arangodb::aql::NODE_TYPE_OPERATOR_BINARY_AND == node.type
   || arangodb::aql::NODE_TYPE_OPERATOR_BINARY_OR == node.type
   || arangodb::aql::NODE_TYPE_OPERATOR_NARY_AND == node.type
   || arangodb::aql::NODE_TYPE_OPERATOR_NARY_OR == node.type);

  size_t const n = node.numMembers();

  if (!n) {
    // nothing to do
    return true;
  }

  auto& filter = rootFilter.add<Filter>();

  for (size_t i = 0; i < n; ++i) {
    auto const* valueNode = node.getMemberUnchecked(i);
    TRI_ASSERT(valueNode);

    if (!processSubnode(filter, *valueNode)) {
      return false;
    }
  }

  return true;
}

bool processSubnode(
    irs::boolean_filter& rootFilter,
    arangodb::aql::AstNode const& node
) {
  switch (node.type) {
    case arangodb::aql::NODE_TYPE_OPERATOR_UNARY_NOT: // unary minus
      return fromNegation(rootFilter, node);
    case arangodb::aql::NODE_TYPE_OPERATOR_BINARY_AND: // logical and
      return fromBinaryAnd(rootFilter, node);
    case arangodb::aql::NODE_TYPE_OPERATOR_BINARY_OR: // logical or
      return fromGroup<irs::Or>(rootFilter, node);
    case arangodb::aql::NODE_TYPE_OPERATOR_BINARY_EQ: // compare ==
    case arangodb::aql::NODE_TYPE_OPERATOR_BINARY_NE: // compare !=
      return fromBinaryEq(rootFilter, node);
    case arangodb::aql::NODE_TYPE_OPERATOR_BINARY_LT: // compare <
      return fromInterval<irs::Bound::MAX, false>(rootFilter, node);
    case arangodb::aql::NODE_TYPE_OPERATOR_BINARY_LE: // compare <=
      return fromInterval<irs::Bound::MAX, true>(rootFilter, node);
    case arangodb::aql::NODE_TYPE_OPERATOR_BINARY_GT: // compare >
      return fromInterval<irs::Bound::MIN, false>(rootFilter, node);
    case arangodb::aql::NODE_TYPE_OPERATOR_BINARY_GE: // compare >=
      return fromInterval<irs::Bound::MIN, true>(rootFilter, node);
    case arangodb::aql::NODE_TYPE_OPERATOR_BINARY_IN: // compare in
    case arangodb::aql::NODE_TYPE_OPERATOR_BINARY_NIN: // compare not in
      return fromArrayIn(rootFilter, node);
    case arangodb::aql::NODE_TYPE_OPERATOR_TERNARY: // ternary
      break;
    case arangodb::aql::NODE_TYPE_VALUE : // value
    case arangodb::aql::NODE_TYPE_ARRAY: // array
      return fromValue(rootFilter, node);
    case arangodb::aql::NODE_TYPE_FCALL: // function call
      // FIXME TODO
      break;
    case arangodb::aql::NODE_TYPE_FCALL_USER: // user function call
      // FIXME TODO
      break;
    case arangodb::aql::NODE_TYPE_RANGE: // range
      return fromRange(rootFilter, node);
    case arangodb::aql::NODE_TYPE_OPERATOR_NARY_AND: // n-ary and
      return fromGroup<irs::And>(rootFilter, node);
    case arangodb::aql::NODE_TYPE_OPERATOR_NARY_OR: // n-ary or
      return fromGroup<irs::Or>(rootFilter, node);
    default:
      break;
  }

  // unsupported type
  return false;
}

/*static*/ bool FilterFactory::filter(
    irs::boolean_filter& rootFilter,
    arangodb::aql::AstNode const& root
) {
  if (arangodb::aql::NODE_TYPE_FILTER != root.type) {
    // wrong root node type
    return false;
  }

  if (1 != root.numMembers()) {
    //. wrong number of members
    return false;
  }

  auto const* member = root.getMemberUnchecked(0);

  return member && processSubnode(rootFilter, *member);
}

NS_END // iresearch
NS_END // arangodb

// -----------------------------------------------------------------------------
// --SECTION--                                                       END-OF-FILE
// -----------------------------------------------------------------------------
