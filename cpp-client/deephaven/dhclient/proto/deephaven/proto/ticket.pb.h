// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: deephaven/proto/ticket.proto
// Protobuf C++ Version: 4.25.3

#ifndef GOOGLE_PROTOBUF_INCLUDED_deephaven_2fproto_2fticket_2eproto_2epb_2eh
#define GOOGLE_PROTOBUF_INCLUDED_deephaven_2fproto_2fticket_2eproto_2epb_2eh

#include <limits>
#include <string>
#include <type_traits>
#include <utility>

#include "google/protobuf/port_def.inc"
#if PROTOBUF_VERSION < 4025000
#error "This file was generated by a newer version of protoc which is"
#error "incompatible with your Protocol Buffer headers. Please update"
#error "your headers."
#endif  // PROTOBUF_VERSION

#if 4025003 < PROTOBUF_MIN_PROTOC_VERSION
#error "This file was generated by an older version of protoc which is"
#error "incompatible with your Protocol Buffer headers. Please"
#error "regenerate this file with a newer version of protoc."
#endif  // PROTOBUF_MIN_PROTOC_VERSION
#include "google/protobuf/port_undef.inc"
#include "google/protobuf/io/coded_stream.h"
#include "google/protobuf/arena.h"
#include "google/protobuf/arenastring.h"
#include "google/protobuf/generated_message_tctable_decl.h"
#include "google/protobuf/generated_message_util.h"
#include "google/protobuf/metadata_lite.h"
#include "google/protobuf/generated_message_reflection.h"
#include "google/protobuf/message.h"
#include "google/protobuf/repeated_field.h"  // IWYU pragma: export
#include "google/protobuf/extension_set.h"  // IWYU pragma: export
#include "google/protobuf/unknown_field_set.h"
// @@protoc_insertion_point(includes)

// Must be included last.
#include "google/protobuf/port_def.inc"

#define PROTOBUF_INTERNAL_EXPORT_deephaven_2fproto_2fticket_2eproto

namespace google {
namespace protobuf {
namespace internal {
class AnyMetadata;
}  // namespace internal
}  // namespace protobuf
}  // namespace google

// Internal implementation detail -- do not use these members.
struct TableStruct_deephaven_2fproto_2fticket_2eproto {
  static const ::uint32_t offsets[];
};
extern const ::google::protobuf::internal::DescriptorTable
    descriptor_table_deephaven_2fproto_2fticket_2eproto;
namespace io {
namespace deephaven {
namespace proto {
namespace backplane {
namespace grpc {
class Ticket;
struct TicketDefaultTypeInternal;
extern TicketDefaultTypeInternal _Ticket_default_instance_;
class TypedTicket;
struct TypedTicketDefaultTypeInternal;
extern TypedTicketDefaultTypeInternal _TypedTicket_default_instance_;
}  // namespace grpc
}  // namespace backplane
}  // namespace proto
}  // namespace deephaven
}  // namespace io
namespace google {
namespace protobuf {
}  // namespace protobuf
}  // namespace google

namespace io {
namespace deephaven {
namespace proto {
namespace backplane {
namespace grpc {

// ===================================================================


// -------------------------------------------------------------------

class Ticket final :
    public ::google::protobuf::Message /* @@protoc_insertion_point(class_definition:io.deephaven.proto.backplane.grpc.Ticket) */ {
 public:
  inline Ticket() : Ticket(nullptr) {}
  ~Ticket() override;
  template<typename = void>
  explicit PROTOBUF_CONSTEXPR Ticket(::google::protobuf::internal::ConstantInitialized);

  inline Ticket(const Ticket& from)
      : Ticket(nullptr, from) {}
  Ticket(Ticket&& from) noexcept
    : Ticket() {
    *this = ::std::move(from);
  }

  inline Ticket& operator=(const Ticket& from) {
    CopyFrom(from);
    return *this;
  }
  inline Ticket& operator=(Ticket&& from) noexcept {
    if (this == &from) return *this;
    if (GetArena() == from.GetArena()
  #ifdef PROTOBUF_FORCE_COPY_IN_MOVE
        && GetArena() != nullptr
  #endif  // !PROTOBUF_FORCE_COPY_IN_MOVE
    ) {
      InternalSwap(&from);
    } else {
      CopyFrom(from);
    }
    return *this;
  }

  inline const ::google::protobuf::UnknownFieldSet& unknown_fields() const
      ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return _internal_metadata_.unknown_fields<::google::protobuf::UnknownFieldSet>(::google::protobuf::UnknownFieldSet::default_instance);
  }
  inline ::google::protobuf::UnknownFieldSet* mutable_unknown_fields()
      ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return _internal_metadata_.mutable_unknown_fields<::google::protobuf::UnknownFieldSet>();
  }

  static const ::google::protobuf::Descriptor* descriptor() {
    return GetDescriptor();
  }
  static const ::google::protobuf::Descriptor* GetDescriptor() {
    return default_instance().GetMetadata().descriptor;
  }
  static const ::google::protobuf::Reflection* GetReflection() {
    return default_instance().GetMetadata().reflection;
  }
  static const Ticket& default_instance() {
    return *internal_default_instance();
  }
  static inline const Ticket* internal_default_instance() {
    return reinterpret_cast<const Ticket*>(
               &_Ticket_default_instance_);
  }
  static constexpr int kIndexInFileMessages =
    0;

  friend void swap(Ticket& a, Ticket& b) {
    a.Swap(&b);
  }
  inline void Swap(Ticket* other) {
    if (other == this) return;
  #ifdef PROTOBUF_FORCE_COPY_IN_SWAP
    if (GetArena() != nullptr &&
        GetArena() == other->GetArena()) {
   #else  // PROTOBUF_FORCE_COPY_IN_SWAP
    if (GetArena() == other->GetArena()) {
  #endif  // !PROTOBUF_FORCE_COPY_IN_SWAP
      InternalSwap(other);
    } else {
      ::google::protobuf::internal::GenericSwap(this, other);
    }
  }
  void UnsafeArenaSwap(Ticket* other) {
    if (other == this) return;
    ABSL_DCHECK(GetArena() == other->GetArena());
    InternalSwap(other);
  }

  // implements Message ----------------------------------------------

  Ticket* New(::google::protobuf::Arena* arena = nullptr) const final {
    return CreateMaybeMessage<Ticket>(arena);
  }
  using ::google::protobuf::Message::CopyFrom;
  void CopyFrom(const Ticket& from);
  using ::google::protobuf::Message::MergeFrom;
  void MergeFrom( const Ticket& from) {
    Ticket::MergeImpl(*this, from);
  }
  private:
  static void MergeImpl(::google::protobuf::Message& to_msg, const ::google::protobuf::Message& from_msg);
  public:
  PROTOBUF_ATTRIBUTE_REINITIALIZES void Clear() final;
  bool IsInitialized() const final;

  ::size_t ByteSizeLong() const final;
  const char* _InternalParse(const char* ptr, ::google::protobuf::internal::ParseContext* ctx) final;
  ::uint8_t* _InternalSerialize(
      ::uint8_t* target, ::google::protobuf::io::EpsCopyOutputStream* stream) const final;
  int GetCachedSize() const { return _impl_._cached_size_.Get(); }

  private:
  ::google::protobuf::internal::CachedSize* AccessCachedSize() const final;
  void SharedCtor(::google::protobuf::Arena* arena);
  void SharedDtor();
  void InternalSwap(Ticket* other);

  private:
  friend class ::google::protobuf::internal::AnyMetadata;
  static ::absl::string_view FullMessageName() {
    return "io.deephaven.proto.backplane.grpc.Ticket";
  }
  protected:
  explicit Ticket(::google::protobuf::Arena* arena);
  Ticket(::google::protobuf::Arena* arena, const Ticket& from);
  public:

  static const ClassData _class_data_;
  const ::google::protobuf::Message::ClassData*GetClassData() const final;

  ::google::protobuf::Metadata GetMetadata() const final;

  // nested types ----------------------------------------------------

  // accessors -------------------------------------------------------

  enum : int {
    kTicketFieldNumber = 1,
  };
  // bytes ticket = 1;
  void clear_ticket() ;
  const std::string& ticket() const;
  template <typename Arg_ = const std::string&, typename... Args_>
  void set_ticket(Arg_&& arg, Args_... args);
  std::string* mutable_ticket();
  PROTOBUF_NODISCARD std::string* release_ticket();
  void set_allocated_ticket(std::string* value);

  private:
  const std::string& _internal_ticket() const;
  inline PROTOBUF_ALWAYS_INLINE void _internal_set_ticket(
      const std::string& value);
  std::string* _internal_mutable_ticket();

  public:
  // @@protoc_insertion_point(class_scope:io.deephaven.proto.backplane.grpc.Ticket)
 private:
  class _Internal;

  friend class ::google::protobuf::internal::TcParser;
  static const ::google::protobuf::internal::TcParseTable<
      0, 1, 0,
      0, 2>
      _table_;
  friend class ::google::protobuf::MessageLite;
  friend class ::google::protobuf::Arena;
  template <typename T>
  friend class ::google::protobuf::Arena::InternalHelper;
  using InternalArenaConstructable_ = void;
  using DestructorSkippable_ = void;
  struct Impl_ {

        inline explicit constexpr Impl_(
            ::google::protobuf::internal::ConstantInitialized) noexcept;
        inline explicit Impl_(::google::protobuf::internal::InternalVisibility visibility,
                              ::google::protobuf::Arena* arena);
        inline explicit Impl_(::google::protobuf::internal::InternalVisibility visibility,
                              ::google::protobuf::Arena* arena, const Impl_& from);
    ::google::protobuf::internal::ArenaStringPtr ticket_;
    mutable ::google::protobuf::internal::CachedSize _cached_size_;
    PROTOBUF_TSAN_DECLARE_MEMBER
  };
  union { Impl_ _impl_; };
  friend struct ::TableStruct_deephaven_2fproto_2fticket_2eproto;
};// -------------------------------------------------------------------

class TypedTicket final :
    public ::google::protobuf::Message /* @@protoc_insertion_point(class_definition:io.deephaven.proto.backplane.grpc.TypedTicket) */ {
 public:
  inline TypedTicket() : TypedTicket(nullptr) {}
  ~TypedTicket() override;
  template<typename = void>
  explicit PROTOBUF_CONSTEXPR TypedTicket(::google::protobuf::internal::ConstantInitialized);

  inline TypedTicket(const TypedTicket& from)
      : TypedTicket(nullptr, from) {}
  TypedTicket(TypedTicket&& from) noexcept
    : TypedTicket() {
    *this = ::std::move(from);
  }

  inline TypedTicket& operator=(const TypedTicket& from) {
    CopyFrom(from);
    return *this;
  }
  inline TypedTicket& operator=(TypedTicket&& from) noexcept {
    if (this == &from) return *this;
    if (GetArena() == from.GetArena()
  #ifdef PROTOBUF_FORCE_COPY_IN_MOVE
        && GetArena() != nullptr
  #endif  // !PROTOBUF_FORCE_COPY_IN_MOVE
    ) {
      InternalSwap(&from);
    } else {
      CopyFrom(from);
    }
    return *this;
  }

  inline const ::google::protobuf::UnknownFieldSet& unknown_fields() const
      ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return _internal_metadata_.unknown_fields<::google::protobuf::UnknownFieldSet>(::google::protobuf::UnknownFieldSet::default_instance);
  }
  inline ::google::protobuf::UnknownFieldSet* mutable_unknown_fields()
      ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return _internal_metadata_.mutable_unknown_fields<::google::protobuf::UnknownFieldSet>();
  }

  static const ::google::protobuf::Descriptor* descriptor() {
    return GetDescriptor();
  }
  static const ::google::protobuf::Descriptor* GetDescriptor() {
    return default_instance().GetMetadata().descriptor;
  }
  static const ::google::protobuf::Reflection* GetReflection() {
    return default_instance().GetMetadata().reflection;
  }
  static const TypedTicket& default_instance() {
    return *internal_default_instance();
  }
  static inline const TypedTicket* internal_default_instance() {
    return reinterpret_cast<const TypedTicket*>(
               &_TypedTicket_default_instance_);
  }
  static constexpr int kIndexInFileMessages =
    1;

  friend void swap(TypedTicket& a, TypedTicket& b) {
    a.Swap(&b);
  }
  inline void Swap(TypedTicket* other) {
    if (other == this) return;
  #ifdef PROTOBUF_FORCE_COPY_IN_SWAP
    if (GetArena() != nullptr &&
        GetArena() == other->GetArena()) {
   #else  // PROTOBUF_FORCE_COPY_IN_SWAP
    if (GetArena() == other->GetArena()) {
  #endif  // !PROTOBUF_FORCE_COPY_IN_SWAP
      InternalSwap(other);
    } else {
      ::google::protobuf::internal::GenericSwap(this, other);
    }
  }
  void UnsafeArenaSwap(TypedTicket* other) {
    if (other == this) return;
    ABSL_DCHECK(GetArena() == other->GetArena());
    InternalSwap(other);
  }

  // implements Message ----------------------------------------------

  TypedTicket* New(::google::protobuf::Arena* arena = nullptr) const final {
    return CreateMaybeMessage<TypedTicket>(arena);
  }
  using ::google::protobuf::Message::CopyFrom;
  void CopyFrom(const TypedTicket& from);
  using ::google::protobuf::Message::MergeFrom;
  void MergeFrom( const TypedTicket& from) {
    TypedTicket::MergeImpl(*this, from);
  }
  private:
  static void MergeImpl(::google::protobuf::Message& to_msg, const ::google::protobuf::Message& from_msg);
  public:
  PROTOBUF_ATTRIBUTE_REINITIALIZES void Clear() final;
  bool IsInitialized() const final;

  ::size_t ByteSizeLong() const final;
  const char* _InternalParse(const char* ptr, ::google::protobuf::internal::ParseContext* ctx) final;
  ::uint8_t* _InternalSerialize(
      ::uint8_t* target, ::google::protobuf::io::EpsCopyOutputStream* stream) const final;
  int GetCachedSize() const { return _impl_._cached_size_.Get(); }

  private:
  ::google::protobuf::internal::CachedSize* AccessCachedSize() const final;
  void SharedCtor(::google::protobuf::Arena* arena);
  void SharedDtor();
  void InternalSwap(TypedTicket* other);

  private:
  friend class ::google::protobuf::internal::AnyMetadata;
  static ::absl::string_view FullMessageName() {
    return "io.deephaven.proto.backplane.grpc.TypedTicket";
  }
  protected:
  explicit TypedTicket(::google::protobuf::Arena* arena);
  TypedTicket(::google::protobuf::Arena* arena, const TypedTicket& from);
  public:

  static const ClassData _class_data_;
  const ::google::protobuf::Message::ClassData*GetClassData() const final;

  ::google::protobuf::Metadata GetMetadata() const final;

  // nested types ----------------------------------------------------

  // accessors -------------------------------------------------------

  enum : int {
    kTypeFieldNumber = 2,
    kTicketFieldNumber = 1,
  };
  // string type = 2;
  void clear_type() ;
  const std::string& type() const;
  template <typename Arg_ = const std::string&, typename... Args_>
  void set_type(Arg_&& arg, Args_... args);
  std::string* mutable_type();
  PROTOBUF_NODISCARD std::string* release_type();
  void set_allocated_type(std::string* value);

  private:
  const std::string& _internal_type() const;
  inline PROTOBUF_ALWAYS_INLINE void _internal_set_type(
      const std::string& value);
  std::string* _internal_mutable_type();

  public:
  // .io.deephaven.proto.backplane.grpc.Ticket ticket = 1;
  bool has_ticket() const;
  void clear_ticket() ;
  const ::io::deephaven::proto::backplane::grpc::Ticket& ticket() const;
  PROTOBUF_NODISCARD ::io::deephaven::proto::backplane::grpc::Ticket* release_ticket();
  ::io::deephaven::proto::backplane::grpc::Ticket* mutable_ticket();
  void set_allocated_ticket(::io::deephaven::proto::backplane::grpc::Ticket* value);
  void unsafe_arena_set_allocated_ticket(::io::deephaven::proto::backplane::grpc::Ticket* value);
  ::io::deephaven::proto::backplane::grpc::Ticket* unsafe_arena_release_ticket();

  private:
  const ::io::deephaven::proto::backplane::grpc::Ticket& _internal_ticket() const;
  ::io::deephaven::proto::backplane::grpc::Ticket* _internal_mutable_ticket();

  public:
  // @@protoc_insertion_point(class_scope:io.deephaven.proto.backplane.grpc.TypedTicket)
 private:
  class _Internal;

  friend class ::google::protobuf::internal::TcParser;
  static const ::google::protobuf::internal::TcParseTable<
      1, 2, 1,
      58, 2>
      _table_;
  friend class ::google::protobuf::MessageLite;
  friend class ::google::protobuf::Arena;
  template <typename T>
  friend class ::google::protobuf::Arena::InternalHelper;
  using InternalArenaConstructable_ = void;
  using DestructorSkippable_ = void;
  struct Impl_ {

        inline explicit constexpr Impl_(
            ::google::protobuf::internal::ConstantInitialized) noexcept;
        inline explicit Impl_(::google::protobuf::internal::InternalVisibility visibility,
                              ::google::protobuf::Arena* arena);
        inline explicit Impl_(::google::protobuf::internal::InternalVisibility visibility,
                              ::google::protobuf::Arena* arena, const Impl_& from);
    ::google::protobuf::internal::HasBits<1> _has_bits_;
    mutable ::google::protobuf::internal::CachedSize _cached_size_;
    ::google::protobuf::internal::ArenaStringPtr type_;
    ::io::deephaven::proto::backplane::grpc::Ticket* ticket_;
    PROTOBUF_TSAN_DECLARE_MEMBER
  };
  union { Impl_ _impl_; };
  friend struct ::TableStruct_deephaven_2fproto_2fticket_2eproto;
};

// ===================================================================




// ===================================================================


#ifdef __GNUC__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wstrict-aliasing"
#endif  // __GNUC__
// -------------------------------------------------------------------

// Ticket

// bytes ticket = 1;
inline void Ticket::clear_ticket() {
  PROTOBUF_TSAN_WRITE(&_impl_._tsan_detect_race);
  _impl_.ticket_.ClearToEmpty();
}
inline const std::string& Ticket::ticket() const
    ABSL_ATTRIBUTE_LIFETIME_BOUND {
  // @@protoc_insertion_point(field_get:io.deephaven.proto.backplane.grpc.Ticket.ticket)
  return _internal_ticket();
}
template <typename Arg_, typename... Args_>
inline PROTOBUF_ALWAYS_INLINE void Ticket::set_ticket(Arg_&& arg,
                                                     Args_... args) {
  PROTOBUF_TSAN_WRITE(&_impl_._tsan_detect_race);
  ;
  _impl_.ticket_.SetBytes(static_cast<Arg_&&>(arg), args..., GetArena());
  // @@protoc_insertion_point(field_set:io.deephaven.proto.backplane.grpc.Ticket.ticket)
}
inline std::string* Ticket::mutable_ticket() ABSL_ATTRIBUTE_LIFETIME_BOUND {
  std::string* _s = _internal_mutable_ticket();
  // @@protoc_insertion_point(field_mutable:io.deephaven.proto.backplane.grpc.Ticket.ticket)
  return _s;
}
inline const std::string& Ticket::_internal_ticket() const {
  PROTOBUF_TSAN_READ(&_impl_._tsan_detect_race);
  return _impl_.ticket_.Get();
}
inline void Ticket::_internal_set_ticket(const std::string& value) {
  PROTOBUF_TSAN_WRITE(&_impl_._tsan_detect_race);
  ;
  _impl_.ticket_.Set(value, GetArena());
}
inline std::string* Ticket::_internal_mutable_ticket() {
  PROTOBUF_TSAN_WRITE(&_impl_._tsan_detect_race);
  ;
  return _impl_.ticket_.Mutable( GetArena());
}
inline std::string* Ticket::release_ticket() {
  PROTOBUF_TSAN_WRITE(&_impl_._tsan_detect_race);
  // @@protoc_insertion_point(field_release:io.deephaven.proto.backplane.grpc.Ticket.ticket)
  return _impl_.ticket_.Release();
}
inline void Ticket::set_allocated_ticket(std::string* value) {
  PROTOBUF_TSAN_WRITE(&_impl_._tsan_detect_race);
  _impl_.ticket_.SetAllocated(value, GetArena());
  #ifdef PROTOBUF_FORCE_COPY_DEFAULT_STRING
        if (_impl_.ticket_.IsDefault()) {
          _impl_.ticket_.Set("", GetArena());
        }
  #endif  // PROTOBUF_FORCE_COPY_DEFAULT_STRING
  // @@protoc_insertion_point(field_set_allocated:io.deephaven.proto.backplane.grpc.Ticket.ticket)
}

// -------------------------------------------------------------------

// TypedTicket

// .io.deephaven.proto.backplane.grpc.Ticket ticket = 1;
inline bool TypedTicket::has_ticket() const {
  bool value = (_impl_._has_bits_[0] & 0x00000001u) != 0;
  PROTOBUF_ASSUME(!value || _impl_.ticket_ != nullptr);
  return value;
}
inline void TypedTicket::clear_ticket() {
  PROTOBUF_TSAN_WRITE(&_impl_._tsan_detect_race);
  if (_impl_.ticket_ != nullptr) _impl_.ticket_->Clear();
  _impl_._has_bits_[0] &= ~0x00000001u;
}
inline const ::io::deephaven::proto::backplane::grpc::Ticket& TypedTicket::_internal_ticket() const {
  PROTOBUF_TSAN_READ(&_impl_._tsan_detect_race);
  const ::io::deephaven::proto::backplane::grpc::Ticket* p = _impl_.ticket_;
  return p != nullptr ? *p : reinterpret_cast<const ::io::deephaven::proto::backplane::grpc::Ticket&>(::io::deephaven::proto::backplane::grpc::_Ticket_default_instance_);
}
inline const ::io::deephaven::proto::backplane::grpc::Ticket& TypedTicket::ticket() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
  // @@protoc_insertion_point(field_get:io.deephaven.proto.backplane.grpc.TypedTicket.ticket)
  return _internal_ticket();
}
inline void TypedTicket::unsafe_arena_set_allocated_ticket(::io::deephaven::proto::backplane::grpc::Ticket* value) {
  PROTOBUF_TSAN_WRITE(&_impl_._tsan_detect_race);
  if (GetArena() == nullptr) {
    delete reinterpret_cast<::google::protobuf::MessageLite*>(_impl_.ticket_);
  }
  _impl_.ticket_ = reinterpret_cast<::io::deephaven::proto::backplane::grpc::Ticket*>(value);
  if (value != nullptr) {
    _impl_._has_bits_[0] |= 0x00000001u;
  } else {
    _impl_._has_bits_[0] &= ~0x00000001u;
  }
  // @@protoc_insertion_point(field_unsafe_arena_set_allocated:io.deephaven.proto.backplane.grpc.TypedTicket.ticket)
}
inline ::io::deephaven::proto::backplane::grpc::Ticket* TypedTicket::release_ticket() {
  PROTOBUF_TSAN_WRITE(&_impl_._tsan_detect_race);

  _impl_._has_bits_[0] &= ~0x00000001u;
  ::io::deephaven::proto::backplane::grpc::Ticket* released = _impl_.ticket_;
  _impl_.ticket_ = nullptr;
#ifdef PROTOBUF_FORCE_COPY_IN_RELEASE
  auto* old = reinterpret_cast<::google::protobuf::MessageLite*>(released);
  released = ::google::protobuf::internal::DuplicateIfNonNull(released);
  if (GetArena() == nullptr) {
    delete old;
  }
#else   // PROTOBUF_FORCE_COPY_IN_RELEASE
  if (GetArena() != nullptr) {
    released = ::google::protobuf::internal::DuplicateIfNonNull(released);
  }
#endif  // !PROTOBUF_FORCE_COPY_IN_RELEASE
  return released;
}
inline ::io::deephaven::proto::backplane::grpc::Ticket* TypedTicket::unsafe_arena_release_ticket() {
  PROTOBUF_TSAN_WRITE(&_impl_._tsan_detect_race);
  // @@protoc_insertion_point(field_release:io.deephaven.proto.backplane.grpc.TypedTicket.ticket)

  _impl_._has_bits_[0] &= ~0x00000001u;
  ::io::deephaven::proto::backplane::grpc::Ticket* temp = _impl_.ticket_;
  _impl_.ticket_ = nullptr;
  return temp;
}
inline ::io::deephaven::proto::backplane::grpc::Ticket* TypedTicket::_internal_mutable_ticket() {
  PROTOBUF_TSAN_WRITE(&_impl_._tsan_detect_race);
  _impl_._has_bits_[0] |= 0x00000001u;
  if (_impl_.ticket_ == nullptr) {
    auto* p = CreateMaybeMessage<::io::deephaven::proto::backplane::grpc::Ticket>(GetArena());
    _impl_.ticket_ = reinterpret_cast<::io::deephaven::proto::backplane::grpc::Ticket*>(p);
  }
  return _impl_.ticket_;
}
inline ::io::deephaven::proto::backplane::grpc::Ticket* TypedTicket::mutable_ticket() ABSL_ATTRIBUTE_LIFETIME_BOUND {
  ::io::deephaven::proto::backplane::grpc::Ticket* _msg = _internal_mutable_ticket();
  // @@protoc_insertion_point(field_mutable:io.deephaven.proto.backplane.grpc.TypedTicket.ticket)
  return _msg;
}
inline void TypedTicket::set_allocated_ticket(::io::deephaven::proto::backplane::grpc::Ticket* value) {
  ::google::protobuf::Arena* message_arena = GetArena();
  PROTOBUF_TSAN_WRITE(&_impl_._tsan_detect_race);
  if (message_arena == nullptr) {
    delete reinterpret_cast<::io::deephaven::proto::backplane::grpc::Ticket*>(_impl_.ticket_);
  }

  if (value != nullptr) {
    ::google::protobuf::Arena* submessage_arena = reinterpret_cast<::io::deephaven::proto::backplane::grpc::Ticket*>(value)->GetArena();
    if (message_arena != submessage_arena) {
      value = ::google::protobuf::internal::GetOwnedMessage(message_arena, value, submessage_arena);
    }
    _impl_._has_bits_[0] |= 0x00000001u;
  } else {
    _impl_._has_bits_[0] &= ~0x00000001u;
  }

  _impl_.ticket_ = reinterpret_cast<::io::deephaven::proto::backplane::grpc::Ticket*>(value);
  // @@protoc_insertion_point(field_set_allocated:io.deephaven.proto.backplane.grpc.TypedTicket.ticket)
}

// string type = 2;
inline void TypedTicket::clear_type() {
  PROTOBUF_TSAN_WRITE(&_impl_._tsan_detect_race);
  _impl_.type_.ClearToEmpty();
}
inline const std::string& TypedTicket::type() const
    ABSL_ATTRIBUTE_LIFETIME_BOUND {
  // @@protoc_insertion_point(field_get:io.deephaven.proto.backplane.grpc.TypedTicket.type)
  return _internal_type();
}
template <typename Arg_, typename... Args_>
inline PROTOBUF_ALWAYS_INLINE void TypedTicket::set_type(Arg_&& arg,
                                                     Args_... args) {
  PROTOBUF_TSAN_WRITE(&_impl_._tsan_detect_race);
  ;
  _impl_.type_.Set(static_cast<Arg_&&>(arg), args..., GetArena());
  // @@protoc_insertion_point(field_set:io.deephaven.proto.backplane.grpc.TypedTicket.type)
}
inline std::string* TypedTicket::mutable_type() ABSL_ATTRIBUTE_LIFETIME_BOUND {
  std::string* _s = _internal_mutable_type();
  // @@protoc_insertion_point(field_mutable:io.deephaven.proto.backplane.grpc.TypedTicket.type)
  return _s;
}
inline const std::string& TypedTicket::_internal_type() const {
  PROTOBUF_TSAN_READ(&_impl_._tsan_detect_race);
  return _impl_.type_.Get();
}
inline void TypedTicket::_internal_set_type(const std::string& value) {
  PROTOBUF_TSAN_WRITE(&_impl_._tsan_detect_race);
  ;
  _impl_.type_.Set(value, GetArena());
}
inline std::string* TypedTicket::_internal_mutable_type() {
  PROTOBUF_TSAN_WRITE(&_impl_._tsan_detect_race);
  ;
  return _impl_.type_.Mutable( GetArena());
}
inline std::string* TypedTicket::release_type() {
  PROTOBUF_TSAN_WRITE(&_impl_._tsan_detect_race);
  // @@protoc_insertion_point(field_release:io.deephaven.proto.backplane.grpc.TypedTicket.type)
  return _impl_.type_.Release();
}
inline void TypedTicket::set_allocated_type(std::string* value) {
  PROTOBUF_TSAN_WRITE(&_impl_._tsan_detect_race);
  _impl_.type_.SetAllocated(value, GetArena());
  #ifdef PROTOBUF_FORCE_COPY_DEFAULT_STRING
        if (_impl_.type_.IsDefault()) {
          _impl_.type_.Set("", GetArena());
        }
  #endif  // PROTOBUF_FORCE_COPY_DEFAULT_STRING
  // @@protoc_insertion_point(field_set_allocated:io.deephaven.proto.backplane.grpc.TypedTicket.type)
}

#ifdef __GNUC__
#pragma GCC diagnostic pop
#endif  // __GNUC__

// @@protoc_insertion_point(namespace_scope)
}  // namespace grpc
}  // namespace backplane
}  // namespace proto
}  // namespace deephaven
}  // namespace io


// @@protoc_insertion_point(global_scope)

#include "google/protobuf/port_undef.inc"

#endif  // GOOGLE_PROTOBUF_INCLUDED_deephaven_2fproto_2fticket_2eproto_2epb_2eh
