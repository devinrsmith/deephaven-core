/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.object_pb;

import elemental2.core.JsArray;
import elemental2.core.Uint8Array;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.object_pb.streamresponse.PayloadCase;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven.proto.object_pb.StreamResponse",
        namespace = JsPackage.GLOBAL)
public class StreamResponse {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface DataFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface GetPayloadUnionType {
                @JsOverlay
                static StreamResponse.ToObjectReturnType.DataFieldType.GetPayloadUnionType of(Object o) {
                    return Js.cast(o);
                }

                @JsOverlay
                default String asString() {
                    return Js.asString(this);
                }

                @JsOverlay
                default Uint8Array asUint8Array() {
                    return Js.cast(this);
                }

                @JsOverlay
                default boolean isString() {
                    return (Object) this instanceof String;
                }

                @JsOverlay
                default boolean isUint8Array() {
                    return (Object) this instanceof Uint8Array;
                }
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface TypedExportIdsListFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface TicketFieldType {
                    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                    public interface GetTicketUnionType {
                        @JsOverlay
                        static StreamResponse.ToObjectReturnType.DataFieldType.TypedExportIdsListFieldType.TicketFieldType.GetTicketUnionType of(
                                Object o) {
                            return Js.cast(o);
                        }

                        @JsOverlay
                        default String asString() {
                            return Js.asString(this);
                        }

                        @JsOverlay
                        default Uint8Array asUint8Array() {
                            return Js.cast(this);
                        }

                        @JsOverlay
                        default boolean isString() {
                            return (Object) this instanceof String;
                        }

                        @JsOverlay
                        default boolean isUint8Array() {
                            return (Object) this instanceof Uint8Array;
                        }
                    }

                    @JsOverlay
                    static StreamResponse.ToObjectReturnType.DataFieldType.TypedExportIdsListFieldType.TicketFieldType create() {
                        return Js.uncheckedCast(JsPropertyMap.of());
                    }

                    @JsProperty
                    StreamResponse.ToObjectReturnType.DataFieldType.TypedExportIdsListFieldType.TicketFieldType.GetTicketUnionType getTicket();

                    @JsProperty
                    void setTicket(
                            StreamResponse.ToObjectReturnType.DataFieldType.TypedExportIdsListFieldType.TicketFieldType.GetTicketUnionType ticket);

                    @JsOverlay
                    default void setTicket(String ticket) {
                        setTicket(
                                Js.<StreamResponse.ToObjectReturnType.DataFieldType.TypedExportIdsListFieldType.TicketFieldType.GetTicketUnionType>uncheckedCast(
                                        ticket));
                    }

                    @JsOverlay
                    default void setTicket(Uint8Array ticket) {
                        setTicket(
                                Js.<StreamResponse.ToObjectReturnType.DataFieldType.TypedExportIdsListFieldType.TicketFieldType.GetTicketUnionType>uncheckedCast(
                                        ticket));
                    }
                }

                @JsOverlay
                static StreamResponse.ToObjectReturnType.DataFieldType.TypedExportIdsListFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                StreamResponse.ToObjectReturnType.DataFieldType.TypedExportIdsListFieldType.TicketFieldType getTicket();

                @JsProperty
                String getType();

                @JsProperty
                void setTicket(
                        StreamResponse.ToObjectReturnType.DataFieldType.TypedExportIdsListFieldType.TicketFieldType ticket);

                @JsProperty
                void setType(String type);
            }

            @JsOverlay
            static StreamResponse.ToObjectReturnType.DataFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            StreamResponse.ToObjectReturnType.DataFieldType.GetPayloadUnionType getPayload();

            @JsProperty
            JsArray<StreamResponse.ToObjectReturnType.DataFieldType.TypedExportIdsListFieldType> getTypedExportIdsList();

            @JsProperty
            void setPayload(StreamResponse.ToObjectReturnType.DataFieldType.GetPayloadUnionType payload);

            @JsOverlay
            default void setPayload(String payload) {
                setPayload(
                        Js.<StreamResponse.ToObjectReturnType.DataFieldType.GetPayloadUnionType>uncheckedCast(
                                payload));
            }

            @JsOverlay
            default void setPayload(Uint8Array payload) {
                setPayload(
                        Js.<StreamResponse.ToObjectReturnType.DataFieldType.GetPayloadUnionType>uncheckedCast(
                                payload));
            }

            @JsProperty
            void setTypedExportIdsList(
                    JsArray<StreamResponse.ToObjectReturnType.DataFieldType.TypedExportIdsListFieldType> typedExportIdsList);

            @JsOverlay
            default void setTypedExportIdsList(
                    StreamResponse.ToObjectReturnType.DataFieldType.TypedExportIdsListFieldType[] typedExportIdsList) {
                setTypedExportIdsList(
                        Js.<JsArray<StreamResponse.ToObjectReturnType.DataFieldType.TypedExportIdsListFieldType>>uncheckedCast(
                                typedExportIdsList));
            }
        }

        @JsOverlay
        static StreamResponse.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        StreamResponse.ToObjectReturnType.DataFieldType getData();

        @JsProperty
        void setData(StreamResponse.ToObjectReturnType.DataFieldType data);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface DataFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface GetPayloadUnionType {
                @JsOverlay
                static StreamResponse.ToObjectReturnType0.DataFieldType.GetPayloadUnionType of(Object o) {
                    return Js.cast(o);
                }

                @JsOverlay
                default String asString() {
                    return Js.asString(this);
                }

                @JsOverlay
                default Uint8Array asUint8Array() {
                    return Js.cast(this);
                }

                @JsOverlay
                default boolean isString() {
                    return (Object) this instanceof String;
                }

                @JsOverlay
                default boolean isUint8Array() {
                    return (Object) this instanceof Uint8Array;
                }
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface TypedExportIdsListFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface TicketFieldType {
                    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                    public interface GetTicketUnionType {
                        @JsOverlay
                        static StreamResponse.ToObjectReturnType0.DataFieldType.TypedExportIdsListFieldType.TicketFieldType.GetTicketUnionType of(
                                Object o) {
                            return Js.cast(o);
                        }

                        @JsOverlay
                        default String asString() {
                            return Js.asString(this);
                        }

                        @JsOverlay
                        default Uint8Array asUint8Array() {
                            return Js.cast(this);
                        }

                        @JsOverlay
                        default boolean isString() {
                            return (Object) this instanceof String;
                        }

                        @JsOverlay
                        default boolean isUint8Array() {
                            return (Object) this instanceof Uint8Array;
                        }
                    }

                    @JsOverlay
                    static StreamResponse.ToObjectReturnType0.DataFieldType.TypedExportIdsListFieldType.TicketFieldType create() {
                        return Js.uncheckedCast(JsPropertyMap.of());
                    }

                    @JsProperty
                    StreamResponse.ToObjectReturnType0.DataFieldType.TypedExportIdsListFieldType.TicketFieldType.GetTicketUnionType getTicket();

                    @JsProperty
                    void setTicket(
                            StreamResponse.ToObjectReturnType0.DataFieldType.TypedExportIdsListFieldType.TicketFieldType.GetTicketUnionType ticket);

                    @JsOverlay
                    default void setTicket(String ticket) {
                        setTicket(
                                Js.<StreamResponse.ToObjectReturnType0.DataFieldType.TypedExportIdsListFieldType.TicketFieldType.GetTicketUnionType>uncheckedCast(
                                        ticket));
                    }

                    @JsOverlay
                    default void setTicket(Uint8Array ticket) {
                        setTicket(
                                Js.<StreamResponse.ToObjectReturnType0.DataFieldType.TypedExportIdsListFieldType.TicketFieldType.GetTicketUnionType>uncheckedCast(
                                        ticket));
                    }
                }

                @JsOverlay
                static StreamResponse.ToObjectReturnType0.DataFieldType.TypedExportIdsListFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                StreamResponse.ToObjectReturnType0.DataFieldType.TypedExportIdsListFieldType.TicketFieldType getTicket();

                @JsProperty
                String getType();

                @JsProperty
                void setTicket(
                        StreamResponse.ToObjectReturnType0.DataFieldType.TypedExportIdsListFieldType.TicketFieldType ticket);

                @JsProperty
                void setType(String type);
            }

            @JsOverlay
            static StreamResponse.ToObjectReturnType0.DataFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            StreamResponse.ToObjectReturnType0.DataFieldType.GetPayloadUnionType getPayload();

            @JsProperty
            JsArray<StreamResponse.ToObjectReturnType0.DataFieldType.TypedExportIdsListFieldType> getTypedExportIdsList();

            @JsProperty
            void setPayload(StreamResponse.ToObjectReturnType0.DataFieldType.GetPayloadUnionType payload);

            @JsOverlay
            default void setPayload(String payload) {
                setPayload(
                        Js.<StreamResponse.ToObjectReturnType0.DataFieldType.GetPayloadUnionType>uncheckedCast(
                                payload));
            }

            @JsOverlay
            default void setPayload(Uint8Array payload) {
                setPayload(
                        Js.<StreamResponse.ToObjectReturnType0.DataFieldType.GetPayloadUnionType>uncheckedCast(
                                payload));
            }

            @JsProperty
            void setTypedExportIdsList(
                    JsArray<StreamResponse.ToObjectReturnType0.DataFieldType.TypedExportIdsListFieldType> typedExportIdsList);

            @JsOverlay
            default void setTypedExportIdsList(
                    StreamResponse.ToObjectReturnType0.DataFieldType.TypedExportIdsListFieldType[] typedExportIdsList) {
                setTypedExportIdsList(
                        Js.<JsArray<StreamResponse.ToObjectReturnType0.DataFieldType.TypedExportIdsListFieldType>>uncheckedCast(
                                typedExportIdsList));
            }
        }

        @JsOverlay
        static StreamResponse.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        StreamResponse.ToObjectReturnType0.DataFieldType getData();

        @JsProperty
        void setData(StreamResponse.ToObjectReturnType0.DataFieldType data);
    }

    public static native StreamResponse deserializeBinary(Uint8Array bytes);

    public static native StreamResponse deserializeBinaryFromReader(
            StreamResponse message, Object reader);

    public static native void serializeBinaryToWriter(StreamResponse message, Object writer);

    public static native StreamResponse.ToObjectReturnType toObject(
            boolean includeInstance, StreamResponse msg);

    public native void clearData();

    public native Data getData();

    public native PayloadCase getPayloadCase();

    public native boolean hasData();

    public native Uint8Array serializeBinary();

    public native void setData();

    public native void setData(Data value);

    public native StreamResponse.ToObjectReturnType0 toObject();

    public native StreamResponse.ToObjectReturnType0 toObject(boolean includeInstance);
}
