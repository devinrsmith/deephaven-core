# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
"""Client and server classes corresponding to protobuf-defined services."""
import grpc
import warnings

from pydeephaven.proto import config_pb2 as deephaven_dot_proto_dot_config__pb2

GRPC_GENERATED_VERSION = '1.63.0'
GRPC_VERSION = grpc.__version__
EXPECTED_ERROR_RELEASE = '1.65.0'
SCHEDULED_RELEASE_DATE = 'June 25, 2024'
_version_not_supported = False

try:
    from grpc._utilities import first_version_is_lower
    _version_not_supported = first_version_is_lower(GRPC_VERSION, GRPC_GENERATED_VERSION)
except ImportError:
    _version_not_supported = True

if _version_not_supported:
    warnings.warn(
        f'The grpc package installed is at version {GRPC_VERSION},'
        + f' but the generated code in deephaven/proto/config_pb2_grpc.py depends on'
        + f' grpcio>={GRPC_GENERATED_VERSION}.'
        + f' Please upgrade your grpc module to grpcio>={GRPC_GENERATED_VERSION}'
        + f' or downgrade your generated code using grpcio-tools<={GRPC_VERSION}.'
        + f' This warning will become an error in {EXPECTED_ERROR_RELEASE},'
        + f' scheduled for release on {SCHEDULED_RELEASE_DATE}.',
        RuntimeWarning
    )


class ConfigServiceStub(object):
    """*
    Provides simple configuration data to users. Unauthenticated users may call GetAuthenticationConstants
    to discover hints on how they should proceed with providing their identity, while already-authenticated
    clients may call GetConfigurationConstants for details on using the platform.
    """

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.GetAuthenticationConstants = channel.unary_unary(
                '/io.deephaven.proto.backplane.grpc.ConfigService/GetAuthenticationConstants',
                request_serializer=deephaven_dot_proto_dot_config__pb2.AuthenticationConstantsRequest.SerializeToString,
                response_deserializer=deephaven_dot_proto_dot_config__pb2.AuthenticationConstantsResponse.FromString,
                _registered_method=True)
        self.GetConfigurationConstants = channel.unary_unary(
                '/io.deephaven.proto.backplane.grpc.ConfigService/GetConfigurationConstants',
                request_serializer=deephaven_dot_proto_dot_config__pb2.ConfigurationConstantsRequest.SerializeToString,
                response_deserializer=deephaven_dot_proto_dot_config__pb2.ConfigurationConstantsResponse.FromString,
                _registered_method=True)


class ConfigServiceServicer(object):
    """*
    Provides simple configuration data to users. Unauthenticated users may call GetAuthenticationConstants
    to discover hints on how they should proceed with providing their identity, while already-authenticated
    clients may call GetConfigurationConstants for details on using the platform.
    """

    def GetAuthenticationConstants(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def GetConfigurationConstants(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')


def add_ConfigServiceServicer_to_server(servicer, server):
    rpc_method_handlers = {
            'GetAuthenticationConstants': grpc.unary_unary_rpc_method_handler(
                    servicer.GetAuthenticationConstants,
                    request_deserializer=deephaven_dot_proto_dot_config__pb2.AuthenticationConstantsRequest.FromString,
                    response_serializer=deephaven_dot_proto_dot_config__pb2.AuthenticationConstantsResponse.SerializeToString,
            ),
            'GetConfigurationConstants': grpc.unary_unary_rpc_method_handler(
                    servicer.GetConfigurationConstants,
                    request_deserializer=deephaven_dot_proto_dot_config__pb2.ConfigurationConstantsRequest.FromString,
                    response_serializer=deephaven_dot_proto_dot_config__pb2.ConfigurationConstantsResponse.SerializeToString,
            ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
            'io.deephaven.proto.backplane.grpc.ConfigService', rpc_method_handlers)
    server.add_generic_rpc_handlers((generic_handler,))


 # This class is part of an EXPERIMENTAL API.
class ConfigService(object):
    """*
    Provides simple configuration data to users. Unauthenticated users may call GetAuthenticationConstants
    to discover hints on how they should proceed with providing their identity, while already-authenticated
    clients may call GetConfigurationConstants for details on using the platform.
    """

    @staticmethod
    def GetAuthenticationConstants(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(
            request,
            target,
            '/io.deephaven.proto.backplane.grpc.ConfigService/GetAuthenticationConstants',
            deephaven_dot_proto_dot_config__pb2.AuthenticationConstantsRequest.SerializeToString,
            deephaven_dot_proto_dot_config__pb2.AuthenticationConstantsResponse.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
            _registered_method=True)

    @staticmethod
    def GetConfigurationConstants(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(
            request,
            target,
            '/io.deephaven.proto.backplane.grpc.ConfigService/GetConfigurationConstants',
            deephaven_dot_proto_dot_config__pb2.ConfigurationConstantsRequest.SerializeToString,
            deephaven_dot_proto_dot_config__pb2.ConfigurationConstantsResponse.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
            _registered_method=True)
