# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: deephaven/proto/console.proto
# Protobuf Python Version: 5.26.1
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


from pydeephaven.proto import ticket_pb2 as deephaven_dot_proto_dot_ticket__pb2
from pydeephaven.proto import application_pb2 as deephaven_dot_proto_dot_application__pb2


DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x1d\x64\x65\x65phaven/proto/console.proto\x12(io.deephaven.proto.backplane.script.grpc\x1a\x1c\x64\x65\x65phaven/proto/ticket.proto\x1a!deephaven/proto/application.proto\"\x18\n\x16GetConsoleTypesRequest\"0\n\x17GetConsoleTypesResponse\x12\x15\n\rconsole_types\x18\x01 \x03(\t\"i\n\x13StartConsoleRequest\x12<\n\tresult_id\x18\x01 \x01(\x0b\x32).io.deephaven.proto.backplane.grpc.Ticket\x12\x14\n\x0csession_type\x18\x02 \x01(\t\"T\n\x14StartConsoleResponse\x12<\n\tresult_id\x18\x01 \x01(\x0b\x32).io.deephaven.proto.backplane.grpc.Ticket\"\x14\n\x12GetHeapInfoRequest\"`\n\x13GetHeapInfoResponse\x12\x16\n\nmax_memory\x18\x01 \x01(\x03\x42\x02\x30\x01\x12\x18\n\x0ctotal_memory\x18\x02 \x01(\x03\x42\x02\x30\x01\x12\x17\n\x0b\x66ree_memory\x18\x03 \x01(\x03\x42\x02\x30\x01\"M\n\x16LogSubscriptionRequest\x12#\n\x17last_seen_log_timestamp\x18\x01 \x01(\x03\x42\x02\x30\x01\x12\x0e\n\x06levels\x18\x02 \x03(\t\"S\n\x13LogSubscriptionData\x12\x12\n\x06micros\x18\x01 \x01(\x03\x42\x02\x30\x01\x12\x11\n\tlog_level\x18\x02 \x01(\t\x12\x0f\n\x07message\x18\x03 \x01(\tJ\x04\x08\x04\x10\x05\"j\n\x15\x45xecuteCommandRequest\x12=\n\nconsole_id\x18\x01 \x01(\x0b\x32).io.deephaven.proto.backplane.grpc.Ticket\x12\x0c\n\x04\x63ode\x18\x03 \x01(\tJ\x04\x08\x02\x10\x03\"w\n\x16\x45xecuteCommandResponse\x12\x15\n\rerror_message\x18\x01 \x01(\t\x12\x46\n\x07\x63hanges\x18\x02 \x01(\x0b\x32\x35.io.deephaven.proto.backplane.grpc.FieldsChangeUpdate\"\xb5\x01\n\x1a\x42indTableToVariableRequest\x12=\n\nconsole_id\x18\x01 \x01(\x0b\x32).io.deephaven.proto.backplane.grpc.Ticket\x12\x15\n\rvariable_name\x18\x03 \x01(\t\x12;\n\x08table_id\x18\x04 \x01(\x0b\x32).io.deephaven.proto.backplane.grpc.TicketJ\x04\x08\x02\x10\x03\"\x1d\n\x1b\x42indTableToVariableResponse\"\x94\x01\n\x14\x43\x61ncelCommandRequest\x12=\n\nconsole_id\x18\x01 \x01(\x0b\x32).io.deephaven.proto.backplane.grpc.Ticket\x12=\n\ncommand_id\x18\x02 \x01(\x0b\x32).io.deephaven.proto.backplane.grpc.Ticket\"\x17\n\x15\x43\x61ncelCommandResponse\"n\n\x19\x43\x61ncelAutoCompleteRequest\x12=\n\nconsole_id\x18\x01 \x01(\x0b\x32).io.deephaven.proto.backplane.grpc.Ticket\x12\x12\n\nrequest_id\x18\x02 \x01(\x05\"\x1c\n\x1a\x43\x61ncelAutoCompleteResponse\"\xf1\x05\n\x13\x41utoCompleteRequest\x12=\n\nconsole_id\x18\x05 \x01(\x0b\x32).io.deephaven.proto.backplane.grpc.Ticket\x12\x12\n\nrequest_id\x18\x06 \x01(\x05\x12V\n\ropen_document\x18\x01 \x01(\x0b\x32=.io.deephaven.proto.backplane.script.grpc.OpenDocumentRequestH\x00\x12Z\n\x0f\x63hange_document\x18\x02 \x01(\x0b\x32?.io.deephaven.proto.backplane.script.grpc.ChangeDocumentRequestH\x00\x12\x63\n\x14get_completion_items\x18\x03 \x01(\x0b\x32\x43.io.deephaven.proto.backplane.script.grpc.GetCompletionItemsRequestH\x00\x12_\n\x12get_signature_help\x18\x07 \x01(\x0b\x32\x41.io.deephaven.proto.backplane.script.grpc.GetSignatureHelpRequestH\x00\x12N\n\tget_hover\x18\x08 \x01(\x0b\x32\x39.io.deephaven.proto.backplane.script.grpc.GetHoverRequestH\x00\x12X\n\x0eget_diagnostic\x18\t \x01(\x0b\x32>.io.deephaven.proto.backplane.script.grpc.GetDiagnosticRequestH\x00\x12X\n\x0e\x63lose_document\x18\x04 \x01(\x0b\x32>.io.deephaven.proto.backplane.script.grpc.CloseDocumentRequestH\x00\x42\t\n\x07request\"\x91\x04\n\x14\x41utoCompleteResponse\x12\x12\n\nrequest_id\x18\x02 \x01(\x05\x12\x0f\n\x07success\x18\x03 \x01(\x08\x12`\n\x10\x63ompletion_items\x18\x01 \x01(\x0b\x32\x44.io.deephaven.proto.backplane.script.grpc.GetCompletionItemsResponseH\x00\x12X\n\nsignatures\x18\x04 \x01(\x0b\x32\x42.io.deephaven.proto.backplane.script.grpc.GetSignatureHelpResponseH\x00\x12K\n\x05hover\x18\x05 \x01(\x0b\x32:.io.deephaven.proto.backplane.script.grpc.GetHoverResponseH\x00\x12Y\n\ndiagnostic\x18\x06 \x01(\x0b\x32\x43.io.deephaven.proto.backplane.script.grpc.GetPullDiagnosticResponseH\x00\x12\x64\n\x12\x64iagnostic_publish\x18\x07 \x01(\x0b\x32\x46.io.deephaven.proto.backplane.script.grpc.GetPublishDiagnosticResponseH\x00\x42\n\n\x08response\"\x15\n\x13\x42rowserNextResponse\"\xab\x01\n\x13OpenDocumentRequest\x12\x41\n\nconsole_id\x18\x01 \x01(\x0b\x32).io.deephaven.proto.backplane.grpc.TicketB\x02\x18\x01\x12Q\n\rtext_document\x18\x02 \x01(\x0b\x32:.io.deephaven.proto.backplane.script.grpc.TextDocumentItem\"S\n\x10TextDocumentItem\x12\x0b\n\x03uri\x18\x01 \x01(\t\x12\x13\n\x0blanguage_id\x18\x02 \x01(\t\x12\x0f\n\x07version\x18\x03 \x01(\x05\x12\x0c\n\x04text\x18\x04 \x01(\t\"\xbb\x01\n\x14\x43loseDocumentRequest\x12\x41\n\nconsole_id\x18\x01 \x01(\x0b\x32).io.deephaven.proto.backplane.grpc.TicketB\x02\x18\x01\x12`\n\rtext_document\x18\x02 \x01(\x0b\x32I.io.deephaven.proto.backplane.script.grpc.VersionedTextDocumentIdentifier\"\xc4\x03\n\x15\x43hangeDocumentRequest\x12\x41\n\nconsole_id\x18\x01 \x01(\x0b\x32).io.deephaven.proto.backplane.grpc.TicketB\x02\x18\x01\x12`\n\rtext_document\x18\x02 \x01(\x0b\x32I.io.deephaven.proto.backplane.script.grpc.VersionedTextDocumentIdentifier\x12w\n\x0f\x63ontent_changes\x18\x03 \x03(\x0b\x32^.io.deephaven.proto.backplane.script.grpc.ChangeDocumentRequest.TextDocumentContentChangeEvent\x1a\x8c\x01\n\x1eTextDocumentContentChangeEvent\x12\x46\n\x05range\x18\x01 \x01(\x0b\x32\x37.io.deephaven.proto.backplane.script.grpc.DocumentRange\x12\x14\n\x0crange_length\x18\x02 \x01(\x05\x12\x0c\n\x04text\x18\x03 \x01(\t\"\x93\x01\n\rDocumentRange\x12\x41\n\x05start\x18\x01 \x01(\x0b\x32\x32.io.deephaven.proto.backplane.script.grpc.Position\x12?\n\x03\x65nd\x18\x02 \x01(\x0b\x32\x32.io.deephaven.proto.backplane.script.grpc.Position\"?\n\x1fVersionedTextDocumentIdentifier\x12\x0b\n\x03uri\x18\x01 \x01(\t\x12\x0f\n\x07version\x18\x02 \x01(\x05\"+\n\x08Position\x12\x0c\n\x04line\x18\x01 \x01(\x05\x12\x11\n\tcharacter\x18\x02 \x01(\x05\",\n\rMarkupContent\x12\x0c\n\x04kind\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\t\"\xec\x02\n\x19GetCompletionItemsRequest\x12\x41\n\nconsole_id\x18\x01 \x01(\x0b\x32).io.deephaven.proto.backplane.grpc.TicketB\x02\x18\x01\x12L\n\x07\x63ontext\x18\x02 \x01(\x0b\x32;.io.deephaven.proto.backplane.script.grpc.CompletionContext\x12`\n\rtext_document\x18\x03 \x01(\x0b\x32I.io.deephaven.proto.backplane.script.grpc.VersionedTextDocumentIdentifier\x12\x44\n\x08position\x18\x04 \x01(\x0b\x32\x32.io.deephaven.proto.backplane.script.grpc.Position\x12\x16\n\nrequest_id\x18\x05 \x01(\x05\x42\x02\x18\x01\"D\n\x11\x43ompletionContext\x12\x14\n\x0ctrigger_kind\x18\x01 \x01(\x05\x12\x19\n\x11trigger_character\x18\x02 \x01(\t\"\x92\x01\n\x1aGetCompletionItemsResponse\x12G\n\x05items\x18\x01 \x03(\x0b\x32\x38.io.deephaven.proto.backplane.script.grpc.CompletionItem\x12\x16\n\nrequest_id\x18\x02 \x01(\x05\x42\x02\x18\x01\x12\x13\n\x07success\x18\x03 \x01(\x08\x42\x02\x18\x01\"\xd2\x03\n\x0e\x43ompletionItem\x12\r\n\x05start\x18\x01 \x01(\x05\x12\x0e\n\x06length\x18\x02 \x01(\x05\x12\r\n\x05label\x18\x03 \x01(\t\x12\x0c\n\x04kind\x18\x04 \x01(\x05\x12\x0e\n\x06\x64\x65tail\x18\x05 \x01(\t\x12\x12\n\ndeprecated\x18\x07 \x01(\x08\x12\x11\n\tpreselect\x18\x08 \x01(\x08\x12\x45\n\ttext_edit\x18\t \x01(\x0b\x32\x32.io.deephaven.proto.backplane.script.grpc.TextEdit\x12\x11\n\tsort_text\x18\n \x01(\t\x12\x13\n\x0b\x66ilter_text\x18\x0b \x01(\t\x12\x1a\n\x12insert_text_format\x18\x0c \x01(\x05\x12Q\n\x15\x61\x64\x64itional_text_edits\x18\r \x03(\x0b\x32\x32.io.deephaven.proto.backplane.script.grpc.TextEdit\x12\x19\n\x11\x63ommit_characters\x18\x0e \x03(\t\x12N\n\rdocumentation\x18\x0f \x01(\x0b\x32\x37.io.deephaven.proto.backplane.script.grpc.MarkupContentJ\x04\x08\x06\x10\x07\"`\n\x08TextEdit\x12\x46\n\x05range\x18\x01 \x01(\x0b\x32\x37.io.deephaven.proto.backplane.script.grpc.DocumentRange\x12\x0c\n\x04text\x18\x02 \x01(\t\"\x92\x02\n\x17GetSignatureHelpRequest\x12O\n\x07\x63ontext\x18\x01 \x01(\x0b\x32>.io.deephaven.proto.backplane.script.grpc.SignatureHelpContext\x12`\n\rtext_document\x18\x02 \x01(\x0b\x32I.io.deephaven.proto.backplane.script.grpc.VersionedTextDocumentIdentifier\x12\x44\n\x08position\x18\x03 \x01(\x0b\x32\x32.io.deephaven.proto.backplane.script.grpc.Position\"\xdb\x01\n\x14SignatureHelpContext\x12\x14\n\x0ctrigger_kind\x18\x01 \x01(\x05\x12\x1e\n\x11trigger_character\x18\x02 \x01(\tH\x00\x88\x01\x01\x12\x14\n\x0cis_retrigger\x18\x03 \x01(\x08\x12\x61\n\x15\x61\x63tive_signature_help\x18\x04 \x01(\x0b\x32\x42.io.deephaven.proto.backplane.script.grpc.GetSignatureHelpResponseB\x14\n\x12_trigger_character\"\xd6\x01\n\x18GetSignatureHelpResponse\x12R\n\nsignatures\x18\x01 \x03(\x0b\x32>.io.deephaven.proto.backplane.script.grpc.SignatureInformation\x12\x1d\n\x10\x61\x63tive_signature\x18\x02 \x01(\x05H\x00\x88\x01\x01\x12\x1d\n\x10\x61\x63tive_parameter\x18\x03 \x01(\x05H\x01\x88\x01\x01\x42\x13\n\x11_active_signatureB\x13\n\x11_active_parameter\"\xfd\x01\n\x14SignatureInformation\x12\r\n\x05label\x18\x01 \x01(\t\x12N\n\rdocumentation\x18\x02 \x01(\x0b\x32\x37.io.deephaven.proto.backplane.script.grpc.MarkupContent\x12R\n\nparameters\x18\x03 \x03(\x0b\x32>.io.deephaven.proto.backplane.script.grpc.ParameterInformation\x12\x1d\n\x10\x61\x63tive_parameter\x18\x04 \x01(\x05H\x00\x88\x01\x01\x42\x13\n\x11_active_parameter\"u\n\x14ParameterInformation\x12\r\n\x05label\x18\x01 \x01(\t\x12N\n\rdocumentation\x18\x02 \x01(\x0b\x32\x37.io.deephaven.proto.backplane.script.grpc.MarkupContent\"\xb9\x01\n\x0fGetHoverRequest\x12`\n\rtext_document\x18\x01 \x01(\x0b\x32I.io.deephaven.proto.backplane.script.grpc.VersionedTextDocumentIdentifier\x12\x44\n\x08position\x18\x02 \x01(\x0b\x32\x32.io.deephaven.proto.backplane.script.grpc.Position\"\xa5\x01\n\x10GetHoverResponse\x12I\n\x08\x63ontents\x18\x01 \x01(\x0b\x32\x37.io.deephaven.proto.backplane.script.grpc.MarkupContent\x12\x46\n\x05range\x18\x02 \x01(\x0b\x32\x37.io.deephaven.proto.backplane.script.grpc.DocumentRange\"\xd8\x01\n\x14GetDiagnosticRequest\x12`\n\rtext_document\x18\x01 \x01(\x0b\x32I.io.deephaven.proto.backplane.script.grpc.VersionedTextDocumentIdentifier\x12\x17\n\nidentifier\x18\x02 \x01(\tH\x00\x88\x01\x01\x12\x1f\n\x12previous_result_id\x18\x03 \x01(\tH\x01\x88\x01\x01\x42\r\n\x0b_identifierB\x15\n\x13_previous_result_id\"\x94\x01\n\x19GetPullDiagnosticResponse\x12\x0c\n\x04kind\x18\x01 \x01(\t\x12\x16\n\tresult_id\x18\x02 \x01(\tH\x00\x88\x01\x01\x12\x43\n\x05items\x18\x03 \x03(\x0b\x32\x34.io.deephaven.proto.backplane.script.grpc.DiagnosticB\x0c\n\n_result_id\"\x98\x01\n\x1cGetPublishDiagnosticResponse\x12\x0b\n\x03uri\x18\x01 \x01(\t\x12\x14\n\x07version\x18\x02 \x01(\x05H\x00\x88\x01\x01\x12I\n\x0b\x64iagnostics\x18\x03 \x03(\x0b\x32\x34.io.deephaven.proto.backplane.script.grpc.DiagnosticB\n\n\x08_version\"\xa7\x05\n\nDiagnostic\x12\x46\n\x05range\x18\x01 \x01(\x0b\x32\x37.io.deephaven.proto.backplane.script.grpc.DocumentRange\x12Y\n\x08severity\x18\x02 \x01(\x0e\x32G.io.deephaven.proto.backplane.script.grpc.Diagnostic.DiagnosticSeverity\x12\x11\n\x04\x63ode\x18\x03 \x01(\tH\x00\x88\x01\x01\x12\x63\n\x10\x63ode_description\x18\x04 \x01(\x0b\x32\x44.io.deephaven.proto.backplane.script.grpc.Diagnostic.CodeDescriptionH\x01\x88\x01\x01\x12\x13\n\x06source\x18\x05 \x01(\tH\x02\x88\x01\x01\x12\x0f\n\x07message\x18\x06 \x01(\t\x12P\n\x04tags\x18\x07 \x03(\x0e\x32\x42.io.deephaven.proto.backplane.script.grpc.Diagnostic.DiagnosticTag\x12\x11\n\x04\x64\x61ta\x18\t \x01(\x0cH\x03\x88\x01\x01\x1a\x1f\n\x0f\x43odeDescription\x12\x0c\n\x04href\x18\x01 \x01(\t\"]\n\x12\x44iagnosticSeverity\x12\x14\n\x10NOT_SET_SEVERITY\x10\x00\x12\t\n\x05\x45RROR\x10\x01\x12\x0b\n\x07WARNING\x10\x02\x12\x0f\n\x0bINFORMATION\x10\x03\x12\x08\n\x04HINT\x10\x04\"A\n\rDiagnosticTag\x12\x0f\n\x0bNOT_SET_TAG\x10\x00\x12\x0f\n\x0bUNNECESSARY\x10\x01\x12\x0e\n\nDEPRECATED\x10\x02\x42\x07\n\x05_codeB\x13\n\x11_code_descriptionB\t\n\x07_sourceB\x07\n\x05_data\"\xe6\x30\n\x10\x46igureDescriptor\x12\x12\n\x05title\x18\x01 \x01(\tH\x00\x88\x01\x01\x12\x12\n\ntitle_font\x18\x02 \x01(\t\x12\x13\n\x0btitle_color\x18\x03 \x01(\t\x12\x1b\n\x0fupdate_interval\x18\x07 \x01(\x03\x42\x02\x30\x01\x12\x0c\n\x04\x63ols\x18\x08 \x01(\x05\x12\x0c\n\x04rows\x18\t \x01(\x05\x12Z\n\x06\x63harts\x18\n \x03(\x0b\x32J.io.deephaven.proto.backplane.script.grpc.FigureDescriptor.ChartDescriptor\x12\x0e\n\x06\x65rrors\x18\r \x03(\t\x1a\xce\x05\n\x0f\x43hartDescriptor\x12\x0f\n\x07\x63olspan\x18\x01 \x01(\x05\x12\x0f\n\x07rowspan\x18\x02 \x01(\x05\x12[\n\x06series\x18\x03 \x03(\x0b\x32K.io.deephaven.proto.backplane.script.grpc.FigureDescriptor.SeriesDescriptor\x12\x66\n\x0cmulti_series\x18\x04 \x03(\x0b\x32P.io.deephaven.proto.backplane.script.grpc.FigureDescriptor.MultiSeriesDescriptor\x12W\n\x04\x61xes\x18\x05 \x03(\x0b\x32I.io.deephaven.proto.backplane.script.grpc.FigureDescriptor.AxisDescriptor\x12h\n\nchart_type\x18\x06 \x01(\x0e\x32T.io.deephaven.proto.backplane.script.grpc.FigureDescriptor.ChartDescriptor.ChartType\x12\x12\n\x05title\x18\x07 \x01(\tH\x00\x88\x01\x01\x12\x12\n\ntitle_font\x18\x08 \x01(\t\x12\x13\n\x0btitle_color\x18\t \x01(\t\x12\x13\n\x0bshow_legend\x18\n \x01(\x08\x12\x13\n\x0blegend_font\x18\x0b \x01(\t\x12\x14\n\x0clegend_color\x18\x0c \x01(\t\x12\x0c\n\x04is3d\x18\r \x01(\x08\x12\x0e\n\x06\x63olumn\x18\x0e \x01(\x05\x12\x0b\n\x03row\x18\x0f \x01(\x05\"_\n\tChartType\x12\x06\n\x02XY\x10\x00\x12\x07\n\x03PIE\x10\x01\x12\x0c\n\x04OHLC\x10\x02\x1a\x02\x08\x01\x12\x0c\n\x08\x43\x41TEGORY\x10\x03\x12\x07\n\x03XYZ\x10\x04\x12\x0f\n\x0b\x43\x41TEGORY_3D\x10\x05\x12\x0b\n\x07TREEMAP\x10\x06\x42\x08\n\x06_title\x1a\xfe\x04\n\x10SeriesDescriptor\x12^\n\nplot_style\x18\x01 \x01(\x0e\x32J.io.deephaven.proto.backplane.script.grpc.FigureDescriptor.SeriesPlotStyle\x12\x0c\n\x04name\x18\x02 \x01(\t\x12\x1a\n\rlines_visible\x18\x03 \x01(\x08H\x00\x88\x01\x01\x12\x1b\n\x0eshapes_visible\x18\x04 \x01(\x08H\x01\x88\x01\x01\x12\x18\n\x10gradient_visible\x18\x05 \x01(\x08\x12\x12\n\nline_color\x18\x06 \x01(\t\x12\x1f\n\x12point_label_format\x18\x08 \x01(\tH\x02\x88\x01\x01\x12\x1f\n\x12x_tool_tip_pattern\x18\t \x01(\tH\x03\x88\x01\x01\x12\x1f\n\x12y_tool_tip_pattern\x18\n \x01(\tH\x04\x88\x01\x01\x12\x13\n\x0bshape_label\x18\x0b \x01(\t\x12\x17\n\nshape_size\x18\x0c \x01(\x01H\x05\x88\x01\x01\x12\x13\n\x0bshape_color\x18\r \x01(\t\x12\r\n\x05shape\x18\x0e \x01(\t\x12\x61\n\x0c\x64\x61ta_sources\x18\x0f \x03(\x0b\x32K.io.deephaven.proto.backplane.script.grpc.FigureDescriptor.SourceDescriptorB\x10\n\x0e_lines_visibleB\x11\n\x0f_shapes_visibleB\x15\n\x13_point_label_formatB\x15\n\x13_x_tool_tip_patternB\x15\n\x13_y_tool_tip_patternB\r\n\x0b_shape_sizeJ\x04\x08\x07\x10\x08\x1a\xec\n\n\x15MultiSeriesDescriptor\x12^\n\nplot_style\x18\x01 \x01(\x0e\x32J.io.deephaven.proto.backplane.script.grpc.FigureDescriptor.SeriesPlotStyle\x12\x0c\n\x04name\x18\x02 \x01(\t\x12\x63\n\nline_color\x18\x03 \x01(\x0b\x32O.io.deephaven.proto.backplane.script.grpc.FigureDescriptor.StringMapWithDefault\x12\x64\n\x0bpoint_color\x18\x04 \x01(\x0b\x32O.io.deephaven.proto.backplane.script.grpc.FigureDescriptor.StringMapWithDefault\x12\x64\n\rlines_visible\x18\x05 \x01(\x0b\x32M.io.deephaven.proto.backplane.script.grpc.FigureDescriptor.BoolMapWithDefault\x12\x65\n\x0epoints_visible\x18\x06 \x01(\x0b\x32M.io.deephaven.proto.backplane.script.grpc.FigureDescriptor.BoolMapWithDefault\x12g\n\x10gradient_visible\x18\x07 \x01(\x0b\x32M.io.deephaven.proto.backplane.script.grpc.FigureDescriptor.BoolMapWithDefault\x12k\n\x12point_label_format\x18\x08 \x01(\x0b\x32O.io.deephaven.proto.backplane.script.grpc.FigureDescriptor.StringMapWithDefault\x12k\n\x12x_tool_tip_pattern\x18\t \x01(\x0b\x32O.io.deephaven.proto.backplane.script.grpc.FigureDescriptor.StringMapWithDefault\x12k\n\x12y_tool_tip_pattern\x18\n \x01(\x0b\x32O.io.deephaven.proto.backplane.script.grpc.FigureDescriptor.StringMapWithDefault\x12\x64\n\x0bpoint_label\x18\x0b \x01(\x0b\x32O.io.deephaven.proto.backplane.script.grpc.FigureDescriptor.StringMapWithDefault\x12\x63\n\npoint_size\x18\x0c \x01(\x0b\x32O.io.deephaven.proto.backplane.script.grpc.FigureDescriptor.DoubleMapWithDefault\x12\x64\n\x0bpoint_shape\x18\r \x01(\x0b\x32O.io.deephaven.proto.backplane.script.grpc.FigureDescriptor.StringMapWithDefault\x12l\n\x0c\x64\x61ta_sources\x18\x0e \x03(\x0b\x32V.io.deephaven.proto.backplane.script.grpc.FigureDescriptor.MultiSeriesSourceDescriptor\x1a\x64\n\x14StringMapWithDefault\x12\x1b\n\x0e\x64\x65\x66\x61ult_string\x18\x01 \x01(\tH\x00\x88\x01\x01\x12\x0c\n\x04keys\x18\x02 \x03(\t\x12\x0e\n\x06values\x18\x03 \x03(\tB\x11\n\x0f_default_string\x1a\x64\n\x14\x44oubleMapWithDefault\x12\x1b\n\x0e\x64\x65\x66\x61ult_double\x18\x01 \x01(\x01H\x00\x88\x01\x01\x12\x0c\n\x04keys\x18\x02 \x03(\t\x12\x0e\n\x06values\x18\x03 \x03(\x01\x42\x11\n\x0f_default_double\x1a^\n\x12\x42oolMapWithDefault\x12\x19\n\x0c\x64\x65\x66\x61ult_bool\x18\x01 \x01(\x08H\x00\x88\x01\x01\x12\x0c\n\x04keys\x18\x02 \x03(\t\x12\x0e\n\x06values\x18\x03 \x03(\x08\x42\x0f\n\r_default_bool\x1a\xa6\x08\n\x0e\x41xisDescriptor\x12\n\n\x02id\x18\x01 \x01(\t\x12m\n\x0b\x66ormat_type\x18\x02 \x01(\x0e\x32X.io.deephaven.proto.backplane.script.grpc.FigureDescriptor.AxisDescriptor.AxisFormatType\x12`\n\x04type\x18\x03 \x01(\x0e\x32R.io.deephaven.proto.backplane.script.grpc.FigureDescriptor.AxisDescriptor.AxisType\x12h\n\x08position\x18\x04 \x01(\x0e\x32V.io.deephaven.proto.backplane.script.grpc.FigureDescriptor.AxisDescriptor.AxisPosition\x12\x0b\n\x03log\x18\x05 \x01(\x08\x12\r\n\x05label\x18\x06 \x01(\t\x12\x12\n\nlabel_font\x18\x07 \x01(\t\x12\x12\n\nticks_font\x18\x08 \x01(\t\x12\x1b\n\x0e\x66ormat_pattern\x18\t \x01(\tH\x00\x88\x01\x01\x12\r\n\x05\x63olor\x18\n \x01(\t\x12\x11\n\tmin_range\x18\x0b \x01(\x01\x12\x11\n\tmax_range\x18\x0c \x01(\x01\x12\x1b\n\x13minor_ticks_visible\x18\r \x01(\x08\x12\x1b\n\x13major_ticks_visible\x18\x0e \x01(\x08\x12\x18\n\x10minor_tick_count\x18\x0f \x01(\x05\x12$\n\x17gap_between_major_ticks\x18\x10 \x01(\x01H\x01\x88\x01\x01\x12\x1c\n\x14major_tick_locations\x18\x11 \x03(\x01\x12\x18\n\x10tick_label_angle\x18\x12 \x01(\x01\x12\x0e\n\x06invert\x18\x13 \x01(\x08\x12\x14\n\x0cis_time_axis\x18\x14 \x01(\x08\x12{\n\x1c\x62usiness_calendar_descriptor\x18\x15 \x01(\x0b\x32U.io.deephaven.proto.backplane.script.grpc.FigureDescriptor.BusinessCalendarDescriptor\"*\n\x0e\x41xisFormatType\x12\x0c\n\x08\x43\x41TEGORY\x10\x00\x12\n\n\x06NUMBER\x10\x01\"C\n\x08\x41xisType\x12\x05\n\x01X\x10\x00\x12\x05\n\x01Y\x10\x01\x12\t\n\x05SHAPE\x10\x02\x12\x08\n\x04SIZE\x10\x03\x12\t\n\x05LABEL\x10\x04\x12\t\n\x05\x43OLOR\x10\x05\"B\n\x0c\x41xisPosition\x12\x07\n\x03TOP\x10\x00\x12\n\n\x06\x42OTTOM\x10\x01\x12\x08\n\x04LEFT\x10\x02\x12\t\n\x05RIGHT\x10\x03\x12\x08\n\x04NONE\x10\x04\x42\x11\n\x0f_format_patternB\x1a\n\x18_gap_between_major_ticks\x1a\xf0\x06\n\x1a\x42usinessCalendarDescriptor\x12\x0c\n\x04name\x18\x01 \x01(\t\x12\x11\n\ttime_zone\x18\x02 \x01(\t\x12v\n\rbusiness_days\x18\x03 \x03(\x0e\x32_.io.deephaven.proto.backplane.script.grpc.FigureDescriptor.BusinessCalendarDescriptor.DayOfWeek\x12~\n\x10\x62usiness_periods\x18\x04 \x03(\x0b\x32\x64.io.deephaven.proto.backplane.script.grpc.FigureDescriptor.BusinessCalendarDescriptor.BusinessPeriod\x12o\n\x08holidays\x18\x05 \x03(\x0b\x32].io.deephaven.proto.backplane.script.grpc.FigureDescriptor.BusinessCalendarDescriptor.Holiday\x1a-\n\x0e\x42usinessPeriod\x12\x0c\n\x04open\x18\x01 \x01(\t\x12\r\n\x05\x63lose\x18\x02 \x01(\t\x1a\xf8\x01\n\x07Holiday\x12m\n\x04\x64\x61te\x18\x01 \x01(\x0b\x32_.io.deephaven.proto.backplane.script.grpc.FigureDescriptor.BusinessCalendarDescriptor.LocalDate\x12~\n\x10\x62usiness_periods\x18\x02 \x03(\x0b\x32\x64.io.deephaven.proto.backplane.script.grpc.FigureDescriptor.BusinessCalendarDescriptor.BusinessPeriod\x1a\x35\n\tLocalDate\x12\x0c\n\x04year\x18\x01 \x01(\x05\x12\r\n\x05month\x18\x02 \x01(\x05\x12\x0b\n\x03\x64\x61y\x18\x03 \x01(\x05\"g\n\tDayOfWeek\x12\n\n\x06SUNDAY\x10\x00\x12\n\n\x06MONDAY\x10\x01\x12\x0b\n\x07TUESDAY\x10\x02\x12\r\n\tWEDNESDAY\x10\x03\x12\x0c\n\x08THURSDAY\x10\x04\x12\n\n\x06\x46RIDAY\x10\x05\x12\x0c\n\x08SATURDAY\x10\x06\x1a\xb6\x01\n\x1bMultiSeriesSourceDescriptor\x12\x0f\n\x07\x61xis_id\x18\x01 \x01(\t\x12S\n\x04type\x18\x02 \x01(\x0e\x32\x45.io.deephaven.proto.backplane.script.grpc.FigureDescriptor.SourceType\x12\x1c\n\x14partitioned_table_id\x18\x03 \x01(\x05\x12\x13\n\x0b\x63olumn_name\x18\x04 \x01(\t\x1a\xb4\x02\n\x10SourceDescriptor\x12\x0f\n\x07\x61xis_id\x18\x01 \x01(\t\x12S\n\x04type\x18\x02 \x01(\x0e\x32\x45.io.deephaven.proto.backplane.script.grpc.FigureDescriptor.SourceType\x12\x10\n\x08table_id\x18\x03 \x01(\x05\x12\x1c\n\x14partitioned_table_id\x18\x04 \x01(\x05\x12\x13\n\x0b\x63olumn_name\x18\x05 \x01(\t\x12\x13\n\x0b\x63olumn_type\x18\x06 \x01(\t\x12`\n\tone_click\x18\x07 \x01(\x0b\x32M.io.deephaven.proto.backplane.script.grpc.FigureDescriptor.OneClickDescriptor\x1a\x63\n\x12OneClickDescriptor\x12\x0f\n\x07\x63olumns\x18\x01 \x03(\t\x12\x14\n\x0c\x63olumn_types\x18\x02 \x03(\t\x12&\n\x1erequire_all_filters_to_display\x18\x03 \x01(\x08\"\xa6\x01\n\x0fSeriesPlotStyle\x12\x07\n\x03\x42\x41R\x10\x00\x12\x0f\n\x0bSTACKED_BAR\x10\x01\x12\x08\n\x04LINE\x10\x02\x12\x08\n\x04\x41REA\x10\x03\x12\x10\n\x0cSTACKED_AREA\x10\x04\x12\x07\n\x03PIE\x10\x05\x12\r\n\tHISTOGRAM\x10\x06\x12\x08\n\x04OHLC\x10\x07\x12\x0b\n\x07SCATTER\x10\x08\x12\x08\n\x04STEP\x10\t\x12\r\n\tERROR_BAR\x10\n\x12\x0b\n\x07TREEMAP\x10\x0b\"\xd2\x01\n\nSourceType\x12\x05\n\x01X\x10\x00\x12\x05\n\x01Y\x10\x01\x12\x05\n\x01Z\x10\x02\x12\t\n\x05X_LOW\x10\x03\x12\n\n\x06X_HIGH\x10\x04\x12\t\n\x05Y_LOW\x10\x05\x12\n\n\x06Y_HIGH\x10\x06\x12\x08\n\x04TIME\x10\x07\x12\x08\n\x04OPEN\x10\x08\x12\x08\n\x04HIGH\x10\t\x12\x07\n\x03LOW\x10\n\x12\t\n\x05\x43LOSE\x10\x0b\x12\t\n\x05SHAPE\x10\x0c\x12\x08\n\x04SIZE\x10\r\x12\t\n\x05LABEL\x10\x0e\x12\t\n\x05\x43OLOR\x10\x0f\x12\n\n\x06PARENT\x10\x10\x12\x0e\n\nHOVER_TEXT\x10\x11\x12\x08\n\x04TEXT\x10\x12\x42\x08\n\x06_titleJ\x04\x08\x0b\x10\x0cJ\x04\x08\x0c\x10\r2\xb2\r\n\x0e\x43onsoleService\x12\x98\x01\n\x0fGetConsoleTypes\x12@.io.deephaven.proto.backplane.script.grpc.GetConsoleTypesRequest\x1a\x41.io.deephaven.proto.backplane.script.grpc.GetConsoleTypesResponse\"\x00\x12\x8f\x01\n\x0cStartConsole\x12=.io.deephaven.proto.backplane.script.grpc.StartConsoleRequest\x1a>.io.deephaven.proto.backplane.script.grpc.StartConsoleResponse\"\x00\x12\x8c\x01\n\x0bGetHeapInfo\x12<.io.deephaven.proto.backplane.script.grpc.GetHeapInfoRequest\x1a=.io.deephaven.proto.backplane.script.grpc.GetHeapInfoResponse\"\x00\x12\x96\x01\n\x0fSubscribeToLogs\x12@.io.deephaven.proto.backplane.script.grpc.LogSubscriptionRequest\x1a=.io.deephaven.proto.backplane.script.grpc.LogSubscriptionData\"\x00\x30\x01\x12\x95\x01\n\x0e\x45xecuteCommand\x12?.io.deephaven.proto.backplane.script.grpc.ExecuteCommandRequest\x1a@.io.deephaven.proto.backplane.script.grpc.ExecuteCommandResponse\"\x00\x12\x92\x01\n\rCancelCommand\x12>.io.deephaven.proto.backplane.script.grpc.CancelCommandRequest\x1a?.io.deephaven.proto.backplane.script.grpc.CancelCommandResponse\"\x00\x12\xa4\x01\n\x13\x42indTableToVariable\x12\x44.io.deephaven.proto.backplane.script.grpc.BindTableToVariableRequest\x1a\x45.io.deephaven.proto.backplane.script.grpc.BindTableToVariableResponse\"\x00\x12\x99\x01\n\x12\x41utoCompleteStream\x12=.io.deephaven.proto.backplane.script.grpc.AutoCompleteRequest\x1a>.io.deephaven.proto.backplane.script.grpc.AutoCompleteResponse\"\x00(\x01\x30\x01\x12\xa1\x01\n\x12\x43\x61ncelAutoComplete\x12\x43.io.deephaven.proto.backplane.script.grpc.CancelAutoCompleteRequest\x1a\x44.io.deephaven.proto.backplane.script.grpc.CancelAutoCompleteResponse\"\x00\x12\x9b\x01\n\x16OpenAutoCompleteStream\x12=.io.deephaven.proto.backplane.script.grpc.AutoCompleteRequest\x1a>.io.deephaven.proto.backplane.script.grpc.AutoCompleteResponse\"\x00\x30\x01\x12\x98\x01\n\x16NextAutoCompleteStream\x12=.io.deephaven.proto.backplane.script.grpc.AutoCompleteRequest\x1a=.io.deephaven.proto.backplane.script.grpc.BrowserNextResponse\"\x00\x42\x43H\x01P\x01Z=github.com/deephaven/deephaven-core/go/internal/proto/consoleb\x06proto3')

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'deephaven.proto.console_pb2', _globals)
if not _descriptor._USE_C_DESCRIPTORS:
  _globals['DESCRIPTOR']._loaded_options = None
  _globals['DESCRIPTOR']._serialized_options = b'H\001P\001Z=github.com/deephaven/deephaven-core/go/internal/proto/console'
  _globals['_GETHEAPINFORESPONSE'].fields_by_name['max_memory']._loaded_options = None
  _globals['_GETHEAPINFORESPONSE'].fields_by_name['max_memory']._serialized_options = b'0\001'
  _globals['_GETHEAPINFORESPONSE'].fields_by_name['total_memory']._loaded_options = None
  _globals['_GETHEAPINFORESPONSE'].fields_by_name['total_memory']._serialized_options = b'0\001'
  _globals['_GETHEAPINFORESPONSE'].fields_by_name['free_memory']._loaded_options = None
  _globals['_GETHEAPINFORESPONSE'].fields_by_name['free_memory']._serialized_options = b'0\001'
  _globals['_LOGSUBSCRIPTIONREQUEST'].fields_by_name['last_seen_log_timestamp']._loaded_options = None
  _globals['_LOGSUBSCRIPTIONREQUEST'].fields_by_name['last_seen_log_timestamp']._serialized_options = b'0\001'
  _globals['_LOGSUBSCRIPTIONDATA'].fields_by_name['micros']._loaded_options = None
  _globals['_LOGSUBSCRIPTIONDATA'].fields_by_name['micros']._serialized_options = b'0\001'
  _globals['_OPENDOCUMENTREQUEST'].fields_by_name['console_id']._loaded_options = None
  _globals['_OPENDOCUMENTREQUEST'].fields_by_name['console_id']._serialized_options = b'\030\001'
  _globals['_CLOSEDOCUMENTREQUEST'].fields_by_name['console_id']._loaded_options = None
  _globals['_CLOSEDOCUMENTREQUEST'].fields_by_name['console_id']._serialized_options = b'\030\001'
  _globals['_CHANGEDOCUMENTREQUEST'].fields_by_name['console_id']._loaded_options = None
  _globals['_CHANGEDOCUMENTREQUEST'].fields_by_name['console_id']._serialized_options = b'\030\001'
  _globals['_GETCOMPLETIONITEMSREQUEST'].fields_by_name['console_id']._loaded_options = None
  _globals['_GETCOMPLETIONITEMSREQUEST'].fields_by_name['console_id']._serialized_options = b'\030\001'
  _globals['_GETCOMPLETIONITEMSREQUEST'].fields_by_name['request_id']._loaded_options = None
  _globals['_GETCOMPLETIONITEMSREQUEST'].fields_by_name['request_id']._serialized_options = b'\030\001'
  _globals['_GETCOMPLETIONITEMSRESPONSE'].fields_by_name['request_id']._loaded_options = None
  _globals['_GETCOMPLETIONITEMSRESPONSE'].fields_by_name['request_id']._serialized_options = b'\030\001'
  _globals['_GETCOMPLETIONITEMSRESPONSE'].fields_by_name['success']._loaded_options = None
  _globals['_GETCOMPLETIONITEMSRESPONSE'].fields_by_name['success']._serialized_options = b'\030\001'
  _globals['_FIGUREDESCRIPTOR_CHARTDESCRIPTOR_CHARTTYPE'].values_by_name["OHLC"]._loaded_options = None
  _globals['_FIGUREDESCRIPTOR_CHARTDESCRIPTOR_CHARTTYPE'].values_by_name["OHLC"]._serialized_options = b'\010\001'
  _globals['_FIGUREDESCRIPTOR'].fields_by_name['update_interval']._loaded_options = None
  _globals['_FIGUREDESCRIPTOR'].fields_by_name['update_interval']._serialized_options = b'0\001'
  _globals['_GETCONSOLETYPESREQUEST']._serialized_start=140
  _globals['_GETCONSOLETYPESREQUEST']._serialized_end=164
  _globals['_GETCONSOLETYPESRESPONSE']._serialized_start=166
  _globals['_GETCONSOLETYPESRESPONSE']._serialized_end=214
  _globals['_STARTCONSOLEREQUEST']._serialized_start=216
  _globals['_STARTCONSOLEREQUEST']._serialized_end=321
  _globals['_STARTCONSOLERESPONSE']._serialized_start=323
  _globals['_STARTCONSOLERESPONSE']._serialized_end=407
  _globals['_GETHEAPINFOREQUEST']._serialized_start=409
  _globals['_GETHEAPINFOREQUEST']._serialized_end=429
  _globals['_GETHEAPINFORESPONSE']._serialized_start=431
  _globals['_GETHEAPINFORESPONSE']._serialized_end=527
  _globals['_LOGSUBSCRIPTIONREQUEST']._serialized_start=529
  _globals['_LOGSUBSCRIPTIONREQUEST']._serialized_end=606
  _globals['_LOGSUBSCRIPTIONDATA']._serialized_start=608
  _globals['_LOGSUBSCRIPTIONDATA']._serialized_end=691
  _globals['_EXECUTECOMMANDREQUEST']._serialized_start=693
  _globals['_EXECUTECOMMANDREQUEST']._serialized_end=799
  _globals['_EXECUTECOMMANDRESPONSE']._serialized_start=801
  _globals['_EXECUTECOMMANDRESPONSE']._serialized_end=920
  _globals['_BINDTABLETOVARIABLEREQUEST']._serialized_start=923
  _globals['_BINDTABLETOVARIABLEREQUEST']._serialized_end=1104
  _globals['_BINDTABLETOVARIABLERESPONSE']._serialized_start=1106
  _globals['_BINDTABLETOVARIABLERESPONSE']._serialized_end=1135
  _globals['_CANCELCOMMANDREQUEST']._serialized_start=1138
  _globals['_CANCELCOMMANDREQUEST']._serialized_end=1286
  _globals['_CANCELCOMMANDRESPONSE']._serialized_start=1288
  _globals['_CANCELCOMMANDRESPONSE']._serialized_end=1311
  _globals['_CANCELAUTOCOMPLETEREQUEST']._serialized_start=1313
  _globals['_CANCELAUTOCOMPLETEREQUEST']._serialized_end=1423
  _globals['_CANCELAUTOCOMPLETERESPONSE']._serialized_start=1425
  _globals['_CANCELAUTOCOMPLETERESPONSE']._serialized_end=1453
  _globals['_AUTOCOMPLETEREQUEST']._serialized_start=1456
  _globals['_AUTOCOMPLETEREQUEST']._serialized_end=2209
  _globals['_AUTOCOMPLETERESPONSE']._serialized_start=2212
  _globals['_AUTOCOMPLETERESPONSE']._serialized_end=2741
  _globals['_BROWSERNEXTRESPONSE']._serialized_start=2743
  _globals['_BROWSERNEXTRESPONSE']._serialized_end=2764
  _globals['_OPENDOCUMENTREQUEST']._serialized_start=2767
  _globals['_OPENDOCUMENTREQUEST']._serialized_end=2938
  _globals['_TEXTDOCUMENTITEM']._serialized_start=2940
  _globals['_TEXTDOCUMENTITEM']._serialized_end=3023
  _globals['_CLOSEDOCUMENTREQUEST']._serialized_start=3026
  _globals['_CLOSEDOCUMENTREQUEST']._serialized_end=3213
  _globals['_CHANGEDOCUMENTREQUEST']._serialized_start=3216
  _globals['_CHANGEDOCUMENTREQUEST']._serialized_end=3668
  _globals['_CHANGEDOCUMENTREQUEST_TEXTDOCUMENTCONTENTCHANGEEVENT']._serialized_start=3528
  _globals['_CHANGEDOCUMENTREQUEST_TEXTDOCUMENTCONTENTCHANGEEVENT']._serialized_end=3668
  _globals['_DOCUMENTRANGE']._serialized_start=3671
  _globals['_DOCUMENTRANGE']._serialized_end=3818
  _globals['_VERSIONEDTEXTDOCUMENTIDENTIFIER']._serialized_start=3820
  _globals['_VERSIONEDTEXTDOCUMENTIDENTIFIER']._serialized_end=3883
  _globals['_POSITION']._serialized_start=3885
  _globals['_POSITION']._serialized_end=3928
  _globals['_MARKUPCONTENT']._serialized_start=3930
  _globals['_MARKUPCONTENT']._serialized_end=3974
  _globals['_GETCOMPLETIONITEMSREQUEST']._serialized_start=3977
  _globals['_GETCOMPLETIONITEMSREQUEST']._serialized_end=4341
  _globals['_COMPLETIONCONTEXT']._serialized_start=4343
  _globals['_COMPLETIONCONTEXT']._serialized_end=4411
  _globals['_GETCOMPLETIONITEMSRESPONSE']._serialized_start=4414
  _globals['_GETCOMPLETIONITEMSRESPONSE']._serialized_end=4560
  _globals['_COMPLETIONITEM']._serialized_start=4563
  _globals['_COMPLETIONITEM']._serialized_end=5029
  _globals['_TEXTEDIT']._serialized_start=5031
  _globals['_TEXTEDIT']._serialized_end=5127
  _globals['_GETSIGNATUREHELPREQUEST']._serialized_start=5130
  _globals['_GETSIGNATUREHELPREQUEST']._serialized_end=5404
  _globals['_SIGNATUREHELPCONTEXT']._serialized_start=5407
  _globals['_SIGNATUREHELPCONTEXT']._serialized_end=5626
  _globals['_GETSIGNATUREHELPRESPONSE']._serialized_start=5629
  _globals['_GETSIGNATUREHELPRESPONSE']._serialized_end=5843
  _globals['_SIGNATUREINFORMATION']._serialized_start=5846
  _globals['_SIGNATUREINFORMATION']._serialized_end=6099
  _globals['_PARAMETERINFORMATION']._serialized_start=6101
  _globals['_PARAMETERINFORMATION']._serialized_end=6218
  _globals['_GETHOVERREQUEST']._serialized_start=6221
  _globals['_GETHOVERREQUEST']._serialized_end=6406
  _globals['_GETHOVERRESPONSE']._serialized_start=6409
  _globals['_GETHOVERRESPONSE']._serialized_end=6574
  _globals['_GETDIAGNOSTICREQUEST']._serialized_start=6577
  _globals['_GETDIAGNOSTICREQUEST']._serialized_end=6793
  _globals['_GETPULLDIAGNOSTICRESPONSE']._serialized_start=6796
  _globals['_GETPULLDIAGNOSTICRESPONSE']._serialized_end=6944
  _globals['_GETPUBLISHDIAGNOSTICRESPONSE']._serialized_start=6947
  _globals['_GETPUBLISHDIAGNOSTICRESPONSE']._serialized_end=7099
  _globals['_DIAGNOSTIC']._serialized_start=7102
  _globals['_DIAGNOSTIC']._serialized_end=7781
  _globals['_DIAGNOSTIC_CODEDESCRIPTION']._serialized_start=7538
  _globals['_DIAGNOSTIC_CODEDESCRIPTION']._serialized_end=7569
  _globals['_DIAGNOSTIC_DIAGNOSTICSEVERITY']._serialized_start=7571
  _globals['_DIAGNOSTIC_DIAGNOSTICSEVERITY']._serialized_end=7664
  _globals['_DIAGNOSTIC_DIAGNOSTICTAG']._serialized_start=7666
  _globals['_DIAGNOSTIC_DIAGNOSTICTAG']._serialized_end=7731
  _globals['_FIGUREDESCRIPTOR']._serialized_start=7784
  _globals['_FIGUREDESCRIPTOR']._serialized_end=14030
  _globals['_FIGUREDESCRIPTOR_CHARTDESCRIPTOR']._serialized_start=8031
  _globals['_FIGUREDESCRIPTOR_CHARTDESCRIPTOR']._serialized_end=8749
  _globals['_FIGUREDESCRIPTOR_CHARTDESCRIPTOR_CHARTTYPE']._serialized_start=8644
  _globals['_FIGUREDESCRIPTOR_CHARTDESCRIPTOR_CHARTTYPE']._serialized_end=8739
  _globals['_FIGUREDESCRIPTOR_SERIESDESCRIPTOR']._serialized_start=8752
  _globals['_FIGUREDESCRIPTOR_SERIESDESCRIPTOR']._serialized_end=9390
  _globals['_FIGUREDESCRIPTOR_MULTISERIESDESCRIPTOR']._serialized_start=9393
  _globals['_FIGUREDESCRIPTOR_MULTISERIESDESCRIPTOR']._serialized_end=10781
  _globals['_FIGUREDESCRIPTOR_STRINGMAPWITHDEFAULT']._serialized_start=10783
  _globals['_FIGUREDESCRIPTOR_STRINGMAPWITHDEFAULT']._serialized_end=10883
  _globals['_FIGUREDESCRIPTOR_DOUBLEMAPWITHDEFAULT']._serialized_start=10885
  _globals['_FIGUREDESCRIPTOR_DOUBLEMAPWITHDEFAULT']._serialized_end=10985
  _globals['_FIGUREDESCRIPTOR_BOOLMAPWITHDEFAULT']._serialized_start=10987
  _globals['_FIGUREDESCRIPTOR_BOOLMAPWITHDEFAULT']._serialized_end=11081
  _globals['_FIGUREDESCRIPTOR_AXISDESCRIPTOR']._serialized_start=11084
  _globals['_FIGUREDESCRIPTOR_AXISDESCRIPTOR']._serialized_end=12146
  _globals['_FIGUREDESCRIPTOR_AXISDESCRIPTOR_AXISFORMATTYPE']._serialized_start=11920
  _globals['_FIGUREDESCRIPTOR_AXISDESCRIPTOR_AXISFORMATTYPE']._serialized_end=11962
  _globals['_FIGUREDESCRIPTOR_AXISDESCRIPTOR_AXISTYPE']._serialized_start=11964
  _globals['_FIGUREDESCRIPTOR_AXISDESCRIPTOR_AXISTYPE']._serialized_end=12031
  _globals['_FIGUREDESCRIPTOR_AXISDESCRIPTOR_AXISPOSITION']._serialized_start=12033
  _globals['_FIGUREDESCRIPTOR_AXISDESCRIPTOR_AXISPOSITION']._serialized_end=12099
  _globals['_FIGUREDESCRIPTOR_BUSINESSCALENDARDESCRIPTOR']._serialized_start=12149
  _globals['_FIGUREDESCRIPTOR_BUSINESSCALENDARDESCRIPTOR']._serialized_end=13029
  _globals['_FIGUREDESCRIPTOR_BUSINESSCALENDARDESCRIPTOR_BUSINESSPERIOD']._serialized_start=12573
  _globals['_FIGUREDESCRIPTOR_BUSINESSCALENDARDESCRIPTOR_BUSINESSPERIOD']._serialized_end=12618
  _globals['_FIGUREDESCRIPTOR_BUSINESSCALENDARDESCRIPTOR_HOLIDAY']._serialized_start=12621
  _globals['_FIGUREDESCRIPTOR_BUSINESSCALENDARDESCRIPTOR_HOLIDAY']._serialized_end=12869
  _globals['_FIGUREDESCRIPTOR_BUSINESSCALENDARDESCRIPTOR_LOCALDATE']._serialized_start=12871
  _globals['_FIGUREDESCRIPTOR_BUSINESSCALENDARDESCRIPTOR_LOCALDATE']._serialized_end=12924
  _globals['_FIGUREDESCRIPTOR_BUSINESSCALENDARDESCRIPTOR_DAYOFWEEK']._serialized_start=12926
  _globals['_FIGUREDESCRIPTOR_BUSINESSCALENDARDESCRIPTOR_DAYOFWEEK']._serialized_end=13029
  _globals['_FIGUREDESCRIPTOR_MULTISERIESSOURCEDESCRIPTOR']._serialized_start=13032
  _globals['_FIGUREDESCRIPTOR_MULTISERIESSOURCEDESCRIPTOR']._serialized_end=13214
  _globals['_FIGUREDESCRIPTOR_SOURCEDESCRIPTOR']._serialized_start=13217
  _globals['_FIGUREDESCRIPTOR_SOURCEDESCRIPTOR']._serialized_end=13525
  _globals['_FIGUREDESCRIPTOR_ONECLICKDESCRIPTOR']._serialized_start=13527
  _globals['_FIGUREDESCRIPTOR_ONECLICKDESCRIPTOR']._serialized_end=13626
  _globals['_FIGUREDESCRIPTOR_SERIESPLOTSTYLE']._serialized_start=13629
  _globals['_FIGUREDESCRIPTOR_SERIESPLOTSTYLE']._serialized_end=13795
  _globals['_FIGUREDESCRIPTOR_SOURCETYPE']._serialized_start=13798
  _globals['_FIGUREDESCRIPTOR_SOURCETYPE']._serialized_end=14008
  _globals['_CONSOLESERVICE']._serialized_start=14033
  _globals['_CONSOLESERVICE']._serialized_end=15747
# @@protoc_insertion_point(module_scope)
