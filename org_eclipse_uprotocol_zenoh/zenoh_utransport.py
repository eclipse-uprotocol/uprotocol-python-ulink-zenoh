# -------------------------------------------------------------------------

# Copyright (c) 2023 General Motors GTO LLC
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
# SPDX-FileType: SOURCE
# SPDX-FileCopyrightText: 2023 General Motors GTO LLC
# SPDX-License-Identifier: Apache-2.0

# -------------------------------------------------------------------------




import json
import threading
import time
from builtins import str
from concurrent.futures import Future
from typing import Tuple

import zenoh
from cloudevents.http import CloudEvent
from google.protobuf import any_pb2
from org_eclipse_uprotocol.cloudevent.datamodel.ucloudeventattributes import UCloudEventAttributesBuilder
from org_eclipse_uprotocol.cloudevent.factory.cloudeventfactory import CloudEventFactory
from org_eclipse_uprotocol.cloudevent.factory.ucloudevent import UCloudEvent
from org_eclipse_uprotocol.cloudevent.serialize.base64protobufserializer import Base64ProtobufSerializer
from org_eclipse_uprotocol.cloudevent.serialize.cloudeventserializers import CloudEventSerializers
from org_eclipse_uprotocol.proto.uri_pb2 import UEntity, UUri
from org_eclipse_uprotocol.rpc.rpcclient import RpcClient
from org_eclipse_uprotocol.proto.uattributes_pb2 import UAttributes, UMessageType, UPriority
from org_eclipse_uprotocol.proto.upayload_pb2 import UPayloadFormat
from org_eclipse_uprotocol.transport.builder.uattributesbuilder import UAttributesBuilder
from org_eclipse_uprotocol.transport.ulistener import UListener
from org_eclipse_uprotocol.proto.upayload_pb2 import UPayload
from org_eclipse_uprotocol.proto.ustatus_pb2 import UStatus, UCode
from org_eclipse_uprotocol.transport.utransport import UTransport
from org_eclipse_uprotocol.transport.validate.uattributesvalidator import UAttributesValidator
from org_eclipse_uprotocol.uri.builder.uresource_builder import UResourceBuilder
from org_eclipse_uprotocol.uri.serializer.longuriserializer import LongUriSerializer
from org_eclipse_uprotocol.uri.validator.urivalidator import UriValidator
from org_eclipse_uprotocol.uuid.serializer.longuuidserializer import LongUuidSerializer
from zenoh import Sample

# Dictionary to store requests
m_requests = {}
m_requests_query = {}
subscribers = {}  # remove element when ue unregister it
register_rpc_querable = {}  # remove element when ue unregister it


# Function to add a request
def add_request(req_id):
    global m_requests
    future = Future()
    m_requests[req_id] = future
    return future


def timeout_counter(response_future, reqid, timeout):
    time.sleep(timeout / 1000)
    if not response_future.done():
        response_future.set_exception(
            TimeoutError('Not received response for request ' + reqid + ' within ' + str(timeout / 1000) + ' seconds'))


class Zenoh(UTransport, RpcClient):

    def authenticate(self, u_entity: UEntity) -> UStatus:
        print("unimplemented, it is not needed in python components.")

    def send(self, topic: UUri, payload: UPayload, attributes: UAttributes) -> UStatus:

        # validate attributes
        if attributes.type == UMessageType.UMESSAGE_TYPE_PUBLISH:
            # check uri
            status = UriValidator.validate(topic)
            if status.is_failure():
                return status
            # create publish cloudevent
            ce, serialized_str = ZenohUtils.create_serialized_ce(topic, payload, attributes)

            try:
                ZenohUtils().send_data_to_zenoh(UCloudEvent.get_source(ce), serialized_str)
                return UStatus(message="successfully publish value to zenoh_up")
            except Exception as e:
                print('failed')
                return UStatus(message=str(e), code=UCode.UNKNOWN)

        elif attributes.type == UMessageType.UMESSAGE_TYPE_REQUEST:
            # check uri
            status = UriValidator.validate_rpc_method(topic)
            if status.is_failure():
                return status

            # create request cloudevent
            ce, serialized_str = ZenohUtils.create_serialized_ce(topic, payload, attributes)
            ZenohUtils().send_rpc_request_zenoh(UCloudEvent.get_sink(ce), serialized_str)

        elif attributes.type == UMessageType.UMESSAGE_TYPE_RESPONSE:
            status = UriValidator.validate_rpc_method(topic)
            if status.is_failure():
                return status

            # create response cloudevent
            ce, serialized_str = ZenohUtils.create_serialized_ce(topic, payload, attributes)
            print('response ')
            methoduri = ZenohUtils.replace_special_chars(LongUriSerializer().serialize(topic))
            m_requests_query[UCloudEvent.get_request_id(ce)].reply(Sample(methoduri, serialized_str))
            m_requests_query.pop(UCloudEvent.get_request_id(ce))
            return UStatus(message="successfully send rpc response to zenoh")

        else:
            return UStatus(message="Invalid attributes type")

    def register_listener(self, uri: UUri, listener: UListener) -> UStatus:
        # ToDo If topic is subscribed, then only allow it to register listener
        # ToDo Subscription Service
        if UriValidator.validate_rpc_method(uri).is_success():
            # TODO: only one listener is allowed for one method uri
            methoduri = ZenohUtils.replace_special_chars(LongUriSerializer().serialize(uri))
            conf = ZenohUtils.add_endpoint()
            session = zenoh.open(conf)

            def queryable_callback(query):
                print(f">> [Queryable ] Received Query '{query.selector}'" + (
                    f" with value: {query.value.payload}" if query.value is not None else ""))
                serialized_bytes = Base64ProtobufSerializer.serialize(query.value.payload.decode('utf-8'))
                ce = CloudEventSerializers.JSON.serializer().deserialize(serialized_bytes)
                data = ce.get_data()
                hint = UPayloadFormat.UPAYLOAD_FORMAT_PROTOBUF
                upayload = UPayload(value=data, format=hint)
                priority = UCloudEvent.get_priority(ce)
                uattributes = UAttributesBuilder(
                    LongUuidSerializer.instance().deserialize(UCloudEvent.get_request_id(ce)),

                    UCloudEvent.get_message_type(UCloudEvent.extract_string_value_from_attributes("type", ce)),
                    priority).build()

                m_requests_query[UCloudEvent.get_request_id(ce)] = query
                listener.on_receive(uri, upayload, uattributes)

            querable = ZenohUtils().register_rpc(methoduri, session, queryable_callback)
            register_rpc_querable[listener] = querable

        else:
            new_topic = ZenohUtils.replace_special_chars(LongUriSerializer().serialize(uri))

            def zenoh_subscribe_callback(sample) -> None:
                print("Callback called...")
                serialized_bytes = Base64ProtobufSerializer.serialize(sample.payload.decode('utf-8'))
                ce = CloudEventSerializers.JSON.serializer().deserialize(serialized_bytes)
                data = ce.get_data()
                upayload = UPayload(value=data, format=UPayloadFormat.UPAYLOAD_FORMAT_PROTOBUF)
                priority = UCloudEvent.get_priority(ce)
                uattributes = UAttributesBuilder(
                    LongUuidSerializer.instance().deserialize(
                        UCloudEvent.get_id( ce)),
                    UCloudEvent.get_message_type(UCloudEvent.get_type( ce)),
                    priority).build()
                listener.on_receive(uri, upayload, uattributes)

            conf = ZenohUtils.add_endpoint()

            try:
                session = zenoh.open(conf)
                sub = session.declare_subscriber(new_topic, zenoh_subscribe_callback)
                subscribers[listener] = sub
                return UStatus(message="successfully subscribed")

            except Exception as e:
                print(str(e))
                return UStatus(message=str(e), code=UCode.UNKNOWN)

    def unregister_listener(self, topic: UUri, listener: UListener) -> UStatus:
        print("zenoh_up unregister listener")

    def invoke_method(self, topic: UUri, payload: UPayload, attributes: UAttributes) -> Future:
        # check message type,id and ttl
        req_id = LongUuidSerializer.instance().serialize(attributes.id)
        if attributes.type != UMessageType.UMESSAGE_TYPE_REQUEST:
            raise Exception("Event type is invalid")
        if req_id is None or len(req_id.strip()) == 0:
            raise Exception("Event id is missing")
        if attributes.ttl is None or attributes.ttl <= 0:
            raise Exception("TTl is invalid or missing")

        print('invoke req id', req_id)
        response_future = add_request(req_id)
        # Start a thread to count the timeout
        timeout_thread = threading.Thread(target=timeout_counter, args=(response_future, req_id, attributes.ttl))
        timeout_thread.start()
        self.send(topic, payload, attributes)
        return response_future  # future result to be set by the service.


class ZenohUtils:
    special_chars = ['//', '#']
    special_chars_new_value = ['==', '=']
    replies = None

    @staticmethod
    def replace_special_chars(topic):
        try:
            for i, special_char in enumerate(ZenohUtils.special_chars):
                if special_char in topic:
                    topic = topic.replace(special_char, ZenohUtils.special_chars_new_value[i])
            if topic.startswith("/"):
                topic = topic[1:]
            return topic
        except Exception as e:
            print(f'Error in replacing one or character(s) {ZenohUtils.special_chars}')

    @staticmethod
    def create_serialized_ce(uri, payload, attributes) -> Tuple[CloudEvent, str]:
        # get serialized any data from payload
        data = payload.value
        any_message = any_pb2.Any()
        any_message.ParseFromString(data)
        ce = None
        ce_attributes = UCloudEventAttributesBuilder().with_priority(attributes.priority).with_ttl(
            attributes.ttl).with_token(attributes.token).build()
        if attributes.type == UMessageType.UMESSAGE_TYPE_PUBLISH:

            ce = CloudEventFactory.publish(LongUriSerializer().serialize(uri), any_message, ce_attributes)
        elif attributes.type == UMessageType.UMESSAGE_TYPE_REQUEST:
            applicationuri_for_rpc = LongUriSerializer().serialize(
                UUri(authority=uri.authority, entity=uri.entity, resource=UResourceBuilder.for_rpc_response()))
            # create rpc cloud event
            ce = CloudEventFactory.request(applicationuri_for_rpc, LongUriSerializer().serialize(uri),
                                           LongUuidSerializer.instance().serialize(attributes.id), any_message,
                                           ce_attributes)
        elif attributes.type == UMessageType.UMESSAGE_TYPE_RESPONSE:
            applicationuri_for_rpc = LongUriSerializer().serialize(
                UUri(authority=uri.authority, entity=uri.entity, resource=UResourceBuilder.for_rpc_response()))
            methoduri = LongUriSerializer().serialize(uri)
            req_id = LongUuidSerializer.instance().serialize(attributes.id)
            # create rpc response cloud event
            ce = CloudEventFactory.response(applicationuri_for_rpc, methoduri, req_id, any_message, ce_attributes)

        serialized_bytes = CloudEventSerializers.JSON.serializer().serialize(ce)
        serialized_str = Base64ProtobufSerializer.deserialize(serialized_bytes)
        return ce, serialized_str

    @staticmethod
    def add_endpoint(endpoint='10.0.0.33'):
        conf = zenoh.Config()
        if endpoint is not None:
            endpoint = [f"tcp/{endpoint}:9090"]
            print(f"EEE: {endpoint}")
            conf.insert_json5(zenoh.config.MODE_KEY, json.dumps("client"))
            conf.insert_json5(zenoh.config.CONNECT_KEY, json.dumps(endpoint))
        return conf

    def send_data_to_zenoh(self, topic: str, data_to_send: str) -> None:
        """
        publish data to zenoh_up router
        """
        new_topic = ZenohUtils.replace_special_chars(topic)
        # new_topic = self.replace_special_chars(topic)

        conf = self.add_endpoint()
        session_publish = zenoh.open(conf)

        # declare publisher
        pub = session_publish.declare_publisher(new_topic)
        print(f"Zenoh publishing data -> ('{new_topic}': '{data_to_send}')...")

        # Send to Zenoh router
        pub.put(data_to_send)
        pub.undeclare()
        session_publish.close()

    def send_rpc_request_zenoh(self, methoduri, request):
        global replies
        methoduri = self.replace_special_chars(methoduri)
        print(f'In zenoh_utils, sending rpc: {methoduri}, req: {request}')

        conf = self.add_endpoint()

        session = zenoh.open(conf)

        replies = session.get(methoduri, zenoh.Queue(), target=zenoh.QueryTarget.BEST_MATCHING(), value=request)
        for reply in replies.receiver:
            try:
                serialized_bytes = Base64ProtobufSerializer.serialize(reply.ok.payload.decode('utf-8'))
                ce = CloudEventSerializers.JSON.serializer().deserialize(serialized_bytes)
                data = UCloudEvent.get_payload(ce).SerializeToString()
                hint = UPayloadFormat.UPAYLOAD_FORMAT_PROTOBUF
                upayload = UPayload(value=data,format= hint)
                req_id = UCloudEvent.get_request_id(ce)
                future_result = m_requests[req_id]
                if not future_result.done():
                    print(">> Rpc response Received ('{}': '{}')".format(reply.ok.key_expr,
                                                                         reply.ok.payload.decode("utf-8")))
                    future_result.set_result(upayload)
                else:
                    print("Future result state is already finished or cancelled")
                m_requests.pop(req_id)
                session.close()
                break

            except Exception as ex:
                print(">> Received (Error: '{}')".format(reply.err.payload.decode("utf-8")))

    def register_rpc(self, methoduri, session, callback):
        print(f"Inside Zenoh utils register_rpc(), Key: {methoduri}")
        try:
            queryable = session.declare_queryable(methoduri, callback, False)
            return queryable
        except:
            import traceback
            traceback.print_exc()
            print(f"register_rpc error: {traceback.print_exc()}")
